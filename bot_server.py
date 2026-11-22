"""FX RSI Monitor: TradingView snapshots -> Telegram signal alerts + /status.

This service does NOT determine trade validity, entries, exits or cancellations.
Run one application worker with persistent SQLite storage.
"""
from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
import sqlite3
import time
from contextlib import asynccontextmanager, closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

LOG = logging.getLogger("fx_rsi_monitor")
logging.basicConfig(level=logging.INFO)
# The HTTP client logs the complete Telegram API URL at INFO level,
# which would expose the bot token in hosting logs.
logging.getLogger("httpx").setLevel(logging.WARNING)
PAIRS = ("EURUSD", "USDJPY", "EURJPY")
STATUS_BUTTON = "📊 Current Status"
SIGNAL_RETENTION_MS = 24 * 60 * 60 * 1000
STATUS_KEYBOARD = {
    "keyboard": [[{"text": STATUS_BUTTON}]],
    "resize_keyboard": True,
    "one_time_keyboard": False,
    "is_persistent": True,
}



class Signal(BaseModel):
    model_config = ConfigDict(extra="forbid")
    ticker: Literal["EURUSD", "USDJPY", "EURJPY"]
    level: Literal[30, 70]


class Snapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal[1]
    bar_time: int = Field(gt=1_500_000_000_000)  # Unix epoch milliseconds
    states: dict[str, Literal["BUY", "SELL", "RANGE"]]
    signals: list[Signal] = Field(max_length=3)

    def check_logic(self) -> None:
        if set(self.states) != set(PAIRS):
            raise ValueError("Exactly EURUSD, USDJPY and EURJPY states are required")
        if len({s.ticker for s in self.signals}) != len(self.signals):
            raise ValueError("Only one event per ticker per completed M5 bar")
        for s in self.signals:
            expected = "BUY" if s.level == 70 else "SELL"
            if self.states[s.ticker] != expected:
                raise ValueError(f"{s.ticker} signal/state mismatch")
        if self.bar_time > int(time.time() * 1000) + 15 * 60 * 1000:
            raise ValueError("bar_time is too far in the future")


def env_required(name: str, min_len: int) -> str:
    value = os.getenv(name, "").strip()
    if len(value) < min_len or "REPLACE" in value:
        raise RuntimeError(f"Set {name} to a genuine value (min {min_len} characters)")
    return value


class Config:
    def __init__(self) -> None:
        self.telegram_token = env_required("TELEGRAM_BOT_TOKEN", 16)
        self.telegram_webhook_secret = env_required("TELEGRAM_WEBHOOK_SECRET", 16)
        self.tradingview_webhook_secret = env_required("TRADINGVIEW_WEBHOOK_SECRET", 24)
        self.pairing_code = env_required("PAIRING_CODE", 16)
        self.database_path = Path(os.getenv("DATABASE_PATH", "./fx_rsi.sqlite3"))
        self.stale_after_minutes = max(10, int(os.getenv("STALE_AFTER_MINUTES", "20")))


config: Config | None = None


def db_connect() -> sqlite3.Connection:
    assert config is not None
    config.database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(config.database_path), timeout=10)
    connection.row_factory = sqlite3.Row
    return connection


def initialize_db() -> None:
    with closing(db_connect()) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS latest (
                id INTEGER PRIMARY KEY CHECK(id = 1),
                bar_time INTEGER NOT NULL,
                states_json TEXT NOT NULL,
                received_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS outbox (
                id TEXT PRIMARY KEY,
                chat_id INTEGER NOT NULL,
                body TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                available_at REAL NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS signal_messages (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                signal_at_ms INTEGER NOT NULL,
                expires_at_ms INTEGER NOT NULL,
                PRIMARY KEY(chat_id,message_id)
            );
            CREATE INDEX IF NOT EXISTS signal_expiry_idx
                ON signal_messages(expires_at_ms);
            """
        )
        conn.commit()


def paired_chat(conn: sqlite3.Connection) -> int | None:
    row = conn.execute("SELECT value FROM settings WHERE key = 'paired_chat'").fetchone()
    return int(row["value"]) if row else None


def enqueue(conn: sqlite3.Connection, unique_id: str, chat_id: int, body: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO outbox(id,chat_id,body,available_at) VALUES(?,?,?,?)",
        (unique_id, chat_id, body, time.time()),
    )


def accept_snapshot(snapshot: Snapshot) -> bool:
    """Atomic update; out-of-order/duplicate candle snapshots cannot duplicate alerts."""
    snapshot.check_logic()
    with closing(db_connect()) as conn:
        conn.execute("BEGIN IMMEDIATE")
        last = conn.execute("SELECT bar_time FROM latest WHERE id = 1").fetchone()
        if last and snapshot.bar_time <= last["bar_time"]:
            conn.rollback()
            return False
        conn.execute(
            """INSERT INTO latest(id,bar_time,states_json,received_at)
               VALUES(1,?,?,?)
               ON CONFLICT(id) DO UPDATE SET
               bar_time=excluded.bar_time,
               states_json=excluded.states_json,
               received_at=excluded.received_at""",
            (snapshot.bar_time, json.dumps(snapshot.states), int(time.time())),
        )
        chat_id = paired_chat(conn)
        if chat_id is not None:
            for s in snapshot.signals:
                # Exactly the agreed chat format: no trade direction or interpretation.
                enqueue(conn, f"rsi:{snapshot.bar_time}:{s.ticker}:{s.level}",
                        chat_id, f"{s.ticker} — RSI {s.level}")
        conn.commit()
    return True


def current_status() -> str:
    assert config is not None
    with closing(db_connect()) as conn:
        row = conn.execute("SELECT bar_time,states_json,received_at FROM latest WHERE id=1").fetchone()
    if not row:
        return "Waiting for the first confirmed M5 update from TradingView."
    states = json.loads(row["states_json"])
    report = "\n".join(f"{pair} — {states[pair]}" for pair in PAIRS)
    freshness_minutes = (time.time() * 1000 - row["bar_time"]) / 60_000
    if freshness_minutes > config.stale_after_minutes:
        utc = datetime.fromtimestamp(row["bar_time"] / 1000, timezone.utc)
        report += "\n⚠️ STALE DATA — last M5 bar: " + utc.strftime("%Y-%m-%d %H:%M UTC")
    return report


def handle_telegram_update(data: dict) -> None:
    """Every private-chat request yields one replaceable status and hides the request."""
    update_id = data.get("update_id")
    msg = data.get("message")
    if not isinstance(update_id, int) or not isinstance(msg, dict):
        return
    chat = msg.get("chat") or {}
    if chat.get("type") not in ("private", None):
        return
    chat_id = chat.get("id")
    message_id = msg.get("message_id")
    text = msg.get("text")
    if not isinstance(chat_id, int) or not isinstance(message_id, int):
        return
    if not isinstance(text, str) or not text.strip():
        return
    parts = text.strip().split(maxsplit=1)
    command = parts[0].split("@")[0].lower()
    arg = parts[1].strip() if len(parts) > 1 else ""
    assert config is not None

    with closing(db_connect()) as conn:
        conn.execute("BEGIN IMMEDIATE")
        owner = paired_chat(conn)
        category = command if command in ("/start", "/ping", "/status", "/help") else "other"
        LOG.info("Telegram command=%s paired=%s owner_match=%s",
                 category, owner is not None, owner == chat_id)

        if command == "/start" and hmac.compare_digest(arg, config.pairing_code):
            conn.execute(
                """INSERT INTO settings(key,value) VALUES('paired_chat',?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                (str(chat_id),),
            )
            body = "Connected.\\n" + current_status()
            LOG.info("Telegram pairing accepted.")
        elif owner != chat_id:
            body = ("This Telegram chat is not paired. Send your private "
                    "pairing command: /start YOUR_PRIVATE_CODE")
        else:
            # Pressing the persistent keyboard is just another text request.
            # /status, /ping and user-written requests all produce one status.
            body = current_status()

        # The worker sends this report first; only then it deletes the
        # incoming user message, so no request remains visible in chat.
        enqueue(conn, f"status:{update_id}:req:{message_id}", chat_id, body)
        conn.commit()


def queue_expired_signals() -> None:
    """Durably schedule removal of delivered signals 24 hours after their M5 event."""
    now_ms = int(time.time() * 1000)
    with closing(db_connect()) as conn:
        conn.execute("BEGIN IMMEDIATE")
        due = conn.execute(
            "SELECT chat_id,message_id FROM signal_messages "
            "WHERE expires_at_ms <= ? LIMIT 100",
            (now_ms,),
        ).fetchall()
        for row in due:
            chat_id, message_id = row["chat_id"], row["message_id"]
            enqueue(conn, f"delete_signal:{chat_id}:{message_id}",
                    chat_id, str(message_id))
            conn.execute(
                "DELETE FROM signal_messages WHERE chat_id=? AND message_id=?",
                (chat_id, message_id),
            )
        conn.commit()


async def delivery_worker() -> None:
    """Send replaceable status reports and keep only signals from the last 24h."""
    assert config is not None
    endpoint = f"https://api.telegram.org/bot{config.telegram_token}"
    async with httpx.AsyncClient(timeout=8) as client:
        while True:
            try:
                queue_expired_signals()
                with closing(db_connect()) as conn:
                    rows = conn.execute(
                        "SELECT id,chat_id,body,attempts FROM outbox "
                        "WHERE available_at <= ? ORDER BY available_at,id LIMIT 20",
                        (time.time(),),
                    ).fetchall()
                for row in rows:
                    task_id = row["id"]
                    is_delete = task_id.startswith(
                        ("delete:", "delete_signal:", "delete_request:")
                    )
                    is_status = task_id.startswith("status:")
                    is_signal = task_id.startswith("rsi:")
                    try:
                        if is_delete:
                            response = await client.post(
                                endpoint + "/deleteMessage",
                                json={"chat_id": row["chat_id"],
                                      "message_id": int(row["body"])},
                            )
                            if response.status_code == 400:
                                # Telegram disallows deletion in certain situations
                                # (e.g. messages >48h). Avoid infinite retry loops.
                                LOG.warning("Telegram cannot delete this tracked message (400).")
                            else:
                                response.raise_for_status()
                                if not response.json().get("ok"):
                                    raise RuntimeError("Telegram deletion returned not-ok")
                            with closing(db_connect()) as conn:
                                conn.execute("DELETE FROM outbox WHERE id=?", (task_id,))
                                conn.commit()
                            continue

                        event_time_ms = None
                        if is_signal:
                            # Older queued events should never be delivered as
                            # fresh signals once their 24-hour window is over.
                            event_time_ms = int(task_id.split(":")[1])
                            if event_time_ms + SIGNAL_RETENTION_MS <= int(time.time() * 1000):
                                with closing(db_connect()) as conn:
                                    conn.execute("DELETE FROM outbox WHERE id=?", (task_id,))
                                    conn.commit()
                                continue

                        payload = {"chat_id": row["chat_id"], "text": row["body"]}
                        if is_status:
                            with closing(db_connect()) as conn:
                                if paired_chat(conn) == row["chat_id"]:
                                    payload["reply_markup"] = STATUS_KEYBOARD

                        response = await client.post(endpoint + "/sendMessage", json=payload)
                        response.raise_for_status()
                        result = response.json()
                        if not result.get("ok"):
                            raise RuntimeError("Telegram send returned not-ok")

                        message_id = result.get("result", {}).get("message_id")
                        if not isinstance(message_id, int):
                            raise RuntimeError("Telegram response missing message_id")

                        with closing(db_connect()) as conn:
                            conn.execute("BEGIN IMMEDIATE")
                            if is_status:
                                key = "last_status_message:" + str(row["chat_id"])
                                old = conn.execute(
                                    "SELECT value FROM settings WHERE key=?", (key,)
                                ).fetchone()
                                conn.execute(
                                    """INSERT INTO settings(key,value) VALUES(?,?)
                                       ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                                    (key, str(message_id)),
                                )
                                if old and old["value"] != str(message_id):
                                    old_id = int(old["value"])
                                    enqueue(conn, f"delete:{row['chat_id']}:{old_id}",
                                            row["chat_id"], str(old_id))
                                # Only delete a user request after the bot's
                                # replacement status has been sent successfully.
                                fields = task_id.split(":")
                                if len(fields) == 4 and fields[2] == "req":
                                    req_message_id = int(fields[3])
                                    enqueue(conn,
                                            f"delete_request:{row['chat_id']}:{req_message_id}",
                                            row["chat_id"], str(req_message_id))
                            elif is_signal and event_time_ms is not None:
                                conn.execute(
                                    """INSERT OR IGNORE INTO signal_messages(
                                         chat_id,message_id,signal_at_ms,expires_at_ms)
                                       VALUES(?,?,?,?)""",
                                    (row["chat_id"], message_id, event_time_ms,
                                     event_time_ms + SIGNAL_RETENTION_MS),
                                )
                            # Legacy 'reply:' messages, if any, are not
                            # deleted automatically because their purpose is unknown.
                            conn.execute("DELETE FROM outbox WHERE id=?", (task_id,))
                            conn.commit()
                    except Exception as ex:
                        # Never print the HTTP URL; it contains the bot token.
                        LOG.warning("Telegram task deferred category=%s type=%s",
                                    "delete" if is_delete else
                                    "signal" if is_signal else "status",
                                    type(ex).__name__)
                        backoff = min(300, 2 ** min(int(row["attempts"]) + 1, 8))
                        with closing(db_connect()) as conn:
                            conn.execute(
                                "UPDATE outbox SET attempts=attempts+1, available_at=? WHERE id=?",
                                (time.time() + backoff, task_id),
                            )
                            conn.commit()
                await asyncio.sleep(1.5)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.exception("Unexpected worker error")
                await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global config
    config = Config()
    initialize_db()
    worker = asyncio.create_task(delivery_worker())
    try:
        yield
    finally:
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass


app = FastAPI(title="FX RSI Monitor", docs_url=None, redoc_url=None, lifespan=lifespan)


@app.get("/health")
async def health():
    return {"ok": True}


@app.post("/tradingview/{secret}")
async def tradingview_webhook(secret: str, snapshot: Snapshot, request: Request):
    assert config is not None
    if not hmac.compare_digest(secret, config.tradingview_webhook_secret):
        raise HTTPException(status_code=403, detail="Forbidden")
    if int(request.headers.get("content-length", "0")) > 8000:
        raise HTTPException(status_code=413, detail="Payload too large")
    try:
        updated = accept_snapshot(snapshot)
    except ValueError as ex:
        raise HTTPException(status_code=422, detail=str(ex)) from ex
    return {"ok": True, "new_candle": updated}


@app.post("/telegram")
async def telegram_webhook(request: Request):
    assert config is not None
    sent_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not hmac.compare_digest(sent_secret, config.telegram_webhook_secret):
        raise HTTPException(status_code=403, detail="Forbidden")
    if int(request.headers.get("content-length", "0")) > 8000:
        raise HTTPException(status_code=413, detail="Payload too large")
    content = await request.json()
    if isinstance(content, dict):
        handle_telegram_update(content)
    return {"ok": True}