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
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field
from historical_scan import ScanUnavailable, scan_last_24h

LOG = logging.getLogger("fx_rsi_monitor")
logging.basicConfig(level=logging.INFO)
# The HTTP client logs the complete Telegram API URL at INFO level,
# which would expose the bot token in hosting logs.
logging.getLogger("httpx").setLevel(logging.WARNING)
PAIRS = ("EURUSD", "USDJPY", "EURJPY")
NEW_YORK_TZ = ZoneInfo("America/New_York")
STATUS_BUTTON = "📊 Current Status"
SCAN_BUTTON = "🔎 Scan Last 24h"
SIGNAL_RETENTION_MS = 24 * 60 * 60 * 1000
STATUS_KEYBOARD = {
    "keyboard": [[{"text": STATUS_BUTTON}], [{"text": SCAN_BUTTON}]],
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
            CREATE TABLE IF NOT EXISTS scan_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_update_id INTEGER UNIQUE NOT NULL,
                chat_id INTEGER NOT NULL,
                requested_at INTEGER NOT NULL,
                state TEXT NOT NULL DEFAULT 'queued'
            );
            CREATE INDEX IF NOT EXISTS scan_jobs_state
                ON scan_jobs(state,id);
            CREATE TABLE IF NOT EXISTS replay_messages (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                scan_id INTEGER NOT NULL,
                PRIMARY KEY(chat_id,message_id)
            );
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


def format_signal_time(bar_time_ms: int) -> str:
    """M5 bar close in New York; America/New_York handles EST/EDT automatically."""
    closed_utc = datetime.fromtimestamp(bar_time_ms / 1000, tz=timezone.utc)
    return closed_utc.astimezone(NEW_YORK_TZ).strftime("%Y-%m-%d %H:%M %Z")


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
                # Signal event time is the confirmed M5 close, not the
                # phone's local Telegram UI time or delayed send time.
                ny_time = format_signal_time(snapshot.bar_time)
                enqueue(conn, f"rsi:{snapshot.bar_time}:{s.ticker}:{s.level}",
                        chat_id, f"{s.ticker} — RSI {s.level}\nNY: {ny_time}")
        conn.commit()
    if snapshot.signals:
        # Record emitted candidates without chat IDs, tokens or price data.
        # Useful when auditing visually observed vs actual qualified crossings.
        LOG.info("M5 signal candidates NY=%s events=%s recipient_paired=%s",
                 format_signal_time(snapshot.bar_time),
                 ",".join(f"{s.ticker}:{s.level}" for s in snapshot.signals),
                 chat_id is not None)
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
            body = "Connected.\n" + current_status()
            LOG.info("Telegram pairing accepted.")
        elif owner != chat_id:
            body = ("This Telegram chat is not paired. Send your private "
                    "pairing command: /start YOUR_PRIVATE_CODE")
        elif command == "/scan" or text.strip() == SCAN_BUTTON:
            pending = conn.execute(
                "SELECT id FROM scan_jobs WHERE chat_id=? AND state IN ('queued','running') LIMIT 1",
                (chat_id,),
            ).fetchone()
            if pending:
                body = "🔎 A historical scan is already running. Please wait for the results."
            else:
                conn.execute(
                    """INSERT OR IGNORE INTO scan_jobs(telegram_update_id,chat_id,requested_at,state)
                       VALUES(?,?,?,'queued')""",
                    (update_id, chat_id, int(time.time())),
                )
                body = ("🔎 Scanning available market history from the last 24 hours. "
                        "Using OANDA 5-second candles to approximate RSI 14 intrabar touches. "
                        "Results are not live signals.")
        else:
            # The Current Status button and /status preserve existing behavior.
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



async def historical_scan_worker() -> None:
    """Serialize user-triggered scans; never block Telegram's webhook handler."""
    while True:
        try:
            with closing(db_connect()) as conn:
                conn.execute("BEGIN IMMEDIATE")
                job = conn.execute(
                    "SELECT id,chat_id FROM scan_jobs WHERE state='queued' "
                    "ORDER BY id LIMIT 1"
                ).fetchone()
                if job:
                    conn.execute("UPDATE scan_jobs SET state='running' WHERE id=?",
                                 (job["id"],))
                conn.commit()

            if job is None:
                await asyncio.sleep(2)
                continue

            job_id = job["id"]
            chat_id = job["chat_id"]
            try:
                results, start, end = await scan_last_24h()
                with closing(db_connect()) as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    if paired_chat(conn) != chat_id:
                        # A former owner must not receive a queued scan after re-pairing.
                        conn.execute("UPDATE scan_jobs SET state='cancelled' WHERE id=?",
                                     (job_id,))
                        conn.commit()
                        continue

                    # Rescans replace earlier replay messages, never live alerts.
                    old = conn.execute(
                        "SELECT message_id FROM replay_messages WHERE chat_id=?",
                        (chat_id,),
                    ).fetchall()
                    for item in old:
                        mid = item["message_id"]
                        enqueue(conn, f"delete_replay:{chat_id}:{mid}",
                                chat_id, str(mid))
                        conn.execute(
                            "DELETE FROM signal_messages WHERE chat_id=? AND message_id=?",
                            (chat_id, mid),
                        )
                    conn.execute("DELETE FROM replay_messages WHERE chat_id=?", (chat_id,))
                    conn.execute("DELETE FROM outbox WHERE chat_id=? AND id LIKE 'replay:%'",
                                 (chat_id,))

                    for signal in results:
                        enqueue(
                            conn,
                            f"replay:{job_id}:{signal.time_ms}:{signal.ticker}:{signal.level}",
                            chat_id,
                            f"🧪 HISTORICAL ESTIMATE (OANDA S5)\n"
                            f"{signal.ticker} — RSI {signal.level}\n"
                            f"NY: {signal.ny_time}",
                        )

                    # Replaces "Scanning ..." while preserving the status keyboard.
                    if results:
                        msg = (f"🔎 Scan completed: {len(results)} estimated "
                               f"RSI events from the available last 24 hours.\n"
                               "Each event was posted separately in New York time.\n"
                               "These are 5-second historical approximations, not live alerts.")
                    else:
                        msg = ("🔎 Scan completed. No qualifying RSI 30/70 events "
                               "were reconstructed from available OANDA S5 history "
                               "in the last 24 hours.\nHistorical replay is approximate.")
                    enqueue(conn, f"status:scanfinish:{job_id}", chat_id, msg)
                    conn.execute("UPDATE scan_jobs SET state='done' WHERE id=?",
                                 (job_id,))
                    conn.commit()
                LOG.info("Historical S5 replay finished job=%s event_count=%s",
                         job_id, len(results))
            except ScanUnavailable as ex:
                # Missing credentials, HTTP failures, and API limits are reported
                # clearly; no fabricated results are posted.
                with closing(db_connect()) as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    if paired_chat(conn) == chat_id:
                        enqueue(conn, f"status:scanerror:{job_id}", chat_id,
                                f"🔎 Historical scan unavailable.\n{ex}")
                    conn.execute("UPDATE scan_jobs SET state='failed' WHERE id=?",
                                 (job_id,))
                    conn.commit()
                LOG.warning("Historical scan unavailable job=%s type=%s",
                            job_id, type(ex).__name__)
            except Exception as ex:
                with closing(db_connect()) as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    if paired_chat(conn) == chat_id:
                        enqueue(conn, f"status:scanerror:{job_id}", chat_id,
                                "🔎 Historical scan failed unexpectedly. Try again later.")
                    conn.execute("UPDATE scan_jobs SET state='failed' WHERE id=?",
                                 (job_id,))
                    conn.commit()
                LOG.exception("Historical scan failed job=%s", job_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOG.exception("Unexpected historical scan worker error")
            await asyncio.sleep(5)


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
                        ("delete:", "delete_signal:", "delete_request:", "delete_replay:")
                    )
                    is_status = task_id.startswith("status:")
                    is_live_signal = task_id.startswith("rsi:")
                    is_replay = task_id.startswith("replay:")
                    is_signal = is_live_signal or is_replay
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
                                if task_id.startswith(("delete_replay:", "delete_signal:")):
                                    conn.execute(
                                        "DELETE FROM replay_messages WHERE chat_id=? AND message_id=?",
                                        (row["chat_id"], int(row["body"])),
                                    )
                                conn.execute("DELETE FROM outbox WHERE id=?", (task_id,))
                                conn.commit()
                            continue

                        event_time_ms = None
                        if is_signal:
                            # Older queued events should never be delivered as
                            # fresh signals once their 24-hour window is over.
                            event_time_ms = int(task_id.split(":")[2 if is_replay else 1])
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
                                if is_replay:
                                    conn.execute(
                                        """INSERT OR IGNORE INTO replay_messages(
                                             chat_id,message_id,scan_id) VALUES(?,?,?)""",
                                        (row["chat_id"], message_id,
                                         int(task_id.split(":")[1])),
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
    # Recover jobs interrupted by a deployment restart.
    with closing(db_connect()) as conn:
        conn.execute("UPDATE scan_jobs SET state='queued' WHERE state='running'")
        conn.commit()
    worker = asyncio.create_task(delivery_worker())
    scan_worker = asyncio.create_task(historical_scan_worker())
    try:
        yield
    finally:
        scan_worker.cancel()
        worker.cancel()
        for task in (worker, scan_worker):
            try:
                await task
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