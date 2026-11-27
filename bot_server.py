"""Six-market M5 ADX(14) signal bot: TradingView readings -> Telegram alerts.

This service receives MAER v4 setup-ready observations; it does NOT execute trades.
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
from pydantic import BaseModel, ConfigDict, Field, ValidationError

LOG = logging.getLogger("adx_signal_monitor")
logging.basicConfig(level=logging.INFO)
# The HTTP client logs the complete Telegram API URL at INFO level,
# which would expose the bot token in hosting logs.
logging.getLogger("httpx").setLevel(logging.WARNING)
PAIRS = ("XAUUSD", "NASDAQ", "BTCUSD", "EURUSD", "USDJPY", "EURJPY")
NEW_YORK_TZ = ZoneInfo("America/New_York")
STATUS_BUTTON = "📊 ADX Status"
AUDIT_BUTTON = "📋 ADX Event Audit"
SIGNAL_RETENTION_MS = 24 * 60 * 60 * 1000
STATUS_KEYBOARD = {
    "keyboard": [[{"text": STATUS_BUTTON}], [{"text": AUDIT_BUTTON}]],
    "resize_keyboard": True,
    "one_time_keyboard": False,
    "is_persistent": True,
}



class ADXReading(BaseModel):
    """One last-completed M5 ADX(14) reading for a specific instrument."""
    model_config = ConfigDict(extra="forbid")
    version: Literal[3]
    ticker: Literal["XAUUSD", "NASDAQ", "BTCUSD", "EURUSD", "USDJPY", "EURJPY"]
    source: str = Field(min_length=5, max_length=90)
    bar_time: int = Field(gt=1_500_000_000_000)
    timeframe: Literal["5"]
    period: Literal[14]
    adx: float = Field(ge=0, le=100)

    def check_logic(self) -> None:
        # Different NASDAQ-family contracts share one NASDAQ signal gate.
        if ":" not in self.source:
            raise ValueError("source must include an exchange/broker prefix")
        approved_feeds = {
            "XAUUSD": "VANTAGE:XAUUSD",
            "BTCUSD": "CRYPTO:BTCUSD",
            "EURUSD": "FX_IDC:EURUSD",
            "USDJPY": "FX_IDC:USDJPY",
            "EURJPY": "FX_IDC:EURJPY",
        }
        if self.ticker == "NASDAQ":
            if not self.source.startswith("IG:"):
                raise ValueError("NASDAQ must use the selected IG symbol")
        elif self.source != approved_feeds[self.ticker]:
            raise ValueError("source does not match selected ADX data provider")
        if self.bar_time > int(time.time() * 1000) + 2 * 60 * 1000:
            raise ValueError("bar_time is in the future")


class MAERM5(BaseModel):
    model_config = ConfigDict(extra="forbid")
    bar_time: int = Field(gt=1_500_000_000_000)
    adx: float = Field(ge=0, le=100)
    direction: Literal["BUY", "SELL", "NEUTRAL"]
    ema: float | None
    close: float | None


class MAERSignal(BaseModel):
    model_config = ConfigDict(extra="forbid")
    direction: Literal["BUY", "SELL"]
    trigger_time: int = Field(gt=1_500_000_000_000)
    ready_time: int = Field(gt=1_500_000_000_000)


class MAERMarket(BaseModel):
    model_config = ConfigDict(extra="forbid")
    ticker: Literal["XAUUSD", "NASDAQ", "BTCUSD", "EURUSD", "USDJPY", "EURJPY"]
    source: str = Field(min_length=5, max_length=90)
    timeframe: Literal["5"]
    period: Literal[14]
    m5: list[MAERM5] = Field(max_length=6)
    rsi_timeframe: Literal["1"]
    rsi_period: Literal[14]
    rsi: float | None = Field(ge=0, le=100)
    rsi_status: Literal[0, 30, 50, 70]
    previous_rsi_status: Literal[0, 30, 50, 70]
    rsi50_event: bool
    signal: MAERSignal | None


class MAERBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal[4]
    event_time: int = Field(gt=1_500_000_000_000)
    markets: list[MAERMarket] = Field(min_length=1, max_length=6)


def validate_maer_batch(batch: MAERBatch) -> None:
    now = int(time.time() * 1000)
    if batch.event_time > now + 120000:
        raise ValueError("event_time is in the future")
    if len({m.ticker for m in batch.markets}) != len(batch.markets):
        raise ValueError("duplicate market")
    for market in batch.markets:
        # Reuse the existing feed whitelist, even for RSI-only updates.
        ADXReading(version=3, ticker=market.ticker, source=market.source,
                   bar_time=batch.event_time, timeframe="5", period=14,
                   adx=0).check_logic()
        for reading in market.m5:
            ADXReading(version=3, ticker=market.ticker, source=market.source,
                       bar_time=reading.bar_time, timeframe="5", period=14,
                       adx=reading.adx).check_logic()
        signal = market.signal
        if signal:
            if not signal.trigger_time <= signal.ready_time <= batch.event_time + 120000:
                raise ValueError("invalid signal chronology")
            if market.rsi_status == 0 or market.previous_rsi_status == 0:
                raise ValueError("unknown RSI history")
            if market.rsi_status != 50 and not market.rsi50_event:
                raise ValueError("RSI is not ready")
            for reading in market.m5:
                if reading.bar_time == signal.trigger_time and (
                    reading.direction != signal.direction or reading.adx < 30
                ):
                    raise ValueError("signal disagrees with trigger reading")


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
            CREATE TABLE IF NOT EXISTS authorized_chats (
                chat_id INTEGER PRIMARY KEY,
                paired_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
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
            CREATE TABLE IF NOT EXISTS maer_signals (
                ticker TEXT NOT NULL,
                trigger_time INTEGER NOT NULL,
                ready_time INTEGER NOT NULL,
                direction TEXT NOT NULL,
                PRIMARY KEY(ticker, trigger_time)
            );
            CREATE TABLE IF NOT EXISTS adx_latest (
                ticker TEXT PRIMARY KEY,
                bar_time INTEGER NOT NULL,
                adx REAL NOT NULL,
                source TEXT NOT NULL,
                armed INTEGER NOT NULL,
                received_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS adx_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                bar_time INTEGER NOT NULL,
                adx REAL NOT NULL,
                source TEXT NOT NULL,
                kind TEXT NOT NULL,
                task_id TEXT UNIQUE,
                delivery_status TEXT NOT NULL DEFAULT 'not_applicable'
            );
            CREATE INDEX IF NOT EXISTS adx_audit_time_idx
                ON adx_audit(bar_time);
            """
        )
        # Retire unsent legacy ADX-only notifications, preserving audit history.
        conn.execute("DELETE FROM outbox WHERE id LIKE 'adx:%'")
        # Delivered trading signals remain permanently in chat.
        # Cancel pending legacy signal-deletion tasks before the worker starts.
        conn.execute("DELETE FROM outbox WHERE id LIKE 'delete_signal:%'")
        # One-time retirement of old model data. Never delete new ADX alerts
        # after a service restart: their 24-hour lifecycle is handled by worker.
        converted = conn.execute(
            "SELECT 1 FROM settings WHERE key='adx_migrated_v3'"
        ).fetchone()
        if not converted:
            conn.execute(
                "DELETE FROM outbox WHERE id LIKE 'rsi:%' OR id LIKE 'replay:%' "
                "OR id LIKE 'h1double:%' OR id LIKE 'status:%' "
                "OR id LIKE 'delete_replay:%'"
            )
            for table in ("latest", "pair_latest", "pair_audit", "rsi_signal_gate",
                          "scan_jobs", "replay_messages", "h1_double_sweeps"):
                conn.execute(f"DROP TABLE IF EXISTS {table}")
            conn.execute(
                "INSERT INTO settings(key,value) VALUES('adx_migrated_v3','1')"
            )
        # Keep previously paired users, including upgraded legacy accounts.
        old_owner = conn.execute(
            "SELECT value FROM settings WHERE key='paired_chat'"
        ).fetchone()
        if old_owner:
            conn.execute(
                "INSERT OR IGNORE INTO authorized_chats(chat_id,paired_at) VALUES(?,?)",
                (int(old_owner["value"]), int(time.time())),
            )
        conn.commit()


def authorized_chats(conn: sqlite3.Connection) -> list[int]:
    return [int(row["chat_id"]) for row in conn.execute(
        "SELECT chat_id FROM authorized_chats ORDER BY paired_at,chat_id"
    ).fetchall()]


def is_authorized(conn: sqlite3.Connection, chat_id: int) -> bool:
    return conn.execute(
        "SELECT 1 FROM authorized_chats WHERE chat_id=?", (chat_id,)
    ).fetchone() is not None


def paired_chat(conn: sqlite3.Connection) -> int | None:
    chats = authorized_chats(conn)
    return chats[0] if chats else None


def enqueue(conn: sqlite3.Connection, unique_id: str, chat_id: int, body: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO outbox(id,chat_id,body,available_at) VALUES(?,?,?,?)",
        (unique_id, chat_id, body, time.time()),
    )


def format_signal_time(bar_time_ms: int) -> str:
    """M5 bar close in New York; America/New_York handles EST/EDT automatically."""
    closed_utc = datetime.fromtimestamp(bar_time_ms / 1000, tz=timezone.utc)
    return closed_utc.astimezone(NEW_YORK_TZ).strftime("%Y-%m-%d %H:%M %Z")


def accept_adx_reading(item: ADXReading) -> bool:
    """Detect *rising* M5 ADX crossing 30; only readings below 30 rearm.

    No alert is generated on a downward crossing. Crossings are derived from
    consecutive confirmed readings, not trusted event flags from TradingView.
    All state changes and notification queues are committed atomically.
    """
    item.check_logic()
    with closing(db_connect()) as conn:
        conn.execute("BEGIN IMMEDIATE")
        prior = conn.execute(
            "SELECT bar_time,adx,armed,source FROM adx_latest WHERE ticker=?",
            (item.ticker,),
        ).fetchone()
        if prior and item.bar_time <= prior["bar_time"]:
            conn.rollback()
            return False

        source_changed = bool(prior and prior["source"] != item.source)
        # A new provider has different ADX history: establish a fresh
        # baseline, never manufacture a crossing from unlike price feeds.
        armed = (int(prior["armed"]) if prior and not source_changed
                 else int(item.adx < 30))
        kind = "INITIAL" if prior is None or source_changed else "HEARTBEAT"
        delivery = "not_applicable"
        task_id = None
        if prior and not source_changed:
            previous = float(prior["adx"])
            if item.adx < 30:
                if not armed:
                    kind = "RESET"
                armed = 1
            elif previous < 30 <= item.adx and armed:
                kind = "CROSS30"
                armed = 0
                # ADX qualification is silent. Only a V4 ready signal
                # may create a Telegram notification.
                delivery = "silent_activation"
            else:
                # Above 30 without a new crossing cannot generate an alert.
                armed = 0

        conn.execute(
            """INSERT INTO adx_latest(ticker,bar_time,adx,source,armed,received_at)
               VALUES(?,?,?,?,?,?)
               ON CONFLICT(ticker) DO UPDATE SET
                   bar_time=excluded.bar_time,adx=excluded.adx,
                   source=excluded.source,armed=excluded.armed,
                   received_at=excluded.received_at""",
            (item.ticker, item.bar_time, item.adx, item.source,
             armed, int(time.time())),
        )
        if kind in ("CROSS30", "RESET"):
            conn.execute(
                """INSERT INTO adx_audit
                   (ticker,bar_time,adx,source,kind,task_id,delivery_status)
                   VALUES(?,?,?,?,?,?,?)""",
                (item.ticker, item.bar_time, item.adx, item.source,
                 kind, task_id, delivery),
            )
        conn.execute(
            "DELETE FROM adx_audit WHERE bar_time < ?",
            (item.bar_time - 7 * 24 * 60 * 60 * 1000,),
        )
        conn.commit()
    if kind in ("CROSS30", "RESET"):
        LOG.info("ADX M5 event=%s ticker=%s NY=%s value=%.3f delivery=%s",
                 kind, item.ticker, format_signal_time(item.bar_time),
                 item.adx, delivery)
    return True


def accept_maer_batch(batch: MAERBatch) -> int:
    validate_maer_batch(batch)
    accepted = 0
    for market in batch.markets:
        for reading in sorted(market.m5, key=lambda r: r.bar_time):
            accepted += int(accept_adx_reading(ADXReading(
                version=3, ticker=market.ticker, source=market.source,
                bar_time=reading.bar_time, timeframe="5", period=14,
                adx=reading.adx)))
        signal = market.signal
        if signal is None:
            continue
        now = int(time.time() * 1000)
        if signal.ready_time + SIGNAL_RETENTION_MS <= now:
            continue
        with closing(db_connect()) as conn:
            conn.execute("BEGIN IMMEDIATE")
            inserted = conn.execute(
                "INSERT OR IGNORE INTO maer_signals(ticker,trigger_time,ready_time,direction) "
                "VALUES(?,?,?,?)",
                (market.ticker, signal.trigger_time, signal.ready_time, signal.direction),
            ).rowcount
            if inserted:
                ready_et = datetime.fromtimestamp(signal.ready_time / 1000, tz=NEW_YORK_TZ)
                body = f"{market.ticker}, {signal.direction.title()}, {ready_et.hour}:{ready_et.minute:02d} ET"
                for chat_id in authorized_chats(conn):
                    enqueue(conn, f"maer:{signal.ready_time}:{market.ticker}:{signal.trigger_time}:{chat_id}",
                            chat_id, body)
            conn.commit()
        if inserted:
            LOG.info("MAER V4 ready ticker=%s direction=%s", market.ticker, signal.direction)
    LOG.info("MAER V4 accepted markets=%d new_m5=%d", len(batch.markets), accepted)
    return accepted


def signal_audit() -> str:
    """One line per ticker with an ADX crossing in the last 24h, latest ADX."""
    cutoff_ms = int(time.time() * 1000) - SIGNAL_RETENTION_MS
    with closing(db_connect()) as conn:
        rows = conn.execute(
            """SELECT l.ticker,l.adx FROM adx_latest AS l
               WHERE EXISTS (SELECT 1 FROM adx_audit AS a
                             WHERE a.ticker=l.ticker AND a.kind='CROSS30'
                             AND a.bar_time >= ?)""",
            (cutoff_ms,),
        ).fetchall()
    if not rows:
        return "No ADX events"
    values = {row["ticker"]: row["adx"] for row in rows}
    return "\n".join(f"{ticker}, {values[ticker]:.0f}" for ticker in PAIRS if ticker in values)



def current_status() -> str:
    """Latest confirmed M5 ADX, ticker and rounded integer only."""
    with closing(db_connect()) as conn:
        values = {row["ticker"]: row["adx"] for row in conn.execute(
            "SELECT ticker,adx FROM adx_latest"
        ).fetchall()}
    return "\n".join(
        f"{ticker}, {values[ticker]:.0f}" if ticker in values else f"{ticker}, N/A"
        for ticker in PAIRS
    )


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
        authorized = is_authorized(conn, chat_id)
        category = command if command in ("/start", "/ping", "/status", "/help") else "other"
        LOG.info("Telegram command=%s authorized=%s", category, authorized)

        if command == "/start" and hmac.compare_digest(arg, config.pairing_code):
            if not authorized:
                count = conn.execute("SELECT COUNT(*) FROM authorized_chats").fetchone()[0]
                if count >= 7:
                    body = "All seven authorized user slots are occupied. Contact the bot administrator."
                else:
                    conn.execute(
                        "INSERT INTO authorized_chats(chat_id,paired_at) VALUES(?,?)",
                        (chat_id, int(time.time())),
                    )
                    authorized = True
                    body = "Connected. You are an authorized user.\\n" + current_status()
                    LOG.info("Telegram pairing accepted; authorized_users=%d", count + 1)
            else:
                body = "Already connected.\\n" + current_status()
        elif not authorized:
            body = ("This Telegram chat is not paired. Send your private "
                    "pairing command: /start YOUR_PRIVATE_CODE")
        elif command == "/audit" or text.strip() in (AUDIT_BUTTON, "📋 RSI Event Audit"):
            body = signal_audit()
        elif command == "/scan" or text.strip() == "🔎 Scan Last 24h":
            body = "Historical scan is unavailable. ADX signals and the 24-hour audit remain active."
        else:
            # The Current Status button and /status preserve existing behavior.
            body = current_status()

        # The worker sends this report first; only then it deletes the
        # incoming user message, so no request remains visible in chat.
        enqueue(conn, f"status:{update_id}:req:{message_id}", chat_id, body)
        conn.commit()


async def delivery_worker() -> None:
    """Send replaceable status reports and retain delivered trading signals."""
    assert config is not None
    endpoint = f"https://api.telegram.org/bot{config.telegram_token}"
    async with httpx.AsyncClient(timeout=8) as client:
        while True:
            try:
                with closing(db_connect()) as conn:
                    rows = conn.execute(
                        "SELECT id,chat_id,body,attempts FROM outbox "
                        "WHERE available_at <= ? ORDER BY available_at,id LIMIT 20",
                        (time.time(),),
                    ).fetchall()
                for row in rows:
                    task_id = row["id"]
                    is_delete = task_id.startswith(
                        ("delete:", "delete_request:", "delete_replay:")
                    )
                    is_status = task_id.startswith("status:")
                    is_live_signal = task_id.startswith("adx:")
                    is_signal = is_live_signal or task_id.startswith("maer:")
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
                                    if is_live_signal:
                                        base_task = task_id.rsplit(":", 1)[0]
                                        conn.execute(
                                            "UPDATE adx_audit SET delivery_status='expired' "
                                            "WHERE task_id=? AND delivery_status='queued'",
                                            (base_task,),
                                        )
                                    conn.execute("DELETE FROM outbox WHERE id=?", (task_id,))
                                    conn.commit()
                                continue

                        payload = {"chat_id": row["chat_id"], "text": row["body"]}
                        if is_status:
                            with closing(db_connect()) as conn:
                                if is_authorized(conn, row["chat_id"]):
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
                                if is_live_signal:
                                    base_task = task_id.rsplit(":", 1)[0]
                                    remaining = conn.execute(
                                        "SELECT COUNT(*) FROM outbox WHERE id LIKE ? AND id != ?",
                                        (base_task + ":%", task_id),
                                    ).fetchone()[0]
                                    if remaining == 0:
                                        conn.execute(
                                            "UPDATE adx_audit SET delivery_status='sent' WHERE task_id=?",
                                            (base_task,),
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


app = FastAPI(title="Six-Market ADX Signaller", docs_url=None, redoc_url=None, lifespan=lifespan)


@app.get("/health")
async def health():
    return {"ok": True}


@app.post("/tradingview/{secret}")
async def tradingview_webhook(secret: str, request: Request):
    assert config is not None
    if not hmac.compare_digest(secret, config.tradingview_webhook_secret):
        raise HTTPException(status_code=403, detail="Forbidden")
    if int(request.headers.get("content-length", "0")) > 8000:
        raise HTTPException(status_code=413, detail="Payload too large")
    payload = None
    try:
        payload = await request.json()
        if not isinstance(payload, dict):
            raise ValueError("Expected JSON object")
        if payload.get("version") == 4:
            batch = MAERBatch.model_validate(payload)
            accepted = accept_maer_batch(batch)
            return {"ok": True, "version": 4, "new_readings": accepted}
        # Legacy alerts cannot generate or overwrite MAER observations.
        if payload.get("version") == 3:
            ADXReading.model_validate(payload).check_logic()
            return {"ok": True, "ignored_legacy": True}
        raise ValueError("unsupported protocol")


    except Exception as ex:
        # Safe diagnostics: NEVER log payload/body, URL (contains webhook
        # secret), token, user data, or values of fields. Report error classes
        # and pydantic field NAMES only.
        if isinstance(ex, ValidationError):
            fields = ",".join(
                ".".join(str(p) for p in error["loc"]) + ":" + error["type"]
                for error in ex.errors()
            )
        else:
            fields = "-"
        version = payload.get("version") if isinstance(payload, dict) else None
        # Only constant, explicitly approved error labels may reach logs:
        # never log incoming payloads, source strings, secrets or webhook paths.
        known_reasons = {
            "source must include an exchange/broker prefix": "missing_source_prefix",
            "NASDAQ must use the selected IG symbol": "nasdaq_feed_mismatch",
            "source does not match selected ADX data provider": "feed_mismatch",
            "bar_time is in the future": "future_bar_time",
        }
        reason = known_reasons.get(str(ex), "other") if isinstance(ex, ValueError) else "-"
        LOG.warning(
            "TradingView webhook rejected status=422 version=%s content_type=%s "
            "error=%s fields=%s reason=%s",
            version if isinstance(version, int) else "unknown",
            request.headers.get("content-type", "missing").split(";")[0][:40],
            type(ex).__name__,
            fields,
            reason,
        )
        raise HTTPException(status_code=422, detail=type(ex).__name__) from ex


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