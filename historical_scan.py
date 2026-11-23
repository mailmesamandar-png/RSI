"""Read-only, approximate 24h RSI touch replay from OANDA S5 midpoint candles.

Live strategy: RSI(14) is M5, recomputed intrabar from last completed
M5 Wilder smoothing and the forming M5 price. ONE 70/30 event per pair until
the RSI touches 50, which immediately resets the pair (no candle close).

Historical 5-second candles are an APPROXIMATION, not historical ticks.
Never use scan events to update the live alert model's status or lockout.
"""
from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx

PAIR_TO_OANDA = {"EURUSD": "EUR_USD", "USDJPY": "USD_JPY", "EURJPY": "EUR_JPY"}
NY = ZoneInfo("America/New_York")
M5_SECONDS = 300
S5_SECONDS = 5
RSI_LENGTH = 14
MAX_WINDOW_SECONDS = 24 * 60 * 60
SCAN_WARMUP_BARS = 180


class ScanUnavailable(Exception):
    """Unavailable OANDA authentication, data, or pricing."""


@dataclass(frozen=True)
class ScanSignal:
    ticker: str
    level: int
    time_ms: int

    @property
    def ny_time(self) -> str:
        return datetime.fromtimestamp(
            self.time_ms / 1000, timezone.utc
        ).astimezone(NY).strftime("%Y-%m-%d %H:%M:%S %Z")


def _timestamp(value: str) -> float:
    """OANDA uses RFC3339 timestamps, sometimes with 9 digits of fractions."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _utc_string(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat(
        timespec="seconds"
    ).replace("+00:00", "Z")


def _rsi(gain: float, loss: float) -> float:
    if loss == 0:
        return 100.0 if gain > 0 else 50.0
    if gain == 0:
        return 0.0
    return 100.0 - 100.0 / (1.0 + gain / loss)


class WilderM5:
    def __init__(self, length: int = RSI_LENGTH) -> None:
        self.length = length
        self.previous_close: float | None = None
        self.avg_gain: float | None = None
        self.avg_loss: float | None = None
        self._gains: list[float] = []
        self._losses: list[float] = []

    def _delta(self, price: float) -> tuple[float, float]:
        assert self.previous_close is not None
        diff = price - self.previous_close
        return max(diff, 0.0), max(-diff, 0.0)

    def estimate(self, price: float) -> float | None:
        """RSI at a sampled tick of the currently forming M5 candle."""
        if self.previous_close is None or self.avg_gain is None:
            return None
        g, l = self._delta(price)
        n = self.length
        return _rsi((self.avg_gain * (n - 1) + g) / n,
                    (self.avg_loss * (n - 1) + l) / n)

    def commit(self, price: float) -> float | None:
        """Update Wilder smoothing only once when M5 candle is complete."""
        if self.previous_close is None:
            self.previous_close = price
            return None
        gain, loss = self._delta(price)
        self.previous_close = price
        if self.avg_gain is None:
            self._gains.append(gain)
            self._losses.append(loss)
            if len(self._gains) == self.length:
                self.avg_gain = sum(self._gains) / self.length
                self.avg_loss = sum(self._losses) / self.length
                return _rsi(self.avg_gain, self.avg_loss)
            return None
        n = self.length
        self.avg_gain = (self.avg_gain * (n - 1) + gain) / n
        self.avg_loss = (self.avg_loss * (n - 1) + loss) / n
        return _rsi(self.avg_gain, self.avg_loss)


def _touches(prev: float, current: float, value: int) -> bool:
    if value == 50:
        return ((prev < 50 <= current) or (prev > 50 >= current))
    if value == 70:
        return prev < 70 <= current
    return prev > 30 >= current


def replay_one(
    ticker: str,
    warmup_m5: list[tuple[float, float]],
    samples_s5: list[tuple[float, float]],
    start: float,
    end: float,
) -> list[ScanSignal]:
    """Run independent state machine for ONE symbol over S5 sample closes."""
    calc = WilderM5()
    prev_rsi: float | None = None
    armed = True

    # Approximate the left-edge locked/unlocked state from confirmed M5 bars.
    # True historic intrabar events before this window are not reconstructible.
    for ts, price in sorted(warmup_m5):
        if ts + M5_SECONDS > (int(start // M5_SECONDS) * M5_SECONDS):
            continue
        rsi = calc.commit(price)
        if rsi is None:
            continue
        if prev_rsi is not None:
            if _touches(prev_rsi, rsi, 50):
                armed = True
            elif armed and (_touches(prev_rsi, rsi, 70) or
                            _touches(prev_rsi, rsi, 30)):
                armed = False
        prev_rsi = rsi

    current_bucket: int | None = None
    last_close: float | None = None
    results: list[ScanSignal] = []
    for ts, price in sorted(samples_s5):
        candle_close = ts + S5_SECONDS
        if candle_close < start or candle_close > end:
            continue
        bucket = int(ts // M5_SECONDS)
        if current_bucket is not None and bucket != current_bucket:
            assert last_close is not None
            calc.commit(last_close)
        current_bucket = bucket
        last_close = price
        rsi = calc.estimate(price)
        if rsi is None:
            continue
        if prev_rsi is not None:
            if _touches(prev_rsi, rsi, 50):
                armed = True
            if armed:
                level = 70 if _touches(prev_rsi, rsi, 70) else (
                    30 if _touches(prev_rsi, rsi, 30) else None
                )
                if level is not None:
                    results.append(ScanSignal(ticker, level,
                                              int(round(candle_close * 1000))))
                    armed = False
        prev_rsi = rsi
    return results


def _scan_config() -> tuple[str, str]:
    token = os.getenv("OANDA_API_TOKEN", "").strip()
    if not token:
        raise ScanUnavailable(
            "Historical scan needs OANDA_API_TOKEN in Railway Variables. "
            "Add a personal OANDA v20 API token there; never send it in Telegram or chat."
        )
    env = os.getenv("OANDA_API_ENVIRONMENT", "practice").strip().lower()
    if env not in ("practice", "live"):
        raise ScanUnavailable("OANDA_API_ENVIRONMENT must be 'practice' or 'live'.")
    host = ("https://api-fxpractice.oanda.com" if env == "practice"
            else "https://api-fxtrade.oanda.com")
    return token, host


async def _request_candles(
    client: httpx.AsyncClient, token: str, host: str,
    instrument: str, params: dict,
) -> list[dict]:
    endpoint = f"{host}/v3/instruments/{instrument}/candles"
    try:
        response = await client.get(
            endpoint,
            headers={"Authorization": f"Bearer {token}", "Accept-Datetime-Format": "RFC3339"},
            params=params,
        )
        if response.status_code in (401, 403):
            raise ScanUnavailable("OANDA API denied access (401/403). Check the token and practice/live environment in Railway.")
        if response.status_code == 429:
            raise ScanUnavailable("OANDA rate limit reached. Please wait and try scanning again.")
        if response.status_code != 200:
            raise ScanUnavailable(f"OANDA historical data request failed (HTTP {response.status_code}).")
        body = response.json()
        if not isinstance(body.get("candles"), list):
            raise ScanUnavailable("OANDA response is missing candle data.")
        return body["candles"]
    except httpx.TimeoutException as ex:
        raise ScanUnavailable("OANDA candle retrieval timed out. Try again.") from ex
    except httpx.RequestError as ex:
        raise ScanUnavailable("Cannot reach OANDA historical-data server.") from ex


def _parse_candles(items: list[dict]) -> list[tuple[float, float]]:
    rows: list[tuple[float, float]] = []
    for item in items:
        if not item.get("complete", False):
            continue
        midpoint = item.get("mid") or {}
        if midpoint.get("c") is None or item.get("time") is None:
            continue
        rows.append((_timestamp(str(item["time"])), float(midpoint["c"])))
    return rows


async def _get_pair(
    client: httpx.AsyncClient, token: str, host: str,
    ticker: str, start: float, end: float
) -> list[ScanSignal]:
    instrument = PAIR_TO_OANDA[ticker]
    aligned_start = int(start // M5_SECONDS) * M5_SECONDS
    warmup = _parse_candles(await _request_candles(
        client, token, host, instrument,
        {"granularity": "M5", "price": "M",
         "count": SCAN_WARMUP_BARS, "to": _utc_string(aligned_start)},
    ))
    # OANDA permits up to 5000 candles per request. Each 5h partition
    # contains no more than 3600 S5 bars, even during a 24h open market.
    raw: dict[float, float] = {}
    cursor = start
    while cursor < end:
        stop = min(cursor + 5 * 60 * 60, end)
        for t, close in _parse_candles(await _request_candles(
            client, token, host, instrument,
            {"granularity": "S5", "price": "M",
             "from": _utc_string(cursor), "to": _utc_string(stop)},
        )):
            raw[t] = close
        cursor = stop
    return replay_one(ticker, warmup, sorted(raw.items()), start, end)


async def scan_last_24h() -> tuple[list[ScanSignal], float, float]:
    """Get the available last 24h; weekend/closed hours naturally have no data."""
    token, host = _scan_config()
    end = time.time()
    start = end - MAX_WINDOW_SECONDS
    timeout = httpx.Timeout(35.0, connect=10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        # Serialized across three pairs to avoid hitting OANDA rate limits.
        signals = []
        for ticker in PAIR_TO_OANDA:
            signals.extend(await _get_pair(client, token, host, ticker, start, end))
    return sorted(signals, key=lambda v: (v.time_ms, v.ticker)), start, end
