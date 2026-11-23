"""No-account forex historical replay using Dukascopy's public tick feed.

The live TradingView indicator remains OANDA and is NOT changed. Historical
replay is an estimate using a DIFFERENT broker's midpoint tick prices.
One RSI 30/70 event per symbol until an intrabar RSI 50 touch resets it.
"""
from __future__ import annotations

import asyncio
import lzma
import logging
import os
from pathlib import Path
from email.utils import parsedate_to_datetime
import math
import struct
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import httpx

LOG = logging.getLogger("fx_rsi_historical_scan")

PAIR_SCALES = {"EURUSD": 100_000.0, "USDJPY": 1_000.0, "EURJPY": 1_000.0}
NY = ZoneInfo("America/New_York")
M5_MS = 300_000
HOUR_MS = 3_600_000
DAY_MS = 86_400_000
SCAN_HOURS = 24
WARMUP_ACTIVE_HOURS = 8
CACHE_DIR = Path(os.getenv("HISTORICAL_CACHE_DIR", "/data/dukascopy-cache"))
MIN_REQUEST_INTERVAL_SECONDS = 2.0
RATE_LIMIT_COOLDOWN_SECONDS = 300
URLS = (
    "https://datafeed.dukascopy.com/datafeed",
    "https://www.dukascopy.com/datafeed",
)


class ScanUnavailable(Exception):
    """Cannot complete a historical replay without usable market data."""


class ScanRateLimited(ScanUnavailable):
    """Dukascopy has asked us to stop requesting data temporarily."""


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


class WilderM5:
    """Exact Wilder RSI smoothing on completed M5 closes; provisional on ticks."""

    def __init__(self, length: int = 14):
        self.length = length
        self.previous_close: float | None = None
        self.avg_gain: float | None = None
        self.avg_loss: float | None = None
        self.gains: list[float] = []
        self.losses: list[float] = []

    def _change(self, value: float) -> tuple[float, float]:
        assert self.previous_close is not None
        delta = value - self.previous_close
        return max(0.0, delta), max(0.0, -delta)

    def _rsi(self, gain: float, loss: float) -> float:
        if loss == 0:
            return 100.0 if gain > 0 else 50.0
        if gain == 0:
            return 0.0
        return 100 - 100 / (1 + gain / loss)

    def estimate(self, price: float) -> float | None:
        if self.previous_close is None or self.avg_gain is None:
            return None
        gain, loss = self._change(price)
        n = self.length
        return self._rsi(
            (self.avg_gain * (n - 1) + gain) / n,
            (self.avg_loss * (n - 1) + loss) / n,
        )

    def commit(self, close: float) -> float | None:
        if self.previous_close is None:
            self.previous_close = close
            return None
        gain, loss = self._change(close)
        self.previous_close = close
        if self.avg_gain is None:
            self.gains.append(gain)
            self.losses.append(loss)
            if len(self.gains) >= self.length:
                self.avg_gain = sum(self.gains[-self.length:]) / self.length
                self.avg_loss = sum(self.losses[-self.length:]) / self.length
            else:
                return None
        else:
            n = self.length
            self.avg_gain = (self.avg_gain * (n - 1) + gain) / n
            self.avg_loss = (self.avg_loss * (n - 1) + loss) / n
        return self._rsi(self.avg_gain, self.avg_loss)


def _touch50(prev: float, cur: float) -> bool:
    return prev < 50 <= cur or prev > 50 >= cur


def replay_one(ticker: str, ticks: list[tuple[int, float]],
               start_ms: int, end_ms: int) -> list[ScanSignal]:
    calc = WilderM5()
    armed = True
    prev_rsi: float | None = None
    bucket: int | None = None
    prev_close: float | None = None
    events: list[ScanSignal] = []

    for event_ms, price in sorted(ticks):
        if event_ms > end_ms:
            break
        next_bucket = event_ms // M5_MS
        if bucket is not None and next_bucket != bucket:
            assert prev_close is not None
            calc.commit(prev_close)
        bucket = next_bucket
        prev_close = price
        rsi = calc.estimate(price)
        if rsi is None:
            continue
        if prev_rsi is not None:
            # RSI 50 touch immediately resets, regardless of M5 close.
            if _touch50(prev_rsi, rsi):
                armed = True
            if armed:
                level = (70 if prev_rsi < 70 <= rsi else
                         30 if prev_rsi > 30 >= rsi else None)
                if level is not None:
                    armed = False
                    if start_ms <= event_ms <= end_ms:
                        events.append(ScanSignal(ticker, level, event_ms))
        prev_rsi = rsi
    return events


def _possibly_open(dt: datetime) -> bool:
    """Filter market-closed hours using New York time, including EST/EDT."""
    ny = dt.astimezone(NY)
    day = ny.weekday()
    if day == 5:
        return False
    if day == 4 and ny.hour >= 17:
        return False
    if day == 6 and ny.hour < 17:
        return False
    return True


def _hours_to_read(start_ms: int, end_ms: int) -> list[int]:
    first = (start_ms // HOUR_MS) * HOUR_MS
    last = (end_ms // HOUR_MS) * HOUR_MS
    hours: list[int] = []
    hour = first
    while hour <= last:
        hours.append(hour)
        hour += HOUR_MS

    # M5 Wilder RSI and the per-symbol lockout need a warmup across
    # weekends. Collect the preceding 8 potentially active hours,
    # going back at most 7 days. Do not request empty Saturday files.
    wanted = WARMUP_ACTIVE_HOURS
    hour = first - HOUR_MS
    for _ in range(24 * 7):
        dt = datetime.fromtimestamp(hour / 1000, timezone.utc)
        if _possibly_open(dt):
            hours.append(hour)
            wanted -= 1
            if wanted == 0:
                break
        hour -= HOUR_MS
    return sorted(set(
        h for h in hours
        if _possibly_open(datetime.fromtimestamp(h / 1000, timezone.utc))
    ))


def _decode_tick_hour(data: bytes, hour_ms: int, scale: float
                      ) -> list[tuple[int, float]]:
    if not data:
        # Dukascopy frequently returns 200 with ZERO bytes for a closed
        # market hour or an hour whose tick archive is not published yet.
        return []
    if len(data) > 15_000_000:
        raise ScanUnavailable("Historical tick download exceeded safe size.")
    try:
        raw = lzma.decompress(data)
    except (lzma.LZMAError, EOFError) as ex:
        raise ScanUnavailable("Historical data source returned a non-bi5/corrupt tick file.") from ex
    if len(raw) > 150_000_000 or len(raw) % 20:
        raise ScanUnavailable("Historical tick feed has an invalid record size.")
    samples = []
    for ms_into_hour, ask, bid, _, _ in struct.iter_unpack(">IIIff", raw):
        if ms_into_hour >= HOUR_MS or ask == 0 or bid == 0:
            continue
        mid = (ask + bid) / (2.0 * scale)
        if math.isfinite(mid):
            samples.append((hour_ms + ms_into_hour, mid))
    return samples


def _cached_hour_path(ticker: str, hour_ms: int) -> Path:
    return CACHE_DIR / ticker / f"{hour_ms}.bi5"


def _cached_rate_limit_end() -> float:
    path = CACHE_DIR / "cooldown_until.txt"
    try:
        return float(path.read_text().strip())
    except (OSError, ValueError):
        return 0.0


def _set_rate_limit_end(duration_seconds: float) -> None:
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        until = max(time.time() + duration_seconds, _cached_rate_limit_end())
        (CACHE_DIR / "cooldown_until.txt").write_text(str(until))
    except OSError:
        LOG.warning("Unable to persist Dukascopy cooldown")


def _retry_after_seconds(value: str | None) -> float:
    if not value:
        return 30.0
    try:
        return max(1.0, float(value))
    except ValueError:
        try:
            return max(1.0, parsedate_to_datetime(value).timestamp() - time.time())
        except (TypeError, ValueError, OverflowError):
            return 30.0


async def _read_hour(client: httpx.AsyncClient,
                     ticker: str, hour_ms: int
                     ) -> list[tuple[int, float]]:
    """Fetch conservatively; never switch hosts in order to evade HTTP 429."""
    dt = datetime.fromtimestamp(hour_ms / 1000, timezone.utc)
    path = (f"{ticker}/{dt.year}/{dt.month - 1:02d}/"
            f"{dt.day:02d}/{dt.hour:02d}h_ticks.bi5")
    cached = _cached_hour_path(ticker, hour_ms)
    # Closed historical hours are immutable and may be reused across scans.
    is_final_hour = hour_ms + HOUR_MS < int(time.time() * 1000) - 10 * 60 * 1000
    if is_final_hour:
        try:
            if cached.exists():
                return _decode_tick_hour(cached.read_bytes(), hour_ms,
                                         PAIR_SCALES[ticker])
        except (OSError, ScanUnavailable):
            LOG.warning("Discarding invalid cached data ticker=%s hour=%s",
                        ticker, dt.strftime("%Y-%m-%d %H UTC"))
            try:
                cached.unlink(missing_ok=True)
            except OSError:
                pass

    cooldown = _cached_rate_limit_end() - time.time()
    if cooldown > 0:
        raise ScanRateLimited(
            f"Dukascopy is rate-limiting historical requests. "
            f"Please wait about {math.ceil(cooldown / 60)} minute(s) before retrying."
        )

    last_status: int | None = None
    for base in URLS:
        # A secondary hostname is only used after a non-429 transport error
        # or invalid archive, never to circumvent a provider rate limit.
        for attempt in range(2):
            try:
                response = await client.get(
                    f"{base}/{path}", follow_redirects=True,
                    headers={"Accept": "application/octet-stream"},
                )
            except (httpx.RequestError, httpx.TimeoutException):
                LOG.warning("Historical transport error ticker=%s hour=%s",
                            ticker, dt.strftime("%Y-%m-%d %H UTC"))
                break
            last_status = response.status_code
            if response.status_code == 429:
                wait = _retry_after_seconds(response.headers.get("retry-after"))
                LOG.warning("Dukascopy HTTP 429 ticker=%s hour=%s retry_s=%.0f",
                            ticker, dt.strftime("%Y-%m-%d %H UTC"), wait)
                # Respect large Retry-After values rather than spin-wait or
                # try a second host. Persist delay across bot restarts.
                if wait > 90 or attempt == 1:
                    _set_rate_limit_end(max(RATE_LIMIT_COOLDOWN_SECONDS, wait))
                    raise ScanRateLimited(
                        "Dukascopy has temporarily blocked historical downloads "
                        "with HTTP 429. The scanner is pausing requests to respect "
                        "the provider's limit. Please retry later."
                    )
                await asyncio.sleep(wait)
                continue
            if response.status_code == 404:
                return []
            if response.status_code != 200:
                LOG.warning("Historical HTTP %s ticker=%s hour=%s",
                            response.status_code, ticker,
                            dt.strftime("%Y-%m-%d %H UTC"))
                break
            try:
                ticks = _decode_tick_hour(response.content, hour_ms,
                                          PAIR_SCALES[ticker])
            except ScanUnavailable:
                LOG.warning("Malformed .bi5 archive ticker=%s hour=%s bytes=%s",
                            ticker, dt.strftime("%Y-%m-%d %H UTC"),
                            len(response.content))
                break
            # A successful empty file may be an archive not published yet.
            # Never cache it as permanently empty.
            if is_final_hour and response.content:
                try:
                    cached.parent.mkdir(parents=True, exist_ok=True)
                    temp = cached.with_suffix(".tmp")
                    temp.write_bytes(response.content)
                    temp.replace(cached)
                except OSError:
                    LOG.warning("Could not cache historical tick hour")
            return ticks

    raise ScanUnavailable(
        "Dukascopy did not provide a valid hourly tick file"
        + (f" (last HTTP {last_status})." if last_status else ".")
    )


async def scan_last_24h() -> tuple[list[ScanSignal], float, float]:
    """Paced, cached, honest historical replay of available last-24h ticks."""
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - SCAN_HOURS * 60 * 60 * 1000
    hours = _hours_to_read(start_ms, end_ms)
    if not hours:
        raise ScanUnavailable("There are no trading hours within the scan window.")

    # One request at a time; no concurrent burst at the public provider.
    # Keep each successfully downloaded historic hour on the Railway volume
    # so repeated scans fetch only newly available data.
    timeout = httpx.Timeout(30.0, connect=10.0)
    results: list[ScanSignal] = []
    total_ticks = 0
    async with httpx.AsyncClient(timeout=timeout) as client:
        for ticker in PAIR_SCALES:
            ticks: list[tuple[int, float]] = []
            for hour in hours:
                cached = _cached_hour_path(ticker, hour)
                was_cached = cached.exists()
                chunk = await _read_hour(client, ticker, hour)
                ticks.extend(chunk)
                if not was_cached:
                    await asyncio.sleep(MIN_REQUEST_INTERVAL_SECONDS)
            total_ticks += len(ticks)
            results.extend(replay_one(ticker, ticks, start_ms, end_ms))
    if total_ticks == 0:
        raise ScanUnavailable(
            "No historical ticks were available. Provider may be delayed or market closed."
        )
    return sorted(results, key=lambda s: (s.time_ms, s.ticker)), (
        start_ms / 1000), (end_ms / 1000)
