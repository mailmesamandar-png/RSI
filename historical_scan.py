"""No-account forex historical replay using Dukascopy's public tick feed.

The live TradingView indicator remains OANDA and is NOT changed. Historical
replay is an estimate using a DIFFERENT broker's midpoint tick prices.
One RSI 30/70 event per symbol until an intrabar RSI 50 touch resets it.
"""
from __future__ import annotations

import asyncio
import lzma
import logging
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
URLS = (
    "https://datafeed.dukascopy.com/datafeed",
    "https://www.dukascopy.com/datafeed",
)


class ScanUnavailable(Exception):
    """Cannot complete a historical replay without usable market data."""


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
    # Conservative weekend screening. Empty hours are still allowed if
    # provider reports none, and daylight-saving shift may vary by week.
    day = dt.weekday()
    if day == 5:
        return False
    if day == 6 and dt.hour < 20:
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


async def _read_hour(client: httpx.AsyncClient, semaphore: asyncio.Semaphore,
                     ticker: str, hour_ms: int) -> list[tuple[int, float]]:
    dt = datetime.fromtimestamp(hour_ms / 1000, timezone.utc)
    path = (f"{ticker}/{dt.year}/{dt.month - 1:02d}/"
            f"{dt.day:02d}/{dt.hour:02d}h_ticks.bi5")
    bad_hosts = 0
    last_status: int | None = None

    async with semaphore:
        for base in URLS:
            try:
                response = await client.get(
                    f"{base}/{path}", follow_redirects=True,
                    headers={"Accept": "application/octet-stream"},
                )
            except (httpx.RequestError, httpx.TimeoutException):
                LOG.warning("Historical feed transport error ticker=%s hour=%s",
                            ticker, dt.strftime("%Y-%m-%d %H UTC"))
                continue

            last_status = response.status_code
            if response.status_code == 404:
                # A 404 normally identifies a truly missing archive.
                # Unlike a malformed 200, this is not a parser failure.
                return []
            if response.status_code != 200:
                LOG.warning("Historical feed non-200 ticker=%s hour=%s status=%d",
                            ticker, dt.strftime("%Y-%m-%d %H UTC"), response.status_code)
                continue

            if not response.content:
                # This IS normal, and is the key fix for Monday/weekend scans:
                # HTTP 200 + empty body is no ticks, NOT an LZMA archive.
                return []

            try:
                return _decode_tick_hour(response.content, hour_ms,
                                         PAIR_SCALES[ticker])
            except ScanUnavailable:
                bad_hosts += 1
                LOG.warning(
                    "Historical feed invalid tick format ticker=%s hour=%s "
                    "bytes=%d content_type=%s",
                    ticker, dt.strftime("%Y-%m-%d %H UTC"),
                    len(response.content),
                    response.headers.get("content-type", "unknown")[:60],
                )
                # A CDN can return HTML with HTTP 200 instead of .bi5.
                # Try the alternate feed host before failing.
                continue

    if bad_hosts:
        raise ScanUnavailable(
            "Dukascopy returned non-bi5 data for at least one hour, including "
            "the backup server. Please retry later."
        )
    raise ScanUnavailable(
        "Dukascopy historical feed is unreachable or rate-limited"
        + (f" (last HTTP {last_status})." if last_status else ".")
    )

async def scan_last_24h() -> tuple[list[ScanSignal], float, float]:
    """Fetch public Dukascopy ticks for the last available 24h and replay M5 RSI."""
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - SCAN_HOURS * 60 * 60 * 1000
    hours = _hours_to_read(start_ms, end_ms)
    if not hours:
        raise ScanUnavailable("No accessible trading hours in the requested period.")
    semaphore = asyncio.Semaphore(6)
    timeout = httpx.Timeout(25.0, connect=10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        # Limit memory: download/replay one instrument at a time.
        results: list[ScanSignal] = []
        total_ticks = 0
        for ticker in PAIR_SCALES:
            hour_data = await asyncio.gather(
                *(_read_hour(client, semaphore, ticker, hour) for hour in hours)
            )
            ticks = [sample for chunk in hour_data for sample in chunk]
            total_ticks += len(ticks)
            results.extend(replay_one(ticker, ticks, start_ms, end_ms))
            del hour_data, ticks

    if total_ticks == 0:
        raise ScanUnavailable(
            "No ticks were returned. The market may be closed or historical data "
            "may not yet have been published."
        )
    return sorted(results, key=lambda s: (s.time_ms, s.ticker)), (
        start_ms / 1000), (end_ms / 1000)
