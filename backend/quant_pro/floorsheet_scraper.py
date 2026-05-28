"""Merolagani floorsheet scraper and broker-flow aggregation.

The public TUI expects broker flow data in SQLite but the scraper entry point
was missing. This module provides an idempotent ingestion surface for the CLI
in ``backend.quant_pro.data_scrapers.floorsheet_ingestion``.
"""

from __future__ import annotations

import logging
import re
import sqlite3
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable

import requests
from bs4 import BeautifulSoup

from backend.quant_pro.database import get_db_connection

LOG = logging.getLogger(__name__)

BASE_URL = "https://merolagani.com/Floorsheet.aspx"
AUTOSUGGEST_URL = "https://merolagani.com/handlers/AutoSuggestHandler.ashx?type=Company"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

MIN_SIGNAL_TRADES = 20
MIN_SIGNAL_VOLUME = 5000
NET_RATIO_MAX = 0.15
MIN_CIRC_SHARE = 0.05


@dataclass(frozen=True)
class FloorTrade:
    transact_no: str
    symbol: str
    as_of_date: str
    buyer_broker: int
    seller_broker: int
    quantity: int
    rate: float
    amount: float
    source_url: str
    scraped_at_utc: str


def ensure_floorsheet_tables(conn: sqlite3.Connection | None = None) -> None:
    """Create floorsheet and broker-flow tables without dropping existing data."""
    own_conn = conn is None
    if conn is None:
        conn = get_db_connection()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS floorsheet_trades (
                transact_no TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                buyer_broker INTEGER NOT NULL,
                seller_broker INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                rate REAL NOT NULL,
                amount REAL NOT NULL,
                source_url TEXT,
                scraped_at_utc TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_floorsheet_symbol_date ON floorsheet_trades(symbol, as_of_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_floorsheet_buyer ON floorsheet_trades(symbol, as_of_date, buyer_broker)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_floorsheet_seller ON floorsheet_trades(symbol, as_of_date, seller_broker)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS broker_summary (
                symbol TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                broker_code INTEGER NOT NULL,
                buy_qty INTEGER NOT NULL,
                sell_qty INTEGER NOT NULL,
                net_qty INTEGER NOT NULL,
                buy_amount REAL NOT NULL,
                sell_amount REAL NOT NULL,
                net_amount REAL NOT NULL,
                buy_trades INTEGER NOT NULL,
                sell_trades INTEGER NOT NULL,
                total_trades INTEGER NOT NULL,
                PRIMARY KEY (symbol, as_of_date, broker_code)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS broker_signal_scores (
                symbol TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                total_trades INTEGER NOT NULL,
                total_qty INTEGER NOT NULL,
                total_amount REAL NOT NULL,
                top1_net_share REAL,
                top5_net_share REAL,
                hhi_net REAL,
                accumulation_score REAL,
                flags TEXT,
                PRIMARY KEY (symbol, as_of_date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS broker_signals_v2 (
                symbol TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                hhi_buy REAL,
                hhi_sell REAL,
                hhi_buy_norm REAL,
                hhi_sell_norm REAL,
                circular_score REAL,
                top_pair_pct REAL,
                self_trade_pct REAL,
                smart_money_score REAL,
                pump_score REAL,
                total_volume INTEGER,
                n_trades INTEGER,
                n_brokers_buy INTEGER,
                n_brokers_sell INTEGER,
                PRIMARY KEY (symbol, as_of_date)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bsv2_date ON broker_signals_v2(as_of_date)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS broker_microstructure (
                symbol TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                amihud_illiq REAL,
                roll_spread REAL,
                cs_spread REAL,
                kyle_lambda REAL,
                kyle_pvalue REAL,
                kyle_rsq REAL,
                kyle_cwoib REAL,
                kyle_significant INTEGER,
                pin_proxy REAL,
                micro_score REAL,
                PRIMARY KEY (symbol, as_of_date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS floorsheet_scrape_log (
                as_of_date TEXT PRIMARY KEY,
                total_rows INTEGER NOT NULL,
                total_pages INTEGER NOT NULL,
                scraped_at_utc TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        if own_conn:
            conn.close()


def _money(text: object) -> float:
    return float(str(text or "0").replace(",", "").strip() or 0)


def _int(text: object) -> int:
    return int(round(_money(text)))


def _date_for_form(value: date | str) -> str:
    parsed = date.fromisoformat(str(value)) if not isinstance(value, date) else value
    return parsed.strftime("%m/%d/%Y")


def _date_key(value: date | str) -> str:
    parsed = date.fromisoformat(str(value)) if not isinstance(value, date) else value
    return parsed.isoformat()


def _hidden_form(soup: BeautifulSoup) -> dict[str, str]:
    data: dict[str, str] = {}
    for inp in soup.find_all("input"):
        name = inp.get("name")
        if name and inp.get("type") != "submit":
            data[name] = inp.get("value", "")
    data.setdefault("__EVENTTARGET", "")
    data.setdefault("__EVENTARGUMENT", "")
    return data


def _symbol_ids(session: requests.Session) -> dict[str, str]:
    response = session.post(
        AUTOSUGGEST_URL,
        data={"value": "", "sector": 0},
        headers={**HEADERS, "X-Requested-With": "XMLHttpRequest"},
        timeout=30,
    )
    response.raise_for_status()
    out: dict[str, str] = {}
    for row in response.json():
        sym = str(row.get("d") or "").upper().strip()
        val = str(row.get("v") or "").strip()
        if sym and val:
            out[sym] = val
    return out


def _parse_records(html: str, *, as_of_date: str, source_url: str) -> tuple[list[FloorTrade], int | None]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if table is None:
        return [], None

    scraped_at = datetime.utcnow().replace(microsecond=0).isoformat()
    trades: list[FloorTrade] = []
    for tr in table.find_all("tr")[1:]:
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cells) < 8 or not cells[1].strip():
            continue
        try:
            trades.append(
                FloorTrade(
                    transact_no=str(cells[1]),
                    symbol=str(cells[2]).upper().strip(),
                    as_of_date=as_of_date,
                    buyer_broker=_int(cells[3]),
                    seller_broker=_int(cells[4]),
                    quantity=_int(cells[5]),
                    rate=_money(cells[6]),
                    amount=_money(cells[7]),
                    source_url=source_url,
                    scraped_at_utc=scraped_at,
                )
            )
        except (TypeError, ValueError):
            continue

    pager = soup.find(id=re.compile(r"litRecords$"))
    total_pages = None
    if pager:
        match = re.search(r"Total pages:\s*(\d+)", pager.get_text(" ", strip=True))
        if match:
            total_pages = int(match.group(1))
    return trades, total_pages


def _search_page(
    session: requests.Session,
    *,
    symbol: str,
    symbol_id: str,
    target_date: date | str,
) -> tuple[str, str]:
    first = session.get(BASE_URL, headers=HEADERS, timeout=30)
    first.raise_for_status()
    data = _hidden_form(BeautifulSoup(first.text, "html.parser"))
    data["__EVENTTARGET"] = "ctl00$ContentPlaceHolder1$lbtnSearchFloorsheet"
    data["ctl00$ContentPlaceHolder1$ASCompanyFilter$hdnAutoSuggest"] = str(symbol_id)
    data["ctl00$ContentPlaceHolder1$ASCompanyFilter$txtAutoSuggest"] = symbol.upper()
    data["ctl00$ContentPlaceHolder1$txtBuyerBrokerCodeFilter"] = ""
    data["ctl00$ContentPlaceHolder1$txtSellerBrokerCodeFilter"] = ""
    data["ctl00$ContentPlaceHolder1$txtFloorsheetDateFilter"] = _date_for_form(target_date)
    response = session.post(
        BASE_URL,
        data=data,
        headers={**HEADERS, "Referer": BASE_URL, "Origin": "https://merolagani.com"},
        timeout=45,
    )
    response.raise_for_status()
    return response.text, BASE_URL


def _search_date_page(session: requests.Session, *, target_date: date | str) -> tuple[str, str]:
    first = session.get(BASE_URL, headers=HEADERS, timeout=30)
    first.raise_for_status()
    data = _hidden_form(BeautifulSoup(first.text, "html.parser"))
    data["__EVENTTARGET"] = "ctl00$ContentPlaceHolder1$lbtnSearchFloorsheet"
    data["ctl00$ContentPlaceHolder1$ASCompanyFilter$hdnAutoSuggest"] = ""
    data["ctl00$ContentPlaceHolder1$ASCompanyFilter$txtAutoSuggest"] = ""
    data["ctl00$ContentPlaceHolder1$txtBuyerBrokerCodeFilter"] = ""
    data["ctl00$ContentPlaceHolder1$txtSellerBrokerCodeFilter"] = ""
    data["ctl00$ContentPlaceHolder1$txtFloorsheetDateFilter"] = _date_for_form(target_date)
    response = session.post(
        BASE_URL,
        data=data,
        headers={**HEADERS, "Referer": BASE_URL, "Origin": "https://merolagani.com"},
        timeout=45,
    )
    response.raise_for_status()
    return response.text, BASE_URL


def _page(session: requests.Session, html: str, page_no: int) -> str:
    data = _hidden_form(BeautifulSoup(html, "html.parser"))
    data["ctl00$ContentPlaceHolder1$PagerControl1$hdnCurrentPage"] = str(page_no)
    data["ctl00$ContentPlaceHolder1$PagerControl1$btnPaging"] = ""
    response = session.post(
        BASE_URL,
        data=data,
        headers={**HEADERS, "Referer": BASE_URL, "Origin": "https://merolagani.com"},
        timeout=45,
    )
    response.raise_for_status()
    return response.text


def _fetch_date_all(target_date: date | str, *, max_workers: int = 4) -> tuple[list[FloorTrade], int]:
    session = requests.Session()
    html, url = _search_date_page(session, target_date=target_date)
    as_of_date = _date_key(target_date)
    trades, total_pages = _parse_records(html, as_of_date=as_of_date, source_url=url)
    pages = int(total_pages or 1)

    def fetch_page(page_no: int) -> tuple[int, list[FloorTrade]]:
        page_session = requests.Session()
        page_html = _page(page_session, html, page_no)
        page_trades, _ = _parse_records(page_html, as_of_date=as_of_date, source_url=url)
        return page_no, page_trades

    if pages > 1:
        page_rows: dict[int, list[FloorTrade]] = {}
        with ThreadPoolExecutor(max_workers=max(1, int(max_workers or 1))) as pool:
            futures = [pool.submit(fetch_page, page_no) for page_no in range(2, pages + 1)]
            for fut in as_completed(futures):
                page_no, page_trades = fut.result()
                page_rows[page_no] = page_trades
        for page_no in range(2, pages + 1):
            trades.extend(page_rows.get(page_no, []))
    return trades, pages


def _fetch_symbol_date(symbol: str, symbol_id: str, target_date: date | str) -> list[FloorTrade]:
    session = requests.Session()
    html, url = _search_page(session, symbol=symbol, symbol_id=symbol_id, target_date=target_date)
    as_of_date = _date_key(target_date)
    trades, total_pages = _parse_records(html, as_of_date=as_of_date, source_url=url)
    current_html = html
    for page_no in range(2, int(total_pages or 1) + 1):
        current_html = _page(session, current_html, page_no)
        page_trades, _ = _parse_records(current_html, as_of_date=as_of_date, source_url=url)
        trades.extend(page_trades)
    return [trade for trade in trades if trade.symbol == symbol.upper()]


def _existing(conn: sqlite3.Connection, symbol: str, as_of_date: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM floorsheet_trades WHERE symbol = ? AND as_of_date = ? LIMIT 1",
        (symbol.upper(), as_of_date),
    ).fetchone()
    return row is not None


def _insert_trades(conn: sqlite3.Connection, trades: Iterable[FloorTrade]) -> int:
    rows = [
        (
            trade.transact_no,
            trade.symbol,
            trade.as_of_date,
            trade.buyer_broker,
            trade.seller_broker,
            trade.quantity,
            trade.rate,
            trade.amount,
            trade.source_url,
            trade.scraped_at_utc,
        )
        for trade in trades
    ]
    if not rows:
        return 0
    before = conn.total_changes
    conn.executemany(
        """
        INSERT OR IGNORE INTO floorsheet_trades
        (transact_no, symbol, as_of_date, buyer_broker, seller_broker, quantity, rate, amount, source_url, scraped_at_utc)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return conn.total_changes - before


def _hhi(values: Iterable[float]) -> float:
    clean = [float(value or 0.0) for value in values if float(value or 0.0) > 0.0]
    total = sum(clean)
    if total <= 0:
        return 0.0
    return sum((value / total) ** 2 for value in clean)


def _hhi_normalized(values: Iterable[float]) -> tuple[float, float, int]:
    clean = [float(value or 0.0) for value in values if float(value or 0.0) > 0.0]
    n = len(clean)
    hhi = _hhi(clean)
    if n == 0:
        return 0.0, 0.0, 0
    if n == 1:
        return hhi, 1.0, n
    norm = (hhi - 1.0 / n) / (1.0 - 1.0 / n)
    return hhi, max(0.0, min(1.0, norm)), n


def _compute_signal_metrics(
    raw_rows: list[sqlite3.Row],
    broker_rows: list[tuple],
) -> dict[str, float | int]:
    total_qty = sum(int(row["quantity"] or 0) for row in raw_rows)
    n_trades = len(raw_rows)
    by_broker = {int(row[0]): {"buy_qty": int(row[1] or 0), "sell_qty": int(row[2] or 0)} for row in broker_rows}
    buy_values = [row["buy_qty"] for row in by_broker.values()]
    sell_values = [row["sell_qty"] for row in by_broker.values()]
    hhi_buy, hhi_buy_norm, n_buy = _hhi_normalized(buy_values)
    hhi_sell, hhi_sell_norm, n_sell = _hhi_normalized(sell_values)

    if total_qty <= 0:
        return {
            "hhi_buy": hhi_buy,
            "hhi_sell": hhi_sell,
            "hhi_buy_norm": hhi_buy_norm,
            "hhi_sell_norm": hhi_sell_norm,
            "circular_score": 0.0,
            "top_pair_pct": 0.0,
            "self_trade_pct": 0.0,
            "smart_money_score": 0.0,
            "pump_score": 0.0,
            "total_volume": 0,
            "n_trades": n_trades,
            "n_brokers_buy": n_buy,
            "n_brokers_sell": n_sell,
        }

    self_qty = sum(int(row["quantity"] or 0) for row in raw_rows if int(row["buyer_broker"]) == int(row["seller_broker"]))
    self_trade_pct = float(self_qty / total_qty)
    pair_qty: dict[tuple[int, int], int] = defaultdict(int)
    for row in raw_rows:
        pair_qty[(int(row["buyer_broker"]), int(row["seller_broker"]))] += int(row["quantity"] or 0)
    top_pair_pct = float(max(pair_qty.values()) / total_qty) if pair_qty else 0.0

    total_buy = sum(buy_values)
    total_sell = sum(sell_values)
    sig_buyers = {code for code, vals in by_broker.items() if total_buy > 0 and vals["buy_qty"] >= MIN_CIRC_SHARE * total_buy}
    sig_sellers = {code for code, vals in by_broker.items() if total_sell > 0 and vals["sell_qty"] >= MIN_CIRC_SHARE * total_sell}
    circ_qty = 0
    for code in sig_buyers & sig_sellers:
        buy_qty = float(by_broker[code]["buy_qty"])
        sell_qty = float(by_broker[code]["sell_qty"])
        if abs(buy_qty - sell_qty) / (buy_qty + sell_qty + 1e-9) < NET_RATIO_MAX:
            circ_qty += int(buy_qty + sell_qty)
    circular_score = float(circ_qty / (total_buy + total_sell + 1e-9))

    if n_trades < MIN_SIGNAL_TRADES or total_qty < MIN_SIGNAL_VOLUME:
        pump_score = 0.0
        smart_money_score = 0.0
    else:
        if self_trade_pct >= 0.05:
            pump_score = self_trade_pct
        elif top_pair_pct > 0.40 and circular_score > 0.20:
            pump_score = top_pair_pct * 0.5
        else:
            pump_score = 0.0
        smart_money_score = 0.0
        if hhi_buy_norm > 0.20 and hhi_sell_norm < 0.40 and circular_score < 0.30 and self_trade_pct < 0.10:
            buy_component = min((hhi_buy_norm - 0.20) / 0.60, 1.0)
            sell_component = max(1.0 - hhi_sell_norm / 0.40, 0.0)
            circ_penalty = max(1.0 - circular_score / 0.30, 0.0)
            smart_money_score = float(buy_component * sell_component * circ_penalty)

    return {
        "hhi_buy": round(hhi_buy, 6),
        "hhi_sell": round(hhi_sell, 6),
        "hhi_buy_norm": round(hhi_buy_norm, 6),
        "hhi_sell_norm": round(hhi_sell_norm, 6),
        "circular_score": round(circular_score, 6),
        "top_pair_pct": round(top_pair_pct, 6),
        "self_trade_pct": round(self_trade_pct, 6),
        "smart_money_score": round(min(smart_money_score, 1.0), 6),
        "pump_score": round(min(pump_score, 1.0), 6),
        "total_volume": int(total_qty),
        "n_trades": int(n_trades),
        "n_brokers_buy": int(n_buy),
        "n_brokers_sell": int(n_sell),
    }


def _recompute_aggregates(conn: sqlite3.Connection, symbol: str, as_of_date: str) -> None:
    conn.row_factory = sqlite3.Row
    raw_rows = conn.execute(
        """
        SELECT buyer_broker, seller_broker, quantity, amount
        FROM floorsheet_trades
        WHERE symbol = ? AND as_of_date = ?
        """,
        (symbol.upper(), as_of_date),
    ).fetchall()
    if not raw_rows:
        return

    broker: dict[int, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    total_qty = 0
    total_amount = 0.0
    for row in raw_rows:
        buyer = int(row["buyer_broker"])
        seller = int(row["seller_broker"])
        qty = int(row["quantity"] or 0)
        amount = float(row["amount"] or 0.0)
        total_qty += qty
        total_amount += amount
        broker[buyer]["buy_qty"] += qty
        broker[buyer]["buy_amount"] += amount
        broker[buyer]["buy_trades"] += 1
        broker[seller]["sell_qty"] += qty
        broker[seller]["sell_amount"] += amount
        broker[seller]["sell_trades"] += 1

    flow_rows = []
    net_abs = []
    for code, values in broker.items():
        buy_qty = int(values.get("buy_qty", 0))
        sell_qty = int(values.get("sell_qty", 0))
        buy_amount = float(values.get("buy_amount", 0.0))
        sell_amount = float(values.get("sell_amount", 0.0))
        buy_trades = int(values.get("buy_trades", 0))
        sell_trades = int(values.get("sell_trades", 0))
        net_qty = buy_qty - sell_qty
        net_amount = buy_amount - sell_amount
        flow_rows.append(
            (
                symbol.upper(),
                as_of_date,
                code,
                buy_qty,
                sell_qty,
                net_qty,
                buy_amount,
                sell_amount,
                net_amount,
                buy_trades,
                sell_trades,
                buy_trades + sell_trades,
            )
        )
        net_abs.append(abs(net_qty))

    conn.execute("DELETE FROM broker_summary WHERE symbol = ? AND as_of_date = ?", (symbol.upper(), as_of_date))
    conn.executemany(
        """
        INSERT OR REPLACE INTO broker_summary
        (symbol, as_of_date, broker_code, buy_qty, sell_qty, net_qty, buy_amount, sell_amount, net_amount, buy_trades, sell_trades, total_trades)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        flow_rows,
    )

    denom = float(total_qty or 1)
    sorted_net = sorted(net_abs, reverse=True)
    top1 = sorted_net[0] / denom if sorted_net else 0.0
    top5 = sum(sorted_net[:5]) / denom if sorted_net else 0.0
    shares = [value / denom for value in sorted_net]
    hhi_net = sum(share * share for share in shares)
    flags = "strong_net_inflow" if top5 >= 0.50 else ("moderate_net_inflow" if top5 >= 0.25 else "")
    conn.execute(
        """
        INSERT OR REPLACE INTO broker_signal_scores
        (symbol, as_of_date, total_trades, total_qty, total_amount, top1_net_share, top5_net_share, hhi_net, accumulation_score, flags)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (symbol.upper(), as_of_date, len(raw_rows), total_qty, total_amount, top1, top5, hhi_net, max(0.0, min(100.0, top5 * 100.0)), flags),
    )

    signal_metrics = _compute_signal_metrics(raw_rows, [(row[2], row[3], row[4]) for row in flow_rows])
    conn.execute(
        """
        INSERT OR REPLACE INTO broker_signals_v2
        (symbol, as_of_date, hhi_buy, hhi_sell, hhi_buy_norm, hhi_sell_norm,
         circular_score, top_pair_pct, self_trade_pct, smart_money_score, pump_score,
         total_volume, n_trades, n_brokers_buy, n_brokers_sell)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            symbol.upper(),
            as_of_date,
            signal_metrics["hhi_buy"],
            signal_metrics["hhi_sell"],
            signal_metrics["hhi_buy_norm"],
            signal_metrics["hhi_sell_norm"],
            signal_metrics["circular_score"],
            signal_metrics["top_pair_pct"],
            signal_metrics["self_trade_pct"],
            signal_metrics["smart_money_score"],
            signal_metrics["pump_score"],
            signal_metrics["total_volume"],
            signal_metrics["n_trades"],
            signal_metrics["n_brokers_buy"],
            signal_metrics["n_brokers_sell"],
        ),
    )


def _date_range(start_date: date | str, end_date: date | str) -> list[date]:
    start = date.fromisoformat(str(start_date)) if not isinstance(start_date, date) else start_date
    end = date.fromisoformat(str(end_date)) if not isinstance(end_date, date) else end_date
    out: list[date] = []
    current = start
    while current <= end:
        out.append(current)
        current = date.fromordinal(current.toordinal() + 1)
    return out


def _trading_dates(start_date: date | str, end_date: date | str) -> list[date]:
    try:
        from backend.quant_pro.nepse_calendar import is_trading_day

        return [day for day in _date_range(start_date, end_date) if is_trading_day(day)]
    except Exception:
        return _date_range(start_date, end_date)


def scrape_all(
    *,
    symbols: list[str],
    start_date: date | str,
    end_date: date | str,
    max_workers: int = 4,
    rps: float = 1.0,
    skip_existing: bool = True,
    dry_run: bool = False,
    mark_complete: bool = False,
) -> dict[str, int]:
    """Scrape floorsheet data and recompute broker-flow tables.

    When ``mark_complete`` is true, the scraper fetches the whole market by
    date instead of looping per symbol. This is usually faster for daily runs.
    """
    ensure_floorsheet_tables()
    dates = _trading_dates(start_date, end_date)
    stats = {"total_pairs": 0, "skipped": 0, "fetched": 0, "inserted": 0, "errors": 0}
    delay = 1.0 / max(float(rps or 1.0), 0.1)

    if mark_complete:
        stats["total_pairs"] = len(dates)
        conn = get_db_connection()
        try:
            done_dates = {
                row[0]
                for row in conn.execute(
                    "SELECT as_of_date FROM floorsheet_scrape_log WHERE as_of_date BETWEEN ? AND ?",
                    (_date_key(dates[0]) if dates else "", _date_key(dates[-1]) if dates else ""),
                ).fetchall()
            }
        finally:
            conn.close()

        for day in dates:
            day_key = day.isoformat()
            if skip_existing and day_key in done_dates:
                stats["skipped"] += 1
                continue
            try:
                time.sleep(delay)
                trades, total_pages = _fetch_date_all(day, max_workers=max_workers)
            except Exception as exc:
                stats["errors"] += 1
                LOG.warning("floorsheet date fetch failed %s: %s", day_key, exc)
                continue
            stats["fetched"] += 1
            if dry_run:
                continue
            conn = get_db_connection()
            try:
                inserted = _insert_trades(conn, trades)
                symbols_today = [
                    row[0]
                    for row in conn.execute(
                        "SELECT DISTINCT symbol FROM floorsheet_trades WHERE as_of_date = ?",
                        (day_key,),
                    ).fetchall()
                ]
                for sym in symbols_today:
                    _recompute_aggregates(conn, sym, day_key)
                conn.execute(
                    """
                    INSERT OR REPLACE INTO floorsheet_scrape_log
                    (as_of_date, total_rows, total_pages, scraped_at_utc)
                    VALUES (?, ?, ?, ?)
                    """,
                    (day_key, len(trades), int(total_pages or 0), datetime.utcnow().replace(microsecond=0).isoformat()),
                )
                conn.commit()
                stats["inserted"] += inserted
            finally:
                conn.close()
        return stats

    session = requests.Session()
    symbol_ids = _symbol_ids(session)
    pairs = [(sym.upper(), day) for day in dates for sym in symbols if sym.upper() in symbol_ids]
    stats["total_pairs"] = len(pairs)

    conn = get_db_connection()
    try:
        todo: list[tuple[str, date]] = []
        for sym, day in pairs:
            if skip_existing and _existing(conn, sym, day.isoformat()):
                stats["skipped"] += 1
            else:
                todo.append((sym, day))
    finally:
        conn.close()

    def worker(pair: tuple[str, date]) -> tuple[str, str, list[FloorTrade], str | None]:
        sym, day = pair
        try:
            time.sleep(delay)
            return sym, day.isoformat(), _fetch_symbol_date(sym, symbol_ids[sym], day), None
        except Exception as exc:
            return sym, day.isoformat(), [], str(exc)

    with ThreadPoolExecutor(max_workers=max(1, int(max_workers or 1))) as pool:
        futures = [pool.submit(worker, pair) for pair in todo]
        for fut in as_completed(futures):
            sym, day_key, trades, error = fut.result()
            if error:
                stats["errors"] += 1
                LOG.warning("floorsheet fetch failed %s %s: %s", sym, day_key, error)
                continue
            stats["fetched"] += 1
            if dry_run:
                continue
            conn = get_db_connection()
            try:
                inserted = _insert_trades(conn, trades)
                _recompute_aggregates(conn, sym, day_key)
                conn.commit()
                stats["inserted"] += inserted
            finally:
                conn.close()

    return stats
