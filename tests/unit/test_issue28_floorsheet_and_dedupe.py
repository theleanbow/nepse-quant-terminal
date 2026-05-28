import sqlite3

from backend.quant_pro import database
from backend.quant_pro.floorsheet_scraper import (
    FloorTrade,
    _insert_trades,
    _recompute_aggregates,
    ensure_floorsheet_tables,
)


def test_init_db_dedupes_legacy_stock_prices_and_enforces_unique_upsert(tmp_path, monkeypatch):
    db_path = tmp_path / "market.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE stock_prices (
            symbol TEXT,
            date DATE,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL
        )
        """
    )
    conn.executemany(
        "INSERT INTO stock_prices VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("NABIL", "2026-05-27", 100.0, 101.0, 99.0, 100.0, 1000.0),
            ("NABIL", "2026-05-27", 100.0, 102.0, 99.0, 101.0, 1100.0),
            ("NICA", "2026-05-27", 200.0, 201.0, 199.0, 200.0, 1000.0),
        ],
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("NEPSE_DB_FILE", str(db_path))
    monkeypatch.setattr(database, "_wal_initialized", False)

    database.init_db()

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT close, volume FROM stock_prices WHERE symbol = 'NABIL' AND date = '2026-05-27'"
    ).fetchall()
    assert rows == [(101.0, 1100.0)]

    conn.execute(
        """
        INSERT OR REPLACE INTO stock_prices (symbol, date, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("NABIL", "2026-05-27", 100.0, 103.0, 99.0, 102.0, 1200.0),
    )
    conn.commit()
    rows = conn.execute(
        "SELECT close, volume FROM stock_prices WHERE symbol = 'NABIL' AND date = '2026-05-27'"
    ).fetchall()
    conn.close()

    assert rows == [(102.0, 1200.0)]


def test_floorsheet_trade_insert_is_idempotent_and_recomputes_broker_signals(tmp_path, monkeypatch):
    db_path = tmp_path / "market.db"
    monkeypatch.setenv("NEPSE_DB_FILE", str(db_path))
    monkeypatch.setattr(database, "_wal_initialized", False)
    database.init_db()

    trades = [
        FloorTrade(
            transact_no=f"T{idx}",
            symbol="NABIL",
            as_of_date="2026-05-27",
            buyer_broker=1,
            seller_broker=2 + (idx % 3),
            quantity=500,
            rate=100.0,
            amount=50_000.0,
            source_url="test",
            scraped_at_utc="2026-05-27T00:00:00",
        )
        for idx in range(20)
    ]

    conn = sqlite3.connect(db_path)
    ensure_floorsheet_tables(conn)
    assert _insert_trades(conn, trades) == 20
    assert _insert_trades(conn, trades) == 0
    _recompute_aggregates(conn, "NABIL", "2026-05-27")
    conn.commit()

    raw_count = conn.execute("SELECT COUNT(*) FROM floorsheet_trades").fetchone()[0]
    broker_row = tuple(conn.execute(
        "SELECT buy_qty, sell_qty, net_qty FROM broker_summary WHERE symbol = 'NABIL' AND broker_code = 1"
    ).fetchone())
    signal_row = tuple(conn.execute(
        "SELECT total_volume, n_trades, n_brokers_buy FROM broker_signals_v2 WHERE symbol = 'NABIL'"
    ).fetchone())
    conn.close()

    assert raw_count == 20
    assert broker_row == (10000, 0, 10000)
    assert signal_row == (10000, 20, 1)
