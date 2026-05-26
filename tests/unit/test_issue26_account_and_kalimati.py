import json

import pandas as pd

from backend.market.kalimati_market import _rows_from_api_payload
from backend.trading.paper_execution import PaperExecutionService
from backend.trading.tui_trading_engine import TUITradingEngine
from apps.tui import dashboard_tui


def test_paper_execution_uses_account_seed_nav_for_drawdown_baseline(tmp_path, monkeypatch):
    state_path = tmp_path / "paper_state.json"
    state_path.write_text(json.dumps({"cash": 500_000.0, "daily_start_nav": 500_000.0}))

    service = PaperExecutionService(
        "account_2",
        account_dir=tmp_path,
        initial_capital=1_000_000.0,
        max_positions=5,
    )

    result = service.submit_order(
        "account_2",
        "BUY",
        "NABIL",
        10,
        100.0,
        "test",
        "manual",
    )

    assert service.initial_capital == 500_000.0
    assert result.ok
    assert result.risk_result["reason"] == "accepted"


def test_tui_engine_does_not_halt_seeded_smaller_account_against_global_default(tmp_path):
    state_path = tmp_path / "paper_state.json"
    state_path.write_text(json.dumps({"cash": 500_000.0, "daily_start_nav": 500_000.0}))
    pd.DataFrame(columns=["Date", "Cash", "Positions_Value", "NAV", "Num_Positions"]).to_csv(
        tmp_path / "paper_nav_log.csv",
        index=False,
    )

    engine = TUITradingEngine(
        capital=1_000_000.0,
        portfolio_file=tmp_path / "paper_portfolio.csv",
        trade_log_file=tmp_path / "paper_trade_log.csv",
        nav_log_file=tmp_path / "paper_nav_log.csv",
        state_file=state_path,
        account_id="account_2",
    )
    engine._check_kill_switch()

    assert engine.capital == 500_000.0
    assert not engine._halted


def test_kalimati_daily_prices_api_payload_is_parsed():
    rows = _rows_from_api_payload(
        {
            "status": 200,
            "date": "2026-05-26",
            "prices": [
                {
                    "commodityname": "Tomato Big(Nepali)",
                    "commodityunit": "KG",
                    "minprice": "60.00",
                    "maxprice": "70.00",
                    "avgprice": "65.00",
                }
            ],
        }
    )

    assert rows == [
        {
            "name_nepali": "Tomato Big(Nepali)",
            "name_english": "Tomato Big(Nepali)",
            "unit_nepali": "KG",
            "unit_english": "KG",
            "min": 60.0,
            "max": 70.0,
            "avg": 65.0,
            "date": "2026-05-26",
        }
    ]


def test_account_seed_state_records_seed_nav_as_initial_capital():
    state, _ = dashboard_tui._build_account_seed_state(
        pd.DataFrame(columns=dashboard_tui.PORTFOLIO_COLS),
        500_000.0,
    )

    assert state["cash"] == 500_000.0
    assert state["daily_start_nav"] == 500_000.0
    assert state["initial_capital"] == 500_000.0


def test_profile_snapshot_uses_current_marked_account_nav(tmp_path):
    account_dir = tmp_path
    pd.DataFrame(
        [
            {
                "Entry_Date": "2026-05-20",
                "Symbol": "NABIL",
                "Quantity": 10,
                "Buy_Price": 20.0,
                "Buy_Amount": 200.0,
                "Buy_Fees": 0.0,
                "Total_Cost_Basis": 200.0,
                "Signal_Type": "manual",
                "High_Watermark": 20.0,
                "Last_LTP": 20.0,
                "Last_LTP_Source": "test",
                "Last_LTP_Time_UTC": "",
            }
        ],
        columns=dashboard_tui.PORTFOLIO_COLS,
    ).to_csv(account_dir / "paper_portfolio.csv", index=False)
    pd.DataFrame(columns=dashboard_tui.TRADE_LOG_COLS).to_csv(account_dir / "paper_trade_log.csv", index=False)
    pd.DataFrame(
        [{"Date": "2026-05-20", "Cash": 100.0, "Positions_Value": 200.0, "NAV": 999.0, "Num_Positions": 1}],
        columns=dashboard_tui.NAV_LOG_COLS,
    ).to_csv(account_dir / "paper_nav_log.csv", index=False)
    (account_dir / "paper_state.json").write_text(
        json.dumps({"cash": 100.0, "daily_start_nav": 300.0, "initial_capital": 300.0})
    )

    class FakeMD:
        quotes = pd.DataFrame()

        def ltps(self):
            return {"NABIL": 25.0}

    app = dashboard_tui.NepseDashboard.__new__(dashboard_tui.NepseDashboard)
    app.md = FakeMD()
    snapshot = dashboard_tui.NepseDashboard._profile_runtime_snapshot(app, account_dir=account_dir)

    assert snapshot["cash"] == 100.0
    assert snapshot["nav"] == 350.0
