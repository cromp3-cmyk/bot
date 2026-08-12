"""
main.py - Startpunkt. Bindet bot_core.py, strategies.py und copytrade.py
zusammen, registriert alle Web-Routen und startet alle Hintergrund-Loops.

RENDER START COMMAND: python -u main.py
"""

import asyncio
from aiohttp import web

from bot_core import (
    debug_log, PORT, SYMBOLS, BOTS, load_bot_configs, load_bot_state, state_persist_loop,
    handle_index, handle_symbols, handle_overview, handle_status,
    handle_config_update, handle_control, handle_close_position, handle_reset,
    handle_manual_trade, handle_backtest, handle_ht_sweep, handle_da_sweep,
    basic_auth_middleware, DASHBOARD_USERNAME, DASHBOARD_PASSWORD, DASHBOARD_PASSWORD_GENERATED,
)
from strategies import (
    trading_loop, fib_reversal_poll_loop, binance_1s_poll_loop,
    ht_poll_loop, oms_rsi_poll_loop, scalp_board_poll_loop, quad_stoch_poll_loop,
    da_poll_loop,
)
from copytrade import (
    load_ct_watched, ct_leaderboard_refresh_loop, ct_watch_loop,
    handle_ct_index, handle_ct_status, handle_ct_watch, handle_ct_copy_toggle,
    handle_ct_monitor_toggle, handle_ct_skip_nachkauf_toggle, handle_ct_dry_run_toggle,
    handle_ct_copy_all_coins_toggle,
    handle_ct_set_coin_setting, handle_ct_remove_coin_setting, handle_ct_set_trader_defaults,
)


async def start_web_server():
    app = web.Application(middlewares=[basic_auth_middleware])
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/symbols", handle_symbols)
    app.router.add_get("/api/overview", handle_overview)
    app.router.add_get("/api/status", handle_status)
    app.router.add_post("/api/config", handle_config_update)
    app.router.add_post("/api/control", handle_control)
    app.router.add_post("/api/close", handle_close_position)
    app.router.add_post("/api/manual_trade", handle_manual_trade)
    app.router.add_post("/api/backtest", handle_backtest)
    app.router.add_post("/api/ht_sweep", handle_ht_sweep)
    app.router.add_post("/api/da_sweep", handle_da_sweep)
    app.router.add_post("/api/reset", handle_reset)
    app.router.add_get("/copytrading", handle_ct_index)
    app.router.add_get("/api/ct/status", handle_ct_status)
    app.router.add_post("/api/ct/watch", handle_ct_watch)
    app.router.add_post("/api/ct/copy", handle_ct_copy_toggle)
    app.router.add_post("/api/ct/monitor", handle_ct_monitor_toggle)
    app.router.add_post("/api/ct/skip_nachkauf", handle_ct_skip_nachkauf_toggle)
    app.router.add_post("/api/ct/dry_run", handle_ct_dry_run_toggle)
    app.router.add_post("/api/ct/copy_all_coins", handle_ct_copy_all_coins_toggle)
    app.router.add_post("/api/ct/coin_setting", handle_ct_set_coin_setting)
    app.router.add_post("/api/ct/remove_coin_setting", handle_ct_remove_coin_setting)
    app.router.add_post("/api/ct/trader_defaults", handle_ct_set_trader_defaults)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    debug_log(f"🌐 Dashboard läuft auf Port {PORT}")


async def main():
    print("=" * 60)
    print(f"🚀 Multi-Coin Grid-Bot - Dashboard auf Port {PORT}")
    print(f"   Coins: {', '.join(SYMBOLS)}")
    for s in SYMBOLS:
        cfg = BOTS[s]["config"]
        print(f"   [{s}] DRY_RUN={cfg['dry_run']} Margin={cfg['margin']} Hebel={cfg['leverage']}x Grid={cfg['grid_step_pct']}% TP={cfg['tp_step_pct']}%")
    print("=" * 60)
    if DASHBOARD_PASSWORD_GENERATED:
        print("🔐 KEIN DASHBOARD_PASSWORD gesetzt - automatisch generiertes Passwort (aendert sich bei jedem Neustart!):")
        print(f"   Benutzername: {DASHBOARD_USERNAME}")
        print(f"   Passwort:     {DASHBOARD_PASSWORD}")
        print("   -> Fuer dauerhaften Zugriff DASHBOARD_PASSWORD in Render unter Environment setzen.")
    else:
        print(f"🔐 Dashboard passwortgeschützt (Benutzername: {DASHBOARD_USERNAME})")
    print("=" * 60)

    await load_bot_configs()
    await load_bot_state()
    await load_ct_watched()
    await start_web_server()
    await asyncio.gather(
        trading_loop(),
        *[fib_reversal_poll_loop(s) for s in SYMBOLS],
        *[binance_1s_poll_loop(s) for s in SYMBOLS],
        *[ht_poll_loop(s) for s in SYMBOLS],
        *[oms_rsi_poll_loop(s) for s in SYMBOLS],
        *[scalp_board_poll_loop(s) for s in SYMBOLS],
        *[quad_stoch_poll_loop(s) for s in SYMBOLS],
        *[da_poll_loop(s) for s in SYMBOLS],
        ct_leaderboard_refresh_loop(),
        ct_watch_loop(),
        state_persist_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
