"""
main.py - Startpunkt. Bindet bot_core.py, strategies.py und copytrade.py
zusammen, registriert alle Web-Routen und startet alle Hintergrund-Loops.

RENDER START COMMAND: python -u main.py
"""

import asyncio
from aiohttp import web

from bot_core import (
    debug_log, PORT, SYMBOLS, BOTS, load_bot_configs, load_bot_state, state_persist_loop,
    load_global_settings, handle_global_settings_get, handle_global_settings_update,
    handle_index, handle_symbols, handle_overview, handle_status,
    handle_config_update, handle_control, handle_close_position, handle_reset,
    handle_manual_trade, handle_backtest, handle_ab_sweep, handle_ab_signal_sweep,
    basic_auth_middleware, DASHBOARD_USERNAME, DASHBOARD_PASSWORD, DASHBOARD_PASSWORD_GENERATED,
)
from strategies import (
    trading_loop, binance_1s_poll_loop, ab_poll_loop, rsi_poll_loop, mvwap_poll_loop,
)
from copytrade import (
    load_ct_watched, ct_leaderboard_refresh_loop, ct_watch_loop,
    handle_ct_index, handle_ct_status, handle_ct_watch, handle_ct_copy_toggle,
    handle_ct_monitor_toggle, handle_ct_skip_nachkauf_toggle, handle_ct_dry_run_toggle,
    handle_ct_copy_all_coins_toggle,
    handle_ct_set_coin_setting, handle_ct_remove_coin_setting, handle_ct_set_trader_defaults,
)
from binance_ws import binance_ws_cache_loop
from grid_scalp import grid_scalp_poll_loop


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
    app.router.add_post("/api/ab_sweep", handle_ab_sweep)
    app.router.add_post("/api/ab_signal_sweep", handle_ab_signal_sweep)
    app.router.add_get("/api/global_settings", handle_global_settings_get)
    app.router.add_post("/api/global_settings", handle_global_settings_update)
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
    await load_global_settings()
    await load_ct_watched()
    await start_web_server()
    # return_exceptions=True ist HIER ENTSCHEIDEND: ohne das bringt eine einzige unbehandelte
    # Exception in IRGENDEINER der ~50+ parallelen Tasks (z.B. ein Bug in genau einem Coin/einer
    # Strategie) asyncio.gather() dazu, ALLE anderen Tasks zu canceln und main() mit dieser einen
    # Exception abstuerzen zu lassen - das toetet den KOMPLETTEN Bot fuer JEDEN Coin wegen eines
    # einzelnen, moeglicherweise winzigen Fehlers, und wirkt fuer den Nutzer wie "haengt/startet
    # nicht" (Render startet den Container neu, trifft ggf. sofort wieder denselben Bug). Mit
    # return_exceptions=True laeuft jede Task unabhaengig weiter, bis SIE SELBST endet - ein Crash
    # in einer Task beendet nur diese eine, alle anderen (andere Coins, andere Strategien) laufen
    # normal weiter. Die meisten Loops fangen Fehler ohnehin schon selbst ab (try/except in ihrer
    # eigenen while-True-Schleife) - das hier ist nur das letzte Sicherheitsnetz fuer alles, was
    # AUSSERHALB eines solchen Loops passiert (z.B. beim Task-Start selbst).
    results = await asyncio.gather(
        trading_loop(),
        *[binance_1s_poll_loop(s) for s in SYMBOLS],
        *[ab_poll_loop(s) for s in SYMBOLS],
        *[rsi_poll_loop(s) for s in SYMBOLS],
        *[mvwap_poll_loop(s) for s in SYMBOLS],
        *[grid_scalp_poll_loop(s) for s in SYMBOLS],
        ct_leaderboard_refresh_loop(),
        ct_watch_loop(),
        state_persist_loop(),
        binance_ws_cache_loop(),
        return_exceptions=True,
    )
    # Alle obigen Loops sind 'while True' - normalerweise kehrt hier nichts jemals zurueck. Landet
    # trotzdem eine Exception in 'results', ist eine Task fuer immer tot (kein Auto-Neustart) -
    # das muss lautstark geloggt werden statt leise zu verschwinden.
    for result in results:
        if isinstance(result, BaseException):
            debug_log("🔥 Eine Hintergrund-Task ist dauerhaft abgestürzt (kein automatischer Neustart!)",
                      {"error": str(result), "type": type(result).__name__})


if __name__ == "__main__":
    asyncio.run(main())
