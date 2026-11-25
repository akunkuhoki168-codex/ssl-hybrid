#!/usr/bin/env python3
"""
Live pool engine (epoch 1, from 2026-10-27 ~19:4x WIB): SSL Hybrid Exit Arrows, long-only, shared cash pool, low-cap universe.

Why a new engine: intrabar fills depend on live prices seen by the 5 s poller, so they cannot be re-derived from klines.
The run is therefore event-sourced: every paper fill is appended to ledger.jsonl and the state (cash, positions, trades)
is always the fold of epoch0.json (the frozen earlier run, balance carried over) + ledger.jsonl. Restarts replay the
ledger, then catch up bar by bar (bar-based rules) for the bars that closed while the bot was down.

config.json:
  signal_timing  "intrabar" (default here) | "close"
     intrabar: every ~5 s the indicator is recomputed on the FORMING candle with the live price as its close (live high/low =
               max/min of the forming kline and the polled prices). Up arrow = live close crosses above the Exit SSL (HMA15)
               versus the previous closed bar  -> BUY at once at the live price (reason ARROW_INTRABAR) if a slot is free.
               Down arrow intrabar on a held coin -> SELL at once (ARROW_INTRABAR). Max one entry and one exit per coin per
               candle; a coin exited in a candle is not re-entered in that candle. An arrow that was never seen intrabar but is
               on the closed bar (crossing in the last seconds) is acted on at the next bar open (ARROW_CLOSE). The TradingView
               arrow may disappear before close; such an entry is kept (exits only on a down arrow, TP or SL).
     close:    arrows on the closed bar -> fill at the next bar open (reason ARROW).
  coins            the universe (Binance spot USDT pairs)
  exclude_top_mcap 50: coins ranked 1..N by CoinGecko /coins/markets are dropped from NEW entries (checked at start + every 24 h;
                   a held coin still exits on its rules)
  max_positions  open-long slots. Entry size = equity / max_positions (equity = cash + held coins at the live price),
                 capped at the pool cash (from 2026-10-27 20:3x WIB; before: cash / free slots). When max_positions changes, the
                 bot at start trims held positions above equity / max_positions with a paper partial sell (ledger side RESIZE,
                 live price, 0.1% fee; entry price kept, cost basis + entry fee reduced pro rata, TP/SL recomputed for the smaller
                 size) and logs a live_child segment (from_max_positions -> max_positions) + a mode_switch event.
  tp_usd, sl_usd as before (TP/SL net of the entry fee and the 0.1% exit fee; live 5 s checks fill at the level;
                   catch-up on bar high/low, SL first).
Data: one /api/v3/ticker/price call for all symbols every 5 s; klines at candle boundaries + one coin's forming kline every 5 s
(round robin), i.e. far below Binance's 6000 weight/min.
"""
import os, json, time, math, threading, traceback, shutil
import numpy as np, pandas as pd, requests
import ssl_core as core
import pausectl
import paper_bot as pb
from paper_bot import (api, klines, closed_df, ms_wib, now_ms, write_json, write_jsonl, append_jsonl, readout, chart_payload,
                       portfolio, tpsl_levels, FEE, WARMUP, TFS, MODES, log, wib_to_ms, WIB)
from datetime import datetime
from ctl_valid import valid_capital

TIMING_TEXT = {"intrabar": "intrabar (live)", "close": "on candle close"}

def jsonl_read(p):
    out = []
    try:
        for l in open(p):
            l = l.strip()
            if l:
                try: out.append(json.loads(l))
                except Exception: pass
    except FileNotFoundError: pass
    return out

def live_exit(df, o, h, l, c, tail=600, rsi_len=14):
    """Exit SSL + RSI on the forming bar (closed bars + one forming row). Returns arrow (+1/-1/0), exit line, strength (c-ex)/ATR, rsi."""
    d = df.iloc[-tail:] if len(df) > tail else df
    H = np.r_[d.h.values, h]; L = np.r_[d.l.values, l]; C = np.r_[d.c.values, c]
    st, ex = core.ssl_state(C, core.hma(H, 15), core.hma(L, 15))
    up = C[-1] > ex[-1] and C[-2] <= ex[-2]; dn = C[-1] < ex[-1] and C[-2] >= ex[-2]
    atr = core.wma_tr_atr(H, L, C, 14)[-1]
    s = (C[-1] - ex[-1]) / atr if atr > 0 and math.isfinite(atr) and math.isfinite(ex[-1]) else -1e9
    r = core.rsi(C, rsi_len)[-1]
    rsi_val = float(r) if math.isfinite(r) else None
    return (1 if up else (-1 if dn else 0)), float(ex[-1]), float(s), rsi_val

def top_mcap_symbols(n):
    r = requests.get("https://api.coingecko.com/api/v3/coins/markets", params={"vs_currency": "usd", "order": "market_cap_desc", "per_page": max(n, 1), "page": 1},
                     headers={"User-Agent": "ssl-hybrid-paper-bot/1.0"}, timeout=20)
    r.raise_for_status()
    return {c["symbol"].upper(): {"rank": c.get("market_cap_rank"), "id": c["id"], "mcap": c.get("market_cap")} for c in r.json()[:n]}

class LiveBot(pb.Bot):
    def __init__(self, d, start_wib, cap, coins, mode, cfg):
        super().__init__(d, start_wib, cap, coins, mode, cfg)
        self.cfg = cfg
        self.timing = cfg.get("signal_timing", "intrabar") if cfg.get("signal_timing") in ("intrabar", "close") else "intrabar"
        self.universe = [c.upper() for c in (cfg.get("coins") or [])]
        self.excl_n = int(cfg.get("exclude_top_mcap") or 0)
        self.excluded = {}
        self.epoch0 = None; self.ep = None
        self.forming = {}      # coin -> {"bo","o","h","l"}
        self.seen = set(self.seen)
        self.rr = 0; self.D = {}; self.watch_busy = False
        self.cover_from = now_ms()
        try: ev = json.load(open(self.p("run/intrabar_eval.json")))
        except Exception: ev = {}
        self.flags = ev.get("flags", {})         # "COIN|bar_t" -> "up"/"down" seen intrabar
        self.last_ct = ev.get("last_ct")         # close time of the last processed bar (active tf)
        self.last_eval = ev.get("last_eval_ms")
        # entry filter (+2%/24h for longs, −2%/24h for shorts), RSI entry filter, and sides (long | ls).
        self.entry_filter_2pct = bool(cfg.get("entry_filter_2pct"))
        rf = cfg.get("rsi_filter")
        if isinstance(rf, dict):
            self.rsi_filter = {
                "enabled": bool(rf.get("enabled", True)),
                "length": max(1, int(rf.get("length") or 14)),
                "long_below": float(rf.get("long_below", 35)),
                "short_above": float(rf.get("short_above", 65)),
            }
        elif rf is None:
            self.rsi_filter = {"enabled": False, "length": 14, "long_below": 35.0, "short_above": 65.0}
        else:
            self.rsi_filter = {"enabled": bool(rf), "length": 14, "long_below": 35.0, "short_above": 65.0}
        sides = (cfg.get("sides") or "ls")
        self.sides = "ls" if str(sides).lower() in ("ls", "long_short", "long+short", "both") else "long"
        self.filter_req_handled = 0; self.sides_req_handled = 0
        try:
            ap = json.load(open(self.p("run/settings_applied.json")))
            self.filter_req_handled = int(ap.get("filter_at_ms") or 0)
            self.sides_req_handled = int(ap.get("sides_at_ms") or 0)
            self.reset_req_handled = int(ap.get("reset_at_ms") or 0)
        except Exception: pass
        if not hasattr(self, "reset_req_handled"):
            self.reset_req_handled = 0

    # ------------------------------------------------------------ epoch / ledger
    @property
    def coins_all(self):
        held = sorted(self.P) if hasattr(self, "P") else []
        return self.universe + [c for c in held if c not in self.universe]
    @property
    def active(self): return [c for c in self.universe if c not in self.excluded]
    def tf_now(self): return (self.ep_segs[-1] if self.ep_segs else self.ep)["tf"]
    @property
    def tf(self): return self.tf_now() if getattr(self, "ep", None) else "15m"
    @property
    def rsi_len(self): return int(self.rsi_filter.get("length", 14))
    @property
    def rsi_filter_text(self):
        rf = self.rsi_filter
        if not rf.get("enabled"):
            return f"RSI({rf['length']}) filter: OFF"
        return f"RSI({rf['length']}) filter: Long <{rf['long_below']:g} / Short >{rf['short_above']:g}"
    def ensure_epoch(self):
        """Freeze the earlier run (pool cash, no open positions) into epoch0.json and start the live_pool segment."""
        for g in self.segs:
            if g.get("engine") == "live_pool": self.ep = g
        if self.ep:
            self.epoch0 = json.load(open(self.p("epoch0.json"))); self.ep_segs = [g for g in self.segs if g.get("engine") == "live_pool" or g.get("live_child")]
            return
        if not os.path.exists(self.p("state.json")):   # fresh run (state archived): empty epoch0 holding the start capital, live pool from start_ms
            n = max(self.start_ms, 0); px = self.prices(self.universe)
            ep0 = {"frozen_ms": n, "frozen_wib": ms_wib(n), "cash": self.cap, "balance": self.cap, "bh": self.cap, "coins_old": [], "trades": [], "equity": [],
                   "fills": {}, "fees": 0.0, "segments": [], "switches": [], "skipped": [], "n_skipped": 0, "pool_since_ms": n, "watch": {}, "fresh": True}
            write_json(self.p("epoch0.json"), ep0); self.epoch0 = ep0
            g = {"tf": self.segs[-1]["tf"], "from_tf": self.segs[-1]["tf"], "mode": "arrow_long", "from_mode": "arrow_long", "sizing": "pool", "from_sizing": "pool",
                 "engine": "live_pool", "signal_timing": self.timing, "from_timing": self.timing, "coins": self.universe, "from_coins": self.universe,
                 "exclude_top_mcap": self.excl_n, "max_positions": self.max_pos, "tp_usd": self.tp_usd, "sl_usd": self.sl_usd,
                 "from_ms": n, "from_wib": ms_wib(n), "balance": self.cap, "bh": self.cap, "px": px, "sync": {}, "skipped": [],
                 "why": f"fresh start ${self.cap:.0f} at {self.start_wib} WIB (user reset; earlier run archived to archive/pre_reset_20261110)"}
            self.segs = [dict(self.segs[0], from_ms=n)] + [g]; self.ep = g; self.ep_segs = [g]; self.save_segs()
            self.event("mode_switch", f"FRESH START · ${self.cap:.2f} paper capital · start {self.start_wib} WIB · earlier run archived", key=f"fresh|{n}")
            return
        st = json.load(open(self.p("state.json"))); pool = st.get("pool") or {}
        if pool.get("slots"): raise RuntimeError("cannot start the live pool epoch with open positions in the old engine: " + ",".join(s["coin"] for s in pool["slots"]))
        n = now_ms(); L = json.load(open(self.p("live.json")))
        px = self.prices(self.universe)
        ep0 = {"frozen_ms": n, "frozen_wib": ms_wib(n), "cash": pool["cash"], "balance": pool["cash"], "bh": L.get("bh_balance") or st["bh_close"],
               "coins_old": st["coins_traded"], "trades": jsonl_read(self.p("trades.jsonl")), "equity": jsonl_read(self.p("equity.jsonl")),
               "fills": {c["coin"]: c["fills"] for c in st["coins"]}, "fees": sum(t.get("fee", 0) for t in jsonl_read(self.p("trades.jsonl"))),
               "segments": self.segs, "switches": pool.get("switches", []), "skipped": pool.get("skipped", []), "n_skipped": pool.get("n_skipped", 0),
               "pool_since_ms": pool.get("since_ms"), "watch": st.get("watch", {})}
        write_json(self.p("epoch0.json"), ep0); self.epoch0 = ep0
        g = {"tf": self.segs[-1]["tf"], "from_tf": self.segs[-1]["tf"], "mode": "arrow_long", "from_mode": self.segs[-1].get("mode"), "sizing": "pool", "from_sizing": "pool",
             "engine": "live_pool", "signal_timing": self.timing, "from_timing": "close", "coins": self.universe, "from_coins": st["coins_traded"],
             "exclude_top_mcap": self.excl_n, "max_positions": self.max_pos, "tp_usd": self.tp_usd, "sl_usd": self.sl_usd,
             "from_ms": n, "from_wib": ms_wib(n), "balance": pool["cash"], "bh": ep0["bh"], "px": px, "sync": {}, "skipped": [],
             "why": "switch to intrabar signals + low-cap universe (user 19:32/19:34 WIB)"}
        self.segs.append(g); self.ep = g; self.ep_segs = [g]; self.save_segs()
        self.event("mode_switch", f"MODE arrow_long/close → arrow_long/intrabar · universe {'+'.join(st['coins_traded'])} → {len(self.universe)} low-cap coins ({', '.join(self.universe)}); "
                   f"balance ${pool['cash']:.2f} carried over (all cash, NEAR/XRP closed MANUAL before the switch); old run frozen to epoch0.json", key=f"ep|{n}")
    def fold(self):
        cash = self.epoch0["cash"]; P = {}; trades = []; fills = {}; fees = 0.0
        for f in jsonl_read(self.p("ledger.jsonl")):
            c = f["coin"]; fills.setdefault(c, []).append(f)
            if f["side"] == "BUY":
                fee = f["amt"] * FEE; u = (f["amt"] - fee) / f["px"]; cash -= f["amt"]; fees += fee
                tp, sl = tpsl_levels(u, f["amt"], f.get("tp_usd"), f.get("sl_usd"), "long")
                P[c] = {"units": u, "cost": f["amt"], "entry_t": f["t"], "entry_px": f["px"], "fee": fee, "bar_t": f["bar_t"], "tf": f.get("tf"),
                        "side": "long", "collateral": f["amt"] - fee,
                        "reason": f.get("reason"), "tp_px": tp, "sl_px": sl, "tp_usd": f.get("tp_usd"), "sl_usd": f.get("sl_usd")}
            elif f["side"] == "SHORT":
                fee = f["amt"] * FEE; u = (f["amt"] - fee) / f["px"]; cash -= f["amt"]; fees += fee
                tp, sl = tpsl_levels(u, f["amt"], f.get("tp_usd"), f.get("sl_usd"), "short")
                P[c] = {"units": u, "cost": f["amt"], "entry_t": f["t"], "entry_px": f["px"], "fee": fee, "bar_t": f["bar_t"], "tf": f.get("tf"),
                        "side": "short", "collateral": f["amt"] - fee,
                        "reason": f.get("reason"), "tp_px": tp, "sl_px": sl, "tp_usd": f.get("tp_usd"), "sl_usd": f.get("sl_usd")}
            elif f["side"] == "RESIZE" and c in P:
                p = P[c]; u = min(f["units"], p["units"]); k = u / p["units"]
                if p.get("side") == "short":
                    coll = p.get("collateral") if p.get("collateral") is not None else p["units"] * p["entry_px"]
                    gross = core.cash_short_val(coll * k, u, f["px"]); p["collateral"] = coll * (1 - k)
                else:
                    gross = u * f["px"]
                fee = gross * FEE; pr = gross - fee; cash += pr; fees += fee
                cpart = p["cost"] * k; fpart = p["fee"] * k
                p["units"] -= u; p["cost"] -= cpart; p["fee"] -= fpart
                p["tp_px"], p["sl_px"] = tpsl_levels(p["units"], p["cost"], p.get("tp_usd"), p.get("sl_usd"), p.get("side", "long"))
                p.setdefault("resized", []).append({"t": f["t"], "units": u, "px": f["px"]})
                trades.append({"coin": c, "side": p.get("side", "long"), "entry_t": p["entry_t"], "entry_px": p["entry_px"], "exit_t": f["t"], "exit_px": f["px"], "cost": cpart,
                               "proceeds": pr, "pnl": pr - cpart, "ret": pr / cpart - 1 if cpart else 0.0, "fee": fpart + fee, "entry_fee": fpart, "exit_fee": fee,
                               "bars": round((f["t"] - p["entry_t"]) / pb.BAR, 2), "entry_tf": p["tf"], "exit_tf": f.get("tf"), "entry_mode": "arrow_long", "exit_mode": "arrow_long",
                               "exit_reason": "RESIZE", "entry_reason": p.get("reason"), "sizing": "pool", "units": u, "timing": f.get("timing"), "partial": True})
            elif c in P:
                p = P.pop(c); pos_side = p.get("side", "long")
                if pos_side == "short" or f["side"] == "COVER":
                    coll = p.get("collateral") if p.get("collateral") is not None else p["units"] * p["entry_px"]
                    gross = core.cash_short_val(coll, p["units"], f["px"])
                else:
                    gross = p["units"] * f["px"]
                fee = gross * FEE; pr = gross - fee; cash += pr; fees += fee
                trades.append({"coin": c, "side": pos_side, "entry_t": p["entry_t"], "entry_px": p["entry_px"], "exit_t": f["t"], "exit_px": f["px"], "cost": p["cost"],
                               "proceeds": pr, "pnl": pr - p["cost"], "ret": pr / p["cost"] - 1 if p["cost"] else 0.0, "fee": p["fee"] + fee, "entry_fee": p["fee"], "exit_fee": fee,
                               "bars": round((f["t"] - p["entry_t"]) / pb.BAR, 2), "entry_tf": p["tf"], "exit_tf": f.get("tf"), "entry_mode": "arrow_long", "exit_mode": "arrow_long",
                               "exit_reason": f.get("reason"), "entry_reason": p.get("reason"), "sizing": "pool", "units": p["units"], "timing": f.get("timing"),
                               **({"observed_px": f["observed"]} if f.get("observed") is not None else {})})
        self.cash, self.P, self.trades1, self.fills1, self.fees1 = cash, P, trades, fills, fees
    def bar_of(self, t): B = TFS[self.tf]; return t // B * B
    def did(self, c, side, bo):
        syn = {"BUY": ("BUY",), "SELL": ("SELL",), "SHORT": ("SHORT",), "COVER": ("COVER",),
               "OPEN": ("BUY", "SHORT"), "CLOSE": ("SELL", "COVER")}
        sides = syn.get(side, (side,))
        return any(f["side"] in sides and f.get("bar_t") == bo for f in self.fills1.get(c, []))
    def fill(self, c, side, px, reason, why, t=None, amt=None, observed=None):
        t = t or now_ms(); bo = self.bar_of(t)
        row = {"t": t, "t_wib": ms_wib(t), "coin": c, "side": side, "px": px, "reason": reason, "why": why, "bar_t": bo, "tf": self.tf, "timing": self.timing}
        if side in ("BUY", "SHORT"): row.update(amt=amt, tp_usd=self.tp_usd, sl_usd=self.sl_usd)
        if observed is not None: row["observed"] = observed
        append_jsonl(self.p("ledger.jsonl"), row)
        before = dict(self.P.get(c) or {})
        self.fold()
        if side in ("BUY", "SHORT"):
            p = self.P[c]; lab = "BUY LONG" if side == "BUY" else "SELL SHORT"
            self.event("fill", f"{c}: paper {lab} ${amt:.2f} [{self.tf} · {reason}] at {px:.6g} ({why}) {ms_wib(t)} WIB, fee ${amt * FEE:.4f}; TP {p['tp_px']:.6g} / SL {p['sl_px']:.6g}",
                       key=f"f|{c}|{t}|{side}", coin=c, side=side, px=px, fill_t=t, reason=reason, tf=self.tf, pos_side=p.get("side"))
        else:
            tr = self.trades1[-1]; lab = "COVER SHORT" if side == "COVER" or tr.get("side") == "short" else "SELL LONG"
            self.event("fill", f"{c}: paper {lab} [{self.tf} · {reason}] at {px:.6g} ({why}) {ms_wib(t)} WIB, fee ${tr['exit_fee']:.4f}, P&L ${tr['pnl']:+.2f}",
                       key=f"f|{c}|{t}|{side}", coin=c, side=side, px=px, fill_t=t, reason=reason, tf=self.tf, pnl=tr["pnl"], pos_side=tr.get("side"))
    def entry_amt(self, prices=None):
        """Entry size: equity / max_positions (equity at the live price), capped at the pool cash."""
        pr = dict(self.live or {}); pr.update(prices or {})
        return max(0.0, min(self.cash, self.equity_now(pr) / self.max_pos))
    def check_slots(self):
        """max_positions in config differs from the active segment -> new live_child segment; held positions above
        equity / max_positions are trimmed (paper partial sell RESIZE at the live price, 0.1% fee)."""
        last = self.ep_segs[-1] if self.ep_segs else self.ep
        old = int(last.get("max_positions") or pb.POOL_DEFAULT_MAX)
        if old == self.max_pos: return
        n = now_ms(); px = self.prices(self.coins_all); self.live.update(px)
        eq0 = self.equity_now(px); cash0 = self.cash; target = eq0 / self.max_pos; fees0 = self.fees1
        g = {"tf": self.tf, "from_tf": self.tf, "mode": "arrow_long", "from_mode": "arrow_long", "sizing": "pool", "from_sizing": "pool", "live_child": True,
             "signal_timing": self.timing, "from_ms": n, "from_wib": ms_wib(n), "px": px, "sync": {}, "skipped": [], "max_positions": self.max_pos, "from_max_positions": old,
             "tp_usd": self.tp_usd, "sl_usd": self.sl_usd, "sizing_rule": "equity/max_positions capped at cash", "balance": eq0, "equity_before": eq0, "cash_before": cash0,
             "why": f"max positions {old} → {self.max_pos} (user 20:32 WIB: expand to {self.max_pos} slots)"}
        self.segs.append(g); self.ep_segs.append(g); self.save_segs()
        for c in sorted(self.P):
            p = self.P[c]; v = self.pos_value(p, px[c])
            if v - target <= 0.01 or v <= 0: continue
            u = p["units"] * (v - target) / v; t = now_ms()
            append_jsonl(self.p("ledger.jsonl"), {"t": t, "t_wib": ms_wib(t), "coin": c, "side": "RESIZE", "px": px[c], "units": u, "reason": "RESIZE",
                                                  "why": f"trim to equity/{self.max_pos} = ${target:.2f} (max positions {old} → {self.max_pos})", "bar_t": self.bar_of(t), "tf": self.tf, "timing": self.timing, "observed": px[c]})
            self.fold(); q = self.P[c]; tr = self.trades1[-1]
            g["sync"][c] = {"side": "RESIZE", "px": px[c], "units_sold": u, "proceeds": tr["proceeds"], "fee": tr["exit_fee"], "units_after": q["units"], "cost_after": q["cost"],
                            "value_after": self.pos_value(q, px[c]), "tp_px": q["tp_px"], "sl_px": q["sl_px"], "pos_side": q.get("side", "long")}
            self.event("fill", f"{c}: paper RESIZE (partial SELL) {u:.6g} units ${u * px[c]:.2f} [{self.tf} · RESIZE] at {px[c]:.6g} (live price, trim to equity/{self.max_pos} ${target:.2f}) {ms_wib(t)} WIB, "
                               f"fee ${tr['exit_fee']:.4f}, realised P&L ${tr['pnl']:+.2f}; now {q['units']:.6g} units, cost ${q['cost']:.2f}, entry {q['entry_px']:.6g}, TP {q['tp_px']:.6g} / SL {q['sl_px']:.6g}",
                       key=f"f|{c}|{t}|RESIZE", coin=c, side="RESIZE", px=px[c], fill_t=t, reason="RESIZE", tf=self.tf, pnl=tr["pnl"])
        eq1 = self.equity_now(px)
        g.update(equity_after=eq1, fees=self.fees1 - fees0, cash_after=self.cash, held_after=sorted(self.P)); self.save_segs()
        self.event("mode_switch", f"MAX POSITIONS {old} → {self.max_pos} at {ms_wib(n)} WIB: entry size = equity/{self.max_pos} (≈${target:.2f}) capped at cash; "
                   + (", ".join(f"{c} trimmed {v['units_sold']:.6g} @ {v['px']:.6g} → ${v['value_after']:.2f}" for c, v in g["sync"].items()) or "no trims")
                   + f"; balance ${eq0:.2f} carried over → ${eq1:.2f} (trim fees ${self.fees1 - fees0:.4f}); cash ${cash0:.2f} → ${self.cash:.2f}, {self.max_pos - len(self.P)} free slots",
                   key=f"mp|{n}", from_max=old, to_max=self.max_pos, equity_before=eq0, equity_after=eq1)
        self.mark(n, px)
    def save_eval(self):
        cut = now_ms() - 3 * 86_400_000
        self.flags = {k: v for k, v in self.flags.items() if int(k.split("|")[1]) > cut}
        write_json(self.p("run/intrabar_eval.json"), {"last_eval_ms": self.last_eval, "last_ct": self.last_ct, "flags": self.flags, "tf": self.tf})

    # ------------------------------------------------------------ data
    def prices(self, coins):
        q = api("/api/v3/ticker/price", {"symbols": json.dumps([c + "USDT" for c in coins], separators=(",", ":"))})
        return {x["symbol"][:-4]: float(x["price"]) for x in q}
    def refresh(self, coins, n):
        B = TFS[self.tf]
        for c in coins:
            try:
                rows = self.store.get(c + "USDT", self.tf, pb.seg_frm(self.tf, n))
                df = closed_df(rows, n)
                if len(df) < WARMUP + 60: continue
                self.D[c] = {"df": df, "ind": core.indicator(df, rsi_len=self.rsi_len), "opens": {r[0]: r[1] for r in rows}, "rows": rows}
                fr = [r for r in rows if r[0] <= n < r[0] + B]
                if fr: self.forming[c] = {"bo": fr[0][0], "o": fr[0][1], "h": fr[0][2], "l": fr[0][3]}
            except Exception as e:
                self.event("error", f"klines {c} {self.tf}: {e}", coin=c)
            self.beat()
    def refresh_forming(self, c):
        B = TFS[self.tf]; n = now_ms()
        try:
            r = klines(c + "USDT", self.tf, None, 1)[-1]
            if r[0] <= n < r[0] + B:
                f = self.forming.get(c)
                if f and f["bo"] == r[0]: f["h"] = max(f["h"], r[2]); f["l"] = min(f["l"], r[3])
                else: self.forming[c] = {"bo": r[0], "o": r[1], "h": r[2], "l": r[3]}
        except Exception as e: log.warning(f"forming {c}: {e}")
    def check_universe(self, force=False):
        if not self.excl_n: return
        try: u = json.load(open(self.p("run/universe.json")))
        except Exception: u = {}
        if not force and u.get("checked_ms", 0) > now_ms() - 86_400_000 and u.get("n") == self.excl_n:
            self.excluded = u.get("excluded", {}); return
        try:
            top = top_mcap_symbols(self.excl_n)
            ex = {c: top[c] for c in self.universe if c in top}
            if set(ex) != set(self.excluded): self.event("universe", f"top-{self.excl_n} mcap check (CoinGecko): " + (", ".join(f"{c} #{v['rank']}" for c, v in ex.items()) + " dropped from new entries" if ex else f"none of the {len(self.universe)} coins is in the top {self.excl_n}"), key=f"u|{now_ms()//3600000}")
            self.excluded = ex
            write_json(self.p("run/universe.json"), {"checked_ms": now_ms(), "checked_wib": ms_wib(now_ms()), "n": self.excl_n, "excluded": ex, "top": sorted(top), "source": "CoinGecko /coins/markets"})
        except Exception as e:
            self.excluded = u.get("excluded", {}); log.warning(f"universe check: {e}")

    # ------------------------------------------------------------ rules
    def process_closed(self, n):
        """Handle every closed bar of the active timeframe since last_ct (normally just one): catch-up TP/SL, ARROW(_CLOSE) exits
        and entries at the next bar open. Then mark equity."""
        B = TFS[self.tf]; cur_bo = n // B * B
        if self.last_ct is None or self.last_ct < self.ep["from_ms"] // B * B + B: self.last_ct = max(self.last_ct or 0, self.ep["from_ms"] // B * B + B)
        bars = list(range(self.last_ct - B + B, cur_bo + 1, B)) if self.last_ct else []
        bars = [ct for ct in range(self.last_ct, cur_bo + 1, B)]
        for ct in bars:
            bo = ct - B
            idx = {c: self.D[c]["df"].index[self.D[c]["df"].t.values == bo] for c in self.D}
            idx = {c: int(v[0]) for c, v in idx.items() if len(v)}
            # 1) catch-up TP/SL on bar high/low for bars not covered live (SL first; shorts: SL above entry, TP below)
            if bo < self.cover_from:
                for c in sorted(self.P):
                    p = self.P[c]; i = idx.get(c)
                    if i is None or p["entry_t"] > bo: continue
                    df = self.D[c]["df"]; o, h, l = (float(df[x].values[i]) for x in "ohl")
                    tfill = max(bo, p["entry_t"] + 1)
                    if p.get("side") == "short":
                        if p["sl_px"] and h >= p["sl_px"]: self.fill(c, "COVER", o if o >= p["sl_px"] else p["sl_px"], "SL", "catch-up: bar high, short", t=tfill)
                        elif p["tp_px"] and l <= p["tp_px"]: self.fill(c, "COVER", o if o <= p["tp_px"] else p["tp_px"], "TP", "catch-up: bar low, short", t=tfill)
                    else:
                        if p["sl_px"] and l <= p["sl_px"]: self.fill(c, "SELL", o if o <= p["sl_px"] else p["sl_px"], "SL", "catch-up: bar low", t=tfill)
                        elif p["tp_px"] and h >= p["tp_px"]: self.fill(c, "SELL", o if o >= p["tp_px"] else p["tp_px"], "TP", "catch-up: bar high", t=tfill)
            # 2+3) closed-bar arrows (down first): close the opposite side, then try to open with the 2% filter
            rs = "ARROW" if self.timing == "close" else "ARROW_CLOSE"
            downs, ups = [], []
            for c in self.active:
                i = idx.get(c)
                if i is None: continue
                a = self.D[c]["ind"]["arrow"][i]
                if not a: continue
                if self.timing == "intrabar" and (self.flags.get(f"{c}|{bo}") or self.did(c, "OPEN", bo) or self.did(c, "CLOSE", bo)): continue
                rec = (-pb.strength_at(self.D[c]["df"], self.D[c]["ind"], i), c)
                (ups if a > 0 else downs).append(rec)
            for ng, c in sorted(downs):
                px = self.D[c]["opens"].get(ct)
                if px is None: continue
                i = idx[c]; r_arr = self.D[c]["ind"].get("rsi")
                rv = float(r_arr[i]) if r_arr is not None and math.isfinite(r_arr[i]) else None
                self.handle_arrow(c, -1, px, rs, f"▼ down arrow on the closed {self.tf} bar {ms_wib(bo)[11:16]}, next bar open", ct, bo, -ng, {c: px}, rsi_val=rv)
            for ng, c in sorted(ups):
                px = self.D[c]["opens"].get(ct)
                if px is None: continue
                i = idx[c]; r_arr = self.D[c]["ind"].get("rsi")
                rv = float(r_arr[i]) if r_arr is not None and math.isfinite(r_arr[i]) else None
                self.handle_arrow(c, 1, px, rs, f"▲ up arrow on the closed {self.tf} bar {ms_wib(bo)[11:16]}, next bar open", ct, bo, -ng, {c: px}, rsi_val=rv)
            # 4) signal events + equity mark at the close
            for c, i in idx.items():
                a = self.D[c]["ind"]["arrow"][i]
                if a: self.event("signal", f"{c}: {'▲ up' if a > 0 else '▼ down'} arrow on the closed {self.tf} bar {ms_wib(bo)}–{ms_wib(ct)[11:16]} WIB (close {self.D[c]['df'].c.values[i]:.6g}, exit line {self.D[c]['ind']['ex_line'][i]:.6g})", key=f"a|{self.tf}|{c}|{bo}", coin=c, side="up" if a > 0 else "down", bar_t=bo, tf=self.tf, arrow=True)
            cl = {c: float(self.D[c]["df"].c.values[i]) for c, i in idx.items()}
            self.mark(ct, cl)
            self.last_ct = ct + B
        self.last_ct = cur_bo + B if self.last_ct is None or self.last_ct <= cur_bo else self.last_ct
        self.save_eval()
    def skip(self, c, bo, s, why, want="long", extra=None):
        sk = {"coin": c, "sig_t": bo, "fill_t": bo + TFS[self.tf], "side": want, "tf": self.tf, "strength": s, "reason": why, "held": sorted(self.P), "t": now_ms()}
        if extra: sk.update(extra)
        k = f"k|{c}|{bo}|{why}|{want}"
        if k not in self.seen: append_jsonl(self.p("skips.jsonl"), sk)
        arrow = "▲ up" if want == "long" else "▼ down"
        det = ""
        if why == "slots full": det = f" ({'+'.join(sorted(self.P))} held, strength {s:.2f} ATR)"
        elif why == "filter_2pct_24h" and extra and extra.get("ret_24h") is not None:
            det = f" (24h {extra['ret_24h']*100:+.2f}% vs {'+2%' if want=='long' else '−2%'})"
        elif why == "rsi_filter":
            rf = self.rsi_filter; rv = extra.get("rsi") if extra else None
            if rv is not None:
                cmp_s = f"<{rf['long_below']:g}" if want == "long" else f">{rf['short_above']:g}"
                det = f" (RSI({rf['length']}) {rv:.1f} vs {cmp_s})"
        self.event("skip", f"{c}: {arrow} arrow ({self.tf} bar {ms_wib(bo)[11:16]} WIB) skipped: {why}{det}; not queued", key=k, coin=c, reason=why, side=want)
    def bh_value(self, prices):
        px0 = self.ep.get("px") or {}; ks = [c for c in self.universe if px0.get(c) and prices.get(c)]
        return self.ep["bh"] * (sum(prices[c] / px0[c] for c in ks) / len(ks)) if ks else self.ep["bh"]
    @property
    def allow_short(self): return self.sides == "ls"
    def pos_value(self, p, px):
        px = p["entry_px"] if px is None else px
        if p.get("side") == "short":
            coll = p.get("collateral") if p.get("collateral") is not None else p["units"] * p["entry_px"]
            return core.cash_short_val(coll, p["units"], px)
        return p["units"] * px
    def equity_now(self, prices):
        return self.cash + sum(self.pos_value(p, prices.get(c)) for c, p in self.P.items())
    def mark(self, t, prices):
        pr = dict(self.live); pr.update(prices)
        append_jsonl(self.p("equity1.jsonl"), {"t": int(t), "eq": round(self.equity_now(pr), 6), "bh": round(self.bh_value(pr), 6)})

    # ------------------------------------------------------------ entry filter + sides
    def close_15m(self, coin, t_ms):
        """Close of the last 15m bar that had already closed by t_ms (so the 24h lookback is independent of the active tf)."""
        bo = (int(t_ms) // pb.BAR) * pb.BAR - pb.BAR
        sym = coin + "USDT"; key = (sym, "15m")
        rows = self.store.mem.get(key)
        if rows is None and os.path.exists(self.store.path(sym, "15m")):
            try:
                rows = json.load(open(self.store.path(sym, "15m"))); self.store.mem[key] = rows
            except Exception: rows = None
        def find(rs):
            px = None
            for r in rs or []:
                if r[0] <= bo: px = r[4]
                else: break
            return px
        px = find(rows)
        if px is None:
            try:
                part = klines(sym, "15m", max(0, bo - pb.BAR), 8)
                if part:
                    have = {r[0]: r for r in (rows or [])}
                    have.update({r[0]: r for r in part})
                    rows = [have[k] for k in sorted(have)]
                    self.store.mem[key] = rows
                    px = find(rows)
            except Exception as e:
                log.warning(f"15m lookback {coin}: {e}")
        return px
    def ret_24h(self, coin, px, n=None):
        """Current price vs 15m close from 24h ago. Returns a fraction or None if data is missing."""
        if px is None or px <= 0: return None
        px0 = self.close_15m(coin, (n or now_ms()) - 86_400_000)
        if not px0 or px0 <= 0: return None
        return px / px0 - 1
    def entry_filter_ok(self, coin, px, want="long", n=None):
        if not self.entry_filter_2pct: return True, None, None
        ret = self.ret_24h(coin, px, n)
        if ret is None: return False, "filter_2pct_24h", None
        if want == "long" and ret < 0.02: return False, "filter_2pct_24h", ret
        if want == "short" and ret > -0.02: return False, "filter_2pct_24h", ret
        return True, None, ret
    def rsi_filter_ok(self, coin, want="long", rsi_val=None):
        rf = self.rsi_filter
        if not rf.get("enabled"):
            return True, None, rsi_val
        if rsi_val is None or not math.isfinite(rsi_val):
            return False, "rsi_filter", None
        if want == "long" and not (rsi_val < rf["long_below"]):
            return False, "rsi_filter", rsi_val
        if want == "short" and not (rsi_val > rf["short_above"]):
            return False, "rsi_filter", rsi_val
        return True, None, rsi_val
    def _try_open(self, c, want, px, reason, why, t, bo, strength, prices=None, rsi_val=None):
        if c in self.P: return
        free = self.max_pos - len(self.P)
        if free <= 0: self.skip(c, bo, strength, "slots full", want=want); return
        if pausectl.current(): self.skip(c, bo, strength, "paused", want=want); return
        if self.cash < 1.0: self.skip(c, bo, strength, "no cash", want=want); return
        ok, whyf, ret = self.entry_filter_ok(c, px, want, t)
        if not ok: self.skip(c, bo, strength, whyf, want=want, extra={"ret_24h": ret}); return
        ok_r, whyr, rval = self.rsi_filter_ok(c, want, rsi_val)
        if not ok_r:
            self.skip(c, bo, strength, whyr, want=want, extra={"rsi": rval})
            return
        side = "BUY" if want == "long" else "SHORT"
        self.fill(c, side, px, reason, why, t=t, amt=self.entry_amt(prices), observed=px)
    def handle_arrow(self, c, direction, px, reason, why, t, bo, strength, prices=None, rsi_val=None):
        """Up arrow: close a short, then try to open a long. Down arrow: close a long, then (if Long+Short) try to open a short."""
        if px is None: return
        p = self.P.get(c); have = (p or {}).get("side", "long") if p else None
        if direction > 0:
            if have == "long": return
            if have == "short":
                self.fill(c, "COVER", px, reason, why + " · close short", t=t, observed=px); t = t + 1
            self._try_open(c, "long", px, reason, why, t, bo, strength, prices, rsi_val=rsi_val)
        else:
            if have == "short": return
            if have == "long":
                self.fill(c, "SELL", px, reason, why + (" · close long" if self.allow_short else ""), t=t, observed=px); t = t + 1
                if not self.allow_short: return
            if self.allow_short:
                self._try_open(c, "short", px, reason, why, t, bo, strength, prices, rsi_val=rsi_val)
    def persist_settings(self):
        write_json(self.p("run/settings_applied.json"), {
            "entry_filter_2pct": self.entry_filter_2pct, "sides": self.sides,
            "filter_at_ms": self.filter_req_handled, "sides_at_ms": self.sides_req_handled,
            "reset_at_ms": getattr(self, "reset_req_handled", 0),
            "wib": ms_wib(now_ms())})
        try:
            cfg = json.load(open(self.p("config.json")))
            cfg["entry_filter_2pct"] = self.entry_filter_2pct
            cfg["sides"] = self.sides
            tmp = self.p("config.json.tmp"); open(tmp, "w").write(json.dumps(cfg, indent=1)); os.replace(tmp, self.p("config.json"))
        except Exception as e:
            log.warning(f"persist settings to config.json: {e}")
    def check_settings(self):
        """Pick up run/filter_request.json and run/sides_request.json (from the poller or the local dashboard)."""
        changed = False
        try: freq = json.load(open(self.p("run/filter_request.json")))
        except Exception: freq = None
        if freq:
            at = int(freq.get("at_ms") or 0)
            if at > self.filter_req_handled and at >= self.start_ms:
                self.filter_req_handled = at
                on = bool(freq.get("entry_filter_2pct"))
                if on != self.entry_filter_2pct:
                    self.entry_filter_2pct = on; changed = True
                    self.event("filter", "ENTRY FILTER +2%/24h " + ("ON: new longs need +2% over the last 24 hours; new shorts need −2% over 24 hours (15m candles, same on every timeframe). Existing positions and exits are unchanged."
                               if on else "OFF: new entries on every qualifying arrow (filter off)."), key=f"flt|{at}")
        try: sreq = json.load(open(self.p("run/sides_request.json")))
        except Exception: sreq = None
        if sreq:
            at = int(sreq.get("at_ms") or 0)
            if at > self.sides_req_handled and at >= self.start_ms:
                self.sides_req_handled = at
                raw = str(sreq.get("sides") or "long").lower()
                new = "ls" if raw in ("ls", "long_short", "long+short", "both") else "long"
                if new != self.sides:
                    old = self.sides; self.sides = new; changed = True
                    if new == "long":
                        px = self.prices(self.coins_all); self.live.update(px); n = now_ms()
                        closed = []
                        for c in list(self.P):
                            if self.P[c].get("side") == "short" and px.get(c):
                                self.fill(c, "COVER", px[c], "MODE", "switch to Long-only: close open short", t=n, observed=px[c]); closed.append(c)
                        self.event("sides", "SIDES Long+Short → Long-only: down arrows no longer open shorts; open shorts closed immediately"
                                   + (f" ({', '.join(closed)})" if closed else " (none open)") + ".", key=f"sides|{at}")
                    else:
                        self.event("sides", "SIDES Long-only → Long+Short (1x paper): an up arrow closes a short and opens a long; a down arrow closes a long and opens a short. Same 5-slot pool, TP +$5 / SL −$5 net.", key=f"sides|{at}")
        if changed:
            self.persist_settings()
            try: self.write_all(now_ms())
            except Exception as e: log.warning(f"write after settings: {e}"); self.write_live()

        elif freq or sreq:
            self.persist_settings()
        return changed

    RESET_FILES = (
        "state.json", "ledger.jsonl", "trades.jsonl", "equity.jsonl", "equity1.jsonl",
        "events.jsonl", "skips.jsonl", "tpsl_exits.jsonl", "epoch0.json", "segments.json",
        "config.json", "live.json", "summary.json", "run/intrabar_eval.json",
        "run/settings_applied.json",
    )
    RESET_WIPE = (
        "state.json", "ledger.jsonl", "trades.jsonl", "equity.jsonl", "equity1.jsonl",
        "events.jsonl", "skips.jsonl", "tpsl_exits.jsonl", "epoch0.json", "segments.json",
        "run/intrabar_eval.json",
    )

    def archive_run(self, tag):
        """Copy the current paper book to archive/pre_reset_<tag>/ (live files are left in place until wipe)."""
        dest = self.p(f"archive/pre_reset_{tag}")
        os.makedirs(os.path.join(dest, "run"), exist_ok=True)
        for f in self.RESET_FILES:
            src = self.p(f)
            if os.path.exists(src):
                dst = os.path.join(dest, f)
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(src, dst)
        return dest

    def persist_capital(self):
        try:
            cfg = json.load(open(self.p("config.json")))
            cfg["capital"] = self.cap
            cfg["start_wib"] = self.start_wib
            tmp = self.p("config.json.tmp")
            open(tmp, "w").write(json.dumps(cfg, indent=1))
            os.replace(tmp, self.p("config.json"))
        except Exception as e:
            log.warning(f"persist capital to config.json: {e}")

    def do_reset(self, capital, why="dashboard"):
        """Archive the current paper run and start a fresh book at `capital`. No catch-up replay.

        last_ct is set to the close of the current bar so process_closed does not trade historical
        bars against the new cash. Open positions are not carried over (they stay in the archive).
        """
        cap = valid_capital(capital)
        if cap is None:
            return False
        n = now_ms()
        tag = datetime.fromtimestamp(n / 1000, WIB).strftime("%Y%m%d_%H%M%S")
        tf = self.tf if getattr(self, "ep", None) or getattr(self, "segs", None) else "15m"
        B = TFS[tf]
        try:
            old_bal = self.equity_now(self.live) if getattr(self, "P", None) is not None else None
        except Exception:
            old_bal = getattr(self, "cash", None)
        old_held = ",".join(sorted(getattr(self, "P", {}) or {})) or "none"
        dest = self.archive_run(tag)
        start_wib = datetime.fromtimestamp(n / 1000, WIB).strftime("%Y-%m-%dT%H:%M")
        self.cap = cap
        self.start_wib = start_wib
        self.start_ms = wib_to_ms(start_wib)
        self.persist_capital()
        for f in self.RESET_WIPE:
            fp = self.p(f)
            if os.path.exists(fp):
                os.remove(fp)
        self.seen = set(); self.fill_px = {}; self._tpsl_done = set()
        self.flags = {}; self.P = {}; self.cash = cap
        self.trades1 = []; self.fills1 = {}; self.fees1 = 0.0
        self.last_ct = n // B * B + B
        self.last_eval = n
        self.cover_from = n
        try:
            px = dict(self.live or {})
            if not px and self.universe:
                px = self.prices(self.universe)
        except Exception as e:
            log.warning(f"reset prices: {e}"); px = {}
        ep0 = {"frozen_ms": n, "frozen_wib": ms_wib(n), "cash": cap, "balance": cap, "bh": cap, "coins_old": [],
               "trades": [], "equity": [], "fills": {}, "fees": 0.0, "segments": [], "switches": [], "skipped": [],
               "n_skipped": 0, "pool_since_ms": n, "watch": {}, "fresh": True}
        write_json(self.p("epoch0.json"), ep0); self.epoch0 = ep0
        g = {"tf": tf, "from_tf": tf, "mode": "arrow_long", "from_mode": "arrow_long", "sizing": "pool", "from_sizing": "pool",
             "engine": "live_pool", "signal_timing": self.timing, "from_timing": self.timing, "coins": self.universe, "from_coins": self.universe,
             "exclude_top_mcap": self.excl_n, "max_positions": self.max_pos, "tp_usd": self.tp_usd, "sl_usd": self.sl_usd,
             "from_ms": n, "from_wib": ms_wib(n), "balance": cap, "bh": cap, "px": px, "sync": {}, "skipped": [],
             "why": f"fresh start ${cap:.0f} at {start_wib} WIB (user reset; earlier run archived to archive/pre_reset_{tag})"}
        self.segs = [{"tf": tf, "from_ms": n, "mode": "arrow_long"}] + [g]
        self.ep = g; self.ep_segs = [g]; self.save_segs()
        for empty in ("ledger.jsonl", "equity1.jsonl", "trades.jsonl", "skips.jsonl", "tpsl_exits.jsonl", "events.jsonl"):
            open(self.p(empty), "w").close()
        self.save_eval()
        self.state = None
        extra = f"held {old_held}"
        if old_bal is not None:
            extra += f", last balance ${old_bal:.2f}"
        self.event("mode_switch", f"FRESH START · ${cap:.2f} paper capital · start {start_wib} WIB · earlier run archived to archive/pre_reset_{tag} ({extra})",
                   key=f"fresh|{n}")
        if not getattr(self, "live", None):
            self.live = px
        if not hasattr(self, "liveind"):
            self.liveind = {}
        try:
            self.write_all(n)
        except Exception as e:
            log.warning(f"write after reset: {e}")
            try: self.write_live()
            except Exception: pass
        log.info(f"paper reset to ${cap:.2f} at {start_wib} WIB; archived to {os.path.relpath(dest, self.d)}")
        return True

    def check_reset(self):
        """Pick up run/reset_request.json. Invalid or stale values are rejected and consumed; nothing is archived."""
        try: req = json.load(open(self.p("run/reset_request.json")))
        except Exception: return False
        try: at = int(req.get("at_ms") or 0)
        except Exception: return False
        handled = getattr(self, "reset_req_handled", 0)
        if at <= handled: return False
        n = now_ms()
        self.reset_req_handled = at
        try:
            os.remove(self.p("run/reset_request.json"))
        except Exception: pass
        self.persist_settings()
        if at < n - 10 * 60 * 1000 or at > n + 120_000:
            log.warning(f"reset request ignored (stale or future at_ms={at})")
            return False
        cap = valid_capital(req.get("capital"))
        if cap is None:
            self.event("error", f"RESET rejected: starting balance {req.get('capital')!r} is invalid (need a number from $1 to $1,000,000).",
                       key=f"resetbad|{at}")
            return False
        return self.do_reset(cap, why=req.get("why") or "dashboard")

    def check_commands(self):
        """Pick up run/*_request.json as soon as they appear (called every loop tick)."""
        changed = False
        try:
            if self.check_tf():
                changed = True
                self.publish_status("tf", self.tf)
        except Exception as e:
            log.warning(f"check_tf: {e}")
        try:
            if self.check_settings():
                changed = True
                self.publish_status("settings")
        except Exception as e:
            log.warning(f"check_settings: {e}")
        try:
            if self.check_reset():
                changed = True
                self.publish_status("reset", self.cap)
        except Exception as e:
            log.warning(f"check_reset: {e}")
        return changed

    def publish_status(self, of=None, value=None):
        """Tell the phone dashboard the applied settings immediately (do not wait for bot.json)."""
        try:
            import tf_poller as tfp
            rel = tfp.load_relay(self.d)
            if not rel:
                return
            n = now_ms()
            payload = {
                "cmd": "status", "of": of, "value": value if value is not None else of,
                "tf": self.tf, "filter": bool(self.entry_filter_2pct), "sides": self.sides,
                "capital": self.cap, "start_wib": self.start_wib, "start_ms": self.start_ms,
                "applied_at_ms": n, "at_ms": n,
            }
            tfp.ntfy_pub(rel["host"], rel["topic"], payload)
            log.info(f"relay status published ({of}={payload['value']}, tf={self.tf}, sides={self.sides}, filter={self.entry_filter_2pct}, cap={self.cap})")
        except Exception as e:
            log.warning(f"relay status: {e}")


    def maybe_publish_live(self, force=False):
        """Write a compact live snapshot for the public page; ntfy at a low cadence (not every tick)."""
        try:
            import live_snap
            snap = live_snap.write_snap()
            # Prefer the dedicated snap server for 5–10 s HTTP; ntfy is a fallback heartbeat only.
            if force:
                live_snap.maybe_ntfy_live(snap, force=True)
        except Exception as e:
            log.warning(f"live snap: {e}")

    def tick(self):
        """5 s: one price call for all symbols, live TP/SL, intrabar arrows."""
        coins = self.coins_all
        try: self.live = self.prices(coins); self.live_t = now_ms()
        except Exception as e: log.warning(f"live price: {e}"); return
        n = self.live_t; B = TFS[self.tf]; bo = n // B * B; self.last_eval = n
        if coins:
            self.refresh_forming(coins[self.rr % len(coins)]); self.rr += 1
        for c, px in self.live.items():
            f = self.forming.get(c)
            if f and f["bo"] == bo: f["h"] = max(f["h"], px); f["l"] = min(f["l"], px)
        # live TP / SL (at the level)
        for c in sorted(self.P):
            p = self.P[c]; px = self.live.get(c)
            if px is None: continue
            if p.get("side") == "short":
                k = "SL" if p["sl_px"] and px >= p["sl_px"] else ("TP" if p["tp_px"] and px <= p["tp_px"] else None)
                if k: self.fill(c, "COVER", p["sl_px"] if k == "SL" else p["tp_px"], k, f"live 5 s check, short, price {px:.6g} at/through the level", t=n, observed=px)
            else:
                k = "SL" if p["sl_px"] and px <= p["sl_px"] else ("TP" if p["tp_px"] and px >= p["tp_px"] else None)
                if k: self.fill(c, "SELL", p["sl_px"] if k == "SL" else p["tp_px"], k, f"live 5 s check, price {px:.6g} at/through the level", t=n, observed=px)
        if self.timing != "intrabar": return
        ups = []; downs = []; nd = 0
        for c in coins:
            d = self.D.get(c); f = self.forming.get(c); px = self.live.get(c)
            if not d or not f or px is None or f["bo"] != bo or int(d["df"].t.values[-1]) != bo - B: continue
            a, ex, s, rval = live_exit(d["df"], f["o"], max(f["h"], px), min(f["l"], px), px, rsi_len=self.rsi_len); nd += 1
            self.liveind[c] = {"arrow": a, "exit_line": ex, "strength": s, "price": px, "bar_t": bo, "t": n, "rsi": rval}
            key = f"{c}|{bo}"
            if a < 0 and c in self.active and not self.flags.get(key) and not self.did(c, "CLOSE", bo):
                downs.append((-s, c, px, ex, rval))
            elif a > 0 and c in self.active and not self.flags.get(key):
                if self.did(c, "OPEN", bo) or self.did(c, "CLOSE", bo): self.flags[key] = "up"; continue
                ups.append((-s, c, px, ex, rval))
        for ng, c, px, ex, rval in sorted(downs):
            self.flags[f"{c}|{bo}"] = "down"
            rtxt = f", RSI({self.rsi_len}) {rval:.1f}" if rval is not None else ""
            self.handle_arrow(c, -1, px, "ARROW_INTRABAR", f"▼ down arrow intrabar: live {px:.6g} < exit line {ex:.6g}{rtxt}", n, bo, -ng, rsi_val=rval)
        for ng, c, px, ex, rval in sorted(ups):
            self.flags[f"{c}|{bo}"] = "up"
            rtxt = f", RSI({self.rsi_len}) {rval:.1f}" if rval is not None else ""
            self.handle_arrow(c, 1, px, "ARROW_INTRABAR", f"▲ up arrow intrabar: live {px:.6g} > exit line {ex:.6g}, strength {-ng:.2f} ATR{rtxt}", n, bo, -ng, rsi_val=rval)
        if ups or downs: self.save_eval()
        if n - getattr(self, "_eval_log", 0) >= 60_000:
            self._eval_log = n; log.info(f"intrabar eval {self.tf} bar {ms_wib(bo)[11:16]}: {nd}/{len(coins)} coins, up {sum(1 for v in self.liveind.values() if v['arrow'] > 0 and v['bar_t'] == bo)}, held {'+'.join(sorted(self.P)) or 'none'}, cash ${self.cash:.2f}")
            self.save_eval()

    # ------------------------------------------------------------ tf switch (dashboard 15m/1h/4h)
    def do_switch(self, new, new_mode=None):
        old = self.tf; n = now_ms(); self.check_pause()
        px = self.prices(self.coins_all); self.live.update(px)
        g = {"tf": new, "from_tf": old, "mode": "arrow_long", "from_mode": "arrow_long", "sizing": "pool", "live_child": True, "signal_timing": self.timing,
             "from_ms": n, "from_wib": ms_wib(n), "px": px, "sync": {}, "skipped": [], "max_positions": self.max_pos, "tp_usd": self.tp_usd, "sl_usd": self.sl_usd}
        self.segs.append(g); self.ep_segs.append(g); self.save_segs()
        self.D = {}; self.forming = {}; self.liveind = {}; self.refresh(self.coins_all, n)
        B_new = TFS[new]; bo_new = n // B_new * B_new
        if self.timing == "intrabar":
            for c in self.coins_all:
                d = self.D.get(c); f = self.forming.get(c); p = px.get(c)
                if d and f and p is not None and f["bo"] == bo_new and int(d["df"].t.values[-1]) == bo_new - B_new:
                    a, ex, s, rval = live_exit(d["df"], f["o"], max(f["h"], p), min(f["l"], p), p, rsi_len=self.rsi_len)
                    self.liveind[c] = {"arrow": a, "exit_line": ex, "strength": s, "price": p, "bar_t": bo_new, "t": n, "rsi": rval}
        why = f"switch {old}→{new}"
        def _st(c):
            d = self.D.get(c)
            return (d["ind"]["arrow_state"][-1] if d else 0), (pb.strength_at(d["df"], d["ind"], len(d["df"]) - 1) if d else 0)
        def _rsi_now(c):
            li = self.liveind.get(c)
            if li and li.get("bar_t") == bo_new and li.get("rsi") is not None:
                return li["rsi"]
            d = self.D.get(c)
            if d and "rsi" in d["ind"]:
                rv = d["ind"]["rsi"][-1]
                if math.isfinite(rv): return float(rv)
            return None
        # close / flip held positions that no longer match the latest arrow, then open flats (strongest first)
        for c in list(sorted(self.P)):
            if c not in px: continue
            st, s = _st(c); side = self.P[c].get("side", "long")
            if side == "long" and st <= 0:
                if st < 0:
                    self.handle_arrow(c, -1, px[c], "SWITCH", why + ": latest arrow down", n, self.bar_of(n), s, px, rsi_val=_rsi_now(c))
                else:
                    self.fill(c, "SELL", px[c], "SWITCH", why + ": latest arrow not up", t=n, observed=px[c])
                g["sync"][c] = {"side": ("COVER/SHORT" if self.P.get(c) and self.P[c].get("side") == "short" else "SELL"), "px": px[c]}
            elif side == "short" and st >= 0:
                if st > 0:
                    self.handle_arrow(c, 1, px[c], "SWITCH", why + ": latest arrow up", n, self.bar_of(n), s, px, rsi_val=_rsi_now(c))
                else:
                    self.fill(c, "COVER", px[c], "SWITCH", why + ": latest arrow not down", t=n, observed=px[c])
                g["sync"][c] = {"side": ("BUY" if self.P.get(c) and self.P[c].get("side") == "long" else "COVER"), "px": px[c]}
        cand_up = sorted((-pb.strength_at(self.D[c]["df"], self.D[c]["ind"], len(self.D[c]["df"]) - 1), c) for c in self.active if c in self.D and c not in self.P and c in px and self.D[c]["ind"]["arrow_state"][-1] > 0)
        for ng, c in cand_up:
            before = c in self.P
            self.handle_arrow(c, 1, px[c], "SWITCH", why + ": latest arrow up (strongest first)", n + 1, self.bar_of(n), -ng, px, rsi_val=_rsi_now(c))
            if c in self.P and not before: g["sync"][c] = {"side": "BUY", "px": px[c]}
            elif c not in self.P and c not in g["sync"]: g["skipped"].append(c + " (slots/filter/pause)")
        if self.allow_short:
            cand_dn = sorted((-pb.strength_at(self.D[c]["df"], self.D[c]["ind"], len(self.D[c]["df"]) - 1), c) for c in self.active if c in self.D and c not in self.P and c in px and self.D[c]["ind"]["arrow_state"][-1] < 0)
            for ng, c in cand_dn:
                before = c in self.P
                self.handle_arrow(c, -1, px[c], "SWITCH", why + ": latest arrow down (strongest first)", n + 1, self.bar_of(n), -ng, px, rsi_val=_rsi_now(c))
                if c in self.P and not before: g["sync"][c] = {"side": "SHORT", "px": px[c]}
                elif c not in self.P and c not in g["sync"]: g["skipped"].append(c + " (slots/filter/pause)")
        self.save_segs(); B = TFS[new]; self.last_ct = n // B * B + B; self.cover_from = n
        self.event("switch", f"TIMEFRAME {old} → {new} at {ms_wib(n)} WIB: " + (", ".join(f"{c} {v['side']} @ {v['px']:.6g}" for c, v in g["sync"].items()) or "no sync fills") + (f"; skipped {', '.join(g['skipped'])}" if g["skipped"] else ""), key=f"tf|{n}", tf=new, from_tf=old)
        self.mark(n, px); self.write_all(now_ms())

    # ------------------------------------------------------------ outputs
    def beat(self):
        if threading.current_thread() is threading.main_thread() and now_ms() - self._beat_t >= 20_000: self.write_live()
    def write_all(self, now):
        ep0 = self.epoch0; coins = []; B = TFS[self.tf]
        trades = sorted(ep0["trades"] + self.trades1, key=lambda t: t["exit_t"])
        eqs = [{"t": e["t"], "eq": e["eq"], "bh": e["bh"]} for e in ep0["equity"]] + jsonl_read(self.p("equity1.jsonl"))
        pts = [(e["t"], e["eq"], e["bh"]) for e in eqs]
        for c in self.coins_all:
            d = self.D.get(c) or {}; df, ind = d.get("df"), d.get("ind"); p = self.P.get(c)
            lp = self.live.get(c) or (float(df.c.values[-1]) if df is not None else None)
            tr = [t for t in self.trades1 if t["coin"] == c]; pn = {t["exit_t"]: t["pnl"] for t in tr}
            fl = [dict(f, fee=(f["amt"] if f["side"] in ("BUY", "SHORT") else (f["units"] * f["px"] if f["side"] == "RESIZE" else 0)) * FEE, **({"pnl": pn.get(f["t"])} if f["side"] in ("SELL", "COVER", "RESIZE") else {"size": f.get("amt")}), **({"size": f["units"] * f["px"]} if f["side"] == "RESIZE" else {})) for f in self.fills1.get(c, [])]
            pos = None
            if p:
                vc = self.pos_value(p, lp or p["entry_px"])
                pos = {"side": p.get("side") or "long", "entry_t": p["entry_t"], "entry_px": p["entry_px"], "units": p["units"], "collateral": p.get("collateral") or 0.0, "cost": p["cost"], "entry_fee": p["fee"],
                       "bars": round((now - p["entry_t"]) / B, 2), "close_px": lp, "value_close": vc, "tf": p["tf"], "tp_px": p["tp_px"], "sl_px": p["sl_px"],
                       "tp_usd": p.get("tp_usd"), "sl_usd": p.get("sl_usd"), "entry_reason": p.get("reason")}
            ro = readout(df, ind, c) if ind is not None else {}
            li = self.liveind.get(c)
            if li and li["bar_t"] == now // B * B: ro["live"] = li
            alloc = self.ep["bh"] / len(self.universe) if c in self.universe else 0.0
            coins.append({"coin": c, "alloc": alloc, "started": True, "value_close": pos["value_close"] if pos else 0.0, "cash": 0.0, "fees": sum(t["fee"] for t in tr) + (p["fee"] if p else 0),
                          "pos": pos, "pending": None, "n_trades": len(tr), "pnl_realised": sum(t["pnl"] for t in tr),
                          "bh_value_close": alloc * (lp / self.ep["px"][c]) if lp and (self.ep.get("px") or {}).get(c) else alloc, "bh_c0": (self.ep.get("px") or {}).get(c),
                          "exposure_pct": 0.0, "ind": ro, "fills": fl, "sizing": "pool", "in_universe": c in self.universe, "excluded": self.excluded.get(c),
                          "tradable": c in self.active})
            if ind is not None: write_json(self.p(f"data/chart_{c}.json"), chart_payload(df, ind, {"fills": fl}))
        res = [{"trades": [], "fees": 0.0, "started": False, "alloc": 0, "bars_total": 0, "bars_in_pos": 0}]
        pf = portfolio(res, self.cap, self.start_ms, pts or [(now, self.cash, self.ep["bh"])])
        pf.update({k: v for k, v in portfolio([{"trades": trades, "fees": 0, "started": False, "alloc": 0, "bars_total": 0, "bars_in_pos": 0}], self.cap, self.start_ms, pts).items() if k not in ("equity", "balance_close", "bh_close", "max_dd_pct", "max_dd_usd")})
        pf["fees"] = ep0["fees"] + self.fees1
        bal = self.equity_now({c: x["pos"]["close_px"] for c, x in zip(self.coins_all, coins) if x["pos"]})
        free = self.max_pos - len(self.P)
        skips = [s for s in jsonl_read(self.p("skips.jsonl"))]
        slots = [{"coin": c, "side": p.get("side") or "long", "units": p["units"], "cost": p["cost"], "entry_t": p["entry_t"], "entry_px": p["entry_px"], "value_close": self.pos_value(p, self.live.get(c) or p["entry_px"]),
                  "tp_px": p["tp_px"], "sl_px": p["sl_px"], "reason": p.get("reason")} for c, p in sorted(self.P.items(), key=lambda x: x[1]["entry_t"])]
        pool = {"sizing": "pool", "max_positions": self.max_pos, "cash": self.cash, "slots_used": len(self.P), "free_slots": free, "next_trade_size": self.entry_amt() if free > 0 else 0.0, "sizing_rule": "equity / max_positions, capped at cash",
                "slots": slots, "tp_usd": self.tp_usd, "sl_usd": self.sl_usd, "since_ms": ep0.get("pool_since_ms"), "since_wib": ms_wib(ep0["pool_since_ms"]) if ep0.get("pool_since_ms") else None,
                "switches": ep0.get("switches", []) + [{"at_ms": self.ep["from_ms"], "at_wib": self.ep["from_wib"], "why": self.ep.get("why") if (ep0.get("fresh")) else "switch to intrabar + low-cap universe", "equity_before": self.ep["balance"], "equity_after": self.ep["balance"], "fees": 0.0, "cash_after": self.ep["balance"], "held_after": []}]
                            + [{"at_ms": g["from_ms"], "at_wib": g["from_wib"], "why": g.get("why") or f"max positions {g['from_max_positions']} → {g['max_positions']}", "equity_before": g.get("equity_before"), "equity_after": g.get("equity_after"),
                                "fees": g.get("fees", 0.0), "cash_after": g.get("cash_after"), "held_after": g.get("held_after", []), "trims": g.get("sync")} for g in self.ep_segs if g.get("from_max_positions")],
                "skipped": (ep0.get("skipped", []) + skips)[-200:], "n_skipped": ep0.get("n_skipped", 0) + len(skips)}
        last_bar = max((int(d["df"].t.values[-1]) for d in self.D.values()), default=now // B * B - B)
        tpsl = f" · TP ${self.tp_usd:g} / SL ${self.sl_usd:g}" if self.tp_usd or self.sl_usd else ""
        ms = [{"at_ms": g["from_ms"], "at_wib": g.get("from_wib"), "from": (g.get("from_mode") or "") + ("/" + g.get("from_sizing", "coin") if g.get("sizing") else ""), "to": g.get("mode") + ("/pool max %s" % g.get("max_positions") if g.get("sizing") == "pool" else "") + (" · " + g["signal_timing"] if g.get("engine") == "live_pool" else "") + (" (was max %s)" % g["from_max_positions"] if g.get("from_max_positions") else ""), "tf": g["tf"], "fills": g.get("sync")}
              for g in self.segs[1:] if g.get("from_mode") and (g.get("from_mode") != g.get("mode") or (g.get("sizing") or "coin") != (g.get("from_sizing") or "coin") or g.get("engine") == "live_pool" or (g.get("from_max_positions") and g.get("from_max_positions") != g.get("max_positions")))]
        ls = self.sides == "ls"
        if self.timing == "close":
            tm_txt = (MODES["arrow_long"] + " · Long+Short (1x paper)") if ls else MODES["arrow_long"]
        else:
            tm_txt = ("Long+Short (1x paper) · Exit Arrows evaluated intrabar on the live price (up arrow closes a short and opens a long; down arrow closes a long and opens a short)"
                      if ls else "Long-only · Exit Arrows evaluated intrabar on the live price (BUY the moment an up arrow appears, SELL on a down arrow)")
        filt = "ON (+2%/24h longs, −2%/24h shorts)" if self.entry_filter_2pct else "OFF"
        self.state = {"mode": "PAPER SIMULATION - no real orders, no API keys", "trade_mode": "arrow_long", "trade_mode_text": tm_txt,
                      "signal_timing": self.timing, "signal_timing_text": "Signals: " + TIMING_TEXT[self.timing], "engine": "live_pool",
                      "entry_filter_2pct": self.entry_filter_2pct, "rsi_filter": self.rsi_filter, "rsi_filter_text": self.rsi_filter_text, "sides": self.sides, "sides_text": "Long+Short" if ls else "Long-only",
                      "universe": self.universe, "universe_active": self.active, "excluded": self.excluded, "exclude_top_mcap": self.excl_n,
                      "universe_info": (self.cfg.get("_universe") or {}), "legacy_coins": ep0["coins_old"],
                      "active_tf": self.tf, "segments": self.segs, "mode_switches": ms, "sizing": "pool", "max_positions": self.max_pos, "pool": pool,
                      "sizing_text": f"Max positions {self.max_pos} · pool sizing{tpsl} · {'Long+Short' if ls else 'Long-only'} · entry filter {filt} · {self.rsi_filter_text} · Signals: {TIMING_TEXT[self.timing]}",
                      "rules": {"tf": self.tf, "sizing": f"shared pool, max {self.max_pos} open positions; size = equity / {self.max_pos} (≈${self.entry_amt() if self.cash > 0 else self.equity_now(self.live) / self.max_pos:.0f}) capped at cash (strongest (close-exit)/ATR first, extra arrows skipped, not queued)",
                                "tp_usd": self.tp_usd, "sl_usd": self.sl_usd, "tpsl": "net of the entry fee and the 0.1% exit fee; live 5 s price checks fill at the trigger level; catch-up on bar high/low, SL first; shorts use entry-price inverse P&L (1x paper)",
                                "direction": "long+short" if ls else "long-only", "sides": self.sides, "entry_filter_2pct": self.entry_filter_2pct, "rsi_filter": self.rsi_filter, "rsi_filter_text": self.rsi_filter_text, "signal_timing": self.timing,
                                "entry": (("Exit Arrow up, evaluated intrabar every ~5 s: close any short and open a long at the live price; Exit Arrow down: close any long and open a short (1x paper)" if ls else "Exit Arrow up, evaluated intrabar every ~5 s on the forming candle (live price as close): BUY at the live price at once")
                                          if self.timing == "intrabar" else ("Exit Arrow up on the closed bar: close short / open long; down arrow: close long / open short (next bar open)" if ls else "Exit Arrow up (close crosses above the Exit SSL HMA15) on the closed bar")),
                                "exit": (("Opposite arrow (flip) or TP/SL. Short TP is below entry, SL is above." if ls else "Exit Arrow down intrabar (live price crosses below the Exit SSL): SELL at once; or TP/SL")
                                         if self.timing == "intrabar" else ("Opposite arrow on the closed bar, or TP/SL" if ls else "Exit Arrow down on the closed bar; or TP/SL")),
                                "entry_filter": "ON: a new long needs the coin up at least +2% over the last 24 hours (15m candles); a new short needs it down at least −2%. Existing positions and exits are not filtered." if self.entry_filter_2pct else "OFF: new entries on every qualifying arrow.",
                                "fee_per_side": FEE, "compounding": True,
                                "fill": "intrabar: live Binance price at the 5 s poll (max 1 entry + 1 exit per coin per candle, no re-entry in the exit candle); arrows only visible on the closed bar: next bar open" if self.timing == "intrabar" else "next bar open (Binance kline open)"},
                      "start_wib": self.start_wib, "start_ms": self.start_ms, "first_fill_ms": -(-self.start_ms // pb.BAR) * pb.BAR, "capital": self.cap, "coins_traded": self.coins_all,
                      "epoch_from_ms": self.ep["from_ms"], "epoch_from_wib": self.ep["from_wib"], "epoch_balance": self.ep["balance"],
                      "last_bar_t": last_bar, "last_bar_wib": ms_wib(last_bar), "last_bar_close_wib": ms_wib(last_bar + B), "processed_at": now_ms(), "processed_at_wib": ms_wib(now_ms()),
                      "boot_ms": self.boot, "bars_processed_since_boot": self.bars_done, "source": pb.SRC["name"],
                      "balance_close": bal, "bh_close": pts[-1][2] if pts else self.ep["bh"], "pnl_close": bal - self.cap,
                      "stats": {k: pf.get(k) for k in ("max_dd_pct", "max_dd_usd", "n_trades", "wins", "losses", "win_rate", "profit_factor", "avg_win", "avg_loss", "best", "worst", "fees", "exposure_pct", "realised_pnl")},
                      "paused": self.paused, "pauses": pausectl.history(), "skipped_entries": skips[-50:],
                      "coins": coins, "watch": self.watch, "_seen": sorted(self.seen), "_first_trade_px": self.fill_px}
        write_json(self.p("state.json"), self.state)
        write_jsonl(self.p("trades.jsonl"), [dict(t, entry_wib=ms_wib(t["entry_t"]), exit_wib=ms_wib(t["exit_t"]), exit_reason=t.get("exit_reason") or "ARROW") for t in trades])
        write_jsonl(self.p("equity.jsonl"), [dict(e, wib=ms_wib(e["t"])) for e in eqs])
        self.write_live()
    def _rsi_map(self):
        B = TFS[self.tf]; bo = now_ms() // B * B; out = {}
        for c in self.coins_all:
            rv = None
            li = self.liveind.get(c)
            if li and li.get("bar_t") == bo and li.get("rsi") is not None:
                rv = li["rsi"]
            else:
                d = self.D.get(c)
                if d and "rsi" in d["ind"]:
                    v = d["ind"]["rsi"][-1]
                    if math.isfinite(v): rv = float(v)
            if rv is not None and math.isfinite(rv):
                out[c] = round(float(rv), 2)
        return out
    def write_live(self):
        if not getattr(self, "state", None): return
        super().write_live()
        try:
            rsi_map = self._rsi_map()
            s = json.load(open(self.p("summary.json")))
            s.update(signal_timing=self.timing, signal_timing_text="Signals: " + TIMING_TEXT[self.timing], universe=self.universe, excluded=self.excluded,
                     coins_traded=self.coins_all, trade_mode_text=self.state["trade_mode_text"],
                     entry_filter_2pct=self.entry_filter_2pct, rsi_filter=self.rsi_filter, rsi_filter_text=self.rsi_filter_text,
                     rsi=rsi_map, sides=self.sides, sides_text="Long+Short" if self.sides == "ls" else "Long-only")
            write_json(self.p("summary.json"), s)
            L = json.load(open(self.p("live.json")))
            lc = L.get("coins") or {}
            for c, rv in rsi_map.items():
                if isinstance(lc.get(c), dict): lc[c]["rsi"] = rv
            L.update(signal_timing=self.timing, last_eval_ms=self.last_eval, last_eval_wib=ms_wib(self.last_eval) if self.last_eval else None,
                     entry_filter_2pct=self.entry_filter_2pct, rsi_filter=self.rsi_filter, rsi_filter_text=self.rsi_filter_text, sides=self.sides)
            write_json(self.p("live.json"), L)
        except Exception as e: log.warning(f"summary extras: {e}")
    def watch_async(self, n):
        if self.watch_busy: return
        self.watch_busy = True
        def run():
            try: self.update_watch(n)
            except Exception as e: self.event("error", f"watchlist: {e}")
            finally: self.watch_busy = False
        threading.Thread(target=run, daemon=True).start()

    def loop(self):
        import signal as sg
        open(self.p("live.json"), "a").close(); os.utime(self.p("live.json"))
        self.liveind = {}; self.live = {}
        self.check_pause()
        self.ensure_epoch(); self.fold(); self.check_slots(); self.check_universe(force=True)
        self.event("start", f"paper bot started (pid {os.getpid()}), live pool engine · arrow_long · {'Long+Short' if self.allow_short else 'Long-only'} · entry filter {'ON' if self.entry_filter_2pct else 'OFF'} · {self.rsi_filter_text} · Signals: {TIMING_TEXT[self.timing]} · {self.tf} · max {self.max_pos} positions, TP ${self.tp_usd:g} / SL ${self.sl_usd:g} · "
                   f"universe {len(self.universe)} coins ({', '.join(self.universe)})" + (f" · excluded (top {self.excl_n} mcap): {', '.join(self.excluded)}" if self.excluded else f" · none in the top {self.excl_n} mcap") +
                   f" · held {', '.join(sorted(self.P)) or 'none'} · cash ${self.cash:.2f}")
        stop = {"v": False}
        def term(*_): stop["v"] = True
        sg.signal(sg.SIGTERM, term); sg.signal(sg.SIGINT, term)
        n = now_ms(); self.refresh(self.coins_all, n)
        # catch-up from where the previous process left off (bar-based), but only bars of the live epoch
        if self.last_eval: self.cover_from = self.last_eval
        self.process_closed(n); self.cover_from = now_ms()
        self.tick(); self.write_all(now_ms()); self.watch_async(n)
        last_tick = now_ms(); last_hb = 0; last_write = now_ms(); last_uni = now_ms()
        while not stop["v"]:
            n = now_ms(); B = TFS[self.tf]
            try:
                if self.check_pause(): self.write_live()
                self.check_commands()
                if n >= self.last_ct + 3000:           # a candle closed: klines for all coins, bar-close rules, full write
                    self.refresh(self.coins_all, n); self.process_closed(n); self.bars_done += 1
                    self.tick(); self.write_all(now_ms()); last_write = now_ms(); last_tick = now_ms()
                    lb = max((int(d["df"].t.values[-1]) for d in self.D.values()), default=0)
                    self.event("bar", f"processed {self.tf} bar {ms_wib(lb)} WIB · balance ${self.equity_now(self.live):.2f} · signals {self.timing}", key=f"b|{self.tf}|{lb}", bar_t=lb, tf=self.tf)
                    self.watch_async(n)
                elif n - last_tick >= 5000:
                    last_tick = n
                    nf = sum(len(v) for v in self.fills1.values()); self.tick()
                    if sum(len(v) for v in self.fills1.values()) != nf or n - last_write >= 60_000: self.write_all(now_ms()); last_write = now_ms()
                    else: self.write_live()
                    self.maybe_publish_live()
                if n - last_uni >= 86_400_000: last_uni = n; self.check_universe(force=True)
            except Exception as e:
                self.event("error", f"loop: {e}"); log.error(traceback.format_exc()); time.sleep(5)
            if n - last_hb >= 30_000:
                last_hb = n
                if (n // 60_000) % 15 == 0 and getattr(self, "_hb_min", None) != n // 60_000:
                    self._hb_min = n // 60_000; self.event("heartbeat", f"alive, uptime {(n - self.boot) / 3.6e6:.2f} h, balance ${self.marked()[0]:.2f}")
            time.sleep(0.1)
        self.save_eval(); self.event("stop", "paper bot stopped (SIGTERM)")
