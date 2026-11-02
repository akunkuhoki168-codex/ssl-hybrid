#!/usr/bin/env python3
"""Build the GitHub Pages index.html = index.base.html (simulation) + the "Real Bot" tab.
Two tabs only: "Real Bot" (id=tabBot, key=bot, default) and "Simulation" (id=tabPap, key=pap); #pap opens Simulation,
any other hash (or none) opens Real Bot. The legacy Signals / Coin detail views (vSig/vDet) stay in the DOM, hidden,
only because the shared JS still references their elements.
Stage 2 applies web/pages_live.patch: the edits made directly on GitHub (28 Oct: controls moved into Simulation, Re-run
button removed, chart sizing) plus the Real Bot 15m/1h/4h control (highlights bot.json "tf"; a tap opens
control/tf.json in GitHub's editor, the box's tf_poller.py applies it). Regenerate the patch after editing the live page:
  python3 build_index.py --raw /tmp/raw.html && diff -u /tmp/raw.html ../ghpages-repo/index.html > pages_live.patch
Usage: python3 build_index.py [--raw] <out.html>"""
import os, sys, subprocess, shutil
RAW = "--raw" in sys.argv; sys.argv = [a for a in sys.argv if a != "--raw"]
H = os.path.dirname(os.path.abspath(__file__)); rd = lambda n: open(os.path.join(H, n)).read()
s = rd("index.base.html")
def rep(a, b, n=1):
    global s
    assert s.count(a) == n, (a, s.count(a)); s = s.replace(a, b)
rep("</style>", rd("bot_css.txt") + "</style>")
rep('  <div class="tabs"><button id="tabSig" class="on">Signals</button><button id="tabDet">Coin detail</button><button id="tabPap">Paper (arrows)</button></div>',
    '  <div class="tabs">\n    <button id="tabBot" class="on">Real Bot</button>\n    <button id="tabPap">Simulation</button>\n  </div>')
rep('  <section id="vSig">', rd("bot_section.txt") + '  <section id="vSig" class="hide" hidden aria-hidden="true"><!-- legacy Signals view: hidden, kept only so the shared JS finds its elements -->')
rep('  <section id="vDet" class="hide">', '  <section id="vDet" class="hide" hidden aria-hidden="true"><!-- legacy Coin detail view: hidden, kept only so the shared JS finds its elements -->')
rep('<script>\nif (typeof window !== "undefined") (function () {', rd("bot_js.txt") + '\n<script>\nif (typeof window !== "undefined") (function () {')
rep('const SETKEYS = ["tf", "dir", "ex", "cap", "coin"];', 'S.tab = ({ "#pap": "pap" })[location.hash] || "bot";   // the Real bot tab is the default on every load\nconst SETKEYS = ["tf", "dir", "ex", "cap", "coin"];')
rep('function setTab(t, load = true) {\n  S.tab = t; save();', 'function setTab(t, load = true) {\n  t = t === "pap" ? "pap" : "bot";   // only two tabs: Simulation (pap) and Real Bot (everything else)\n  S.tab = t; save();')
rep('[["sig", "tabSig", "vSig"], ["det", "tabDet", "vDet"], ["pap", "tabPap", "vPap"]]', '[["bot", "tabBot", "vBot"], ["pap", "tabPap", "vPap"]]')
rep('  document.body.classList.toggle("onPaper", t === "pap");', '  document.body.classList.toggle("onPaper", t === "pap"); document.body.classList.toggle("onBot", t === "bot");')
rep('$("tabSig").onclick = () => setTab("sig"); $("tabDet").onclick = () => setTab("det"); $("tabPap").onclick = () => setTab("pap");', '$("tabBot").onclick = () => setTab("bot");   $("tabPap").onclick = () => setTab("pap");')
rep('$("refresh").onclick = () => refreshAll();', '$("refresh").onclick = () => S.tab === "bot" ? RB.load() : refreshAll();')
rep('function isStale() { return S.tab === "pap"', 'function isStale() { return S.tab === "bot" ? false : S.tab === "pap"')
rep('async function refreshAll() {   // live refresh of what is shown, with the committed settings\n  if (running) return;', 'async function refreshAll() {   // live refresh of what is shown, with the committed settings\n  if (running || S.tab === "bot") return;   // the Real bot tab refreshes itself every 60 s')
rep('  if (running) return;\n  const k = runKey();', '  if (S.tab === "bot") { RB.show(); return; }\n  if (running) return;\n  const k = runKey();')
rep('runNow();   // first load', 'if (S.tab === "bot") RB.show(); else runNow();   // first load')
# ---- auto-run: the simulation tabs run by themselves (no Run press needed) ----
rep('<span id="runLab">▶ Run</span>', '<span id="runLab">↻ Re-run</span>')
rep('$("runLab").textContent = "▶ Run";', '$("runLab").textContent = "↻ Re-run";')
rep('Settings changed, press ▶ Run to update the results.', 'Settings changed, updating automatically…')
rep('aria-label="Run backtest"', 'aria-label="Re-run now (optional, it runs automatically)"')
rep('function markStale() { document.body.classList.toggle("isStale", isStale());', 'var autoT = null;\nfunction autoRun(ms) { clearTimeout(autoT); autoT = setTimeout(() => { if (S.tab !== "bot" && isStale()) runNow(); }, ms || 450); }   // any settings change re-runs by itself\nfunction markStale() { if (S.tab !== "bot" && isStale()) autoRun(); document.body.classList.toggle("isStale", isStale());')
rep('$("capIn").addEventListener("input", () => { const v = parseFloat($("capIn").value); if (isFinite(v) && v > 0 && v !== R.cap) document.body.classList.add("isStale"); });',
    '$("capIn").addEventListener("input", () => { const v = parseFloat($("capIn").value); if (isFinite(v) && v > 0 && v !== R.cap) { document.body.classList.add("isStale"); S.cap = v; save(); autoRun(900); } });')
rep('selectCoin(S.coin); setTab(S.tab, false); markStale();', 'S.auto = true; save();   // auto-refresh is always on at page load\nselectCoin(S.coin); setTab(S.tab, false); markStale();')
open(sys.argv[1], "w").write(s)
pt = os.path.join(H, "pages_live.patch")
if not RAW and os.path.exists(pt) and os.path.getsize(pt):
    r = subprocess.run(["patch", "-s", "--no-backup-if-mismatch", "-r", "-", sys.argv[1], pt], capture_output=True, text=True)
    if r.returncode: sys.exit("pages_live.patch did not apply cleanly:\n" + r.stdout + r.stderr)
    s = open(sys.argv[1]).read()
for bad in ('id="tabSig"', 'id="tabDet"', '"tabSig"', '"tabDet"'): assert bad not in s, "legacy tab left: " + bad
print("wrote", sys.argv[1], len(s), "(raw)" if RAW else "")
