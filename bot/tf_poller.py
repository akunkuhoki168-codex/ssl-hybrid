#!/usr/bin/env python3
"""Timeframe poller: every ~60 s reads control/tf.json from the GitHub Pages repo (dedkovic441-hub/ssl-hybrid-dashboard, main)
via `gh api .../contents/...` (no CDN cache; falls back to raw.githubusercontent.com with a cache-buster) and, when
  tf in {15m,1h,4h}  AND  tf != engine's active tf (summary.json active_tf)  AND  requested_at != last applied requested_at,
writes run/tf_request.json {"tf":..,"at_ms":now} (atomically) which the engine picks up live (Bot.check_tf in its main loop).
State: run/tf_poller_state.json {"applied_requested_at":..,...}. On first start the current requested_at is recorded as applied
(a stale value never triggers a switch). Heartbeat: run/tf_poller.hb, pid: run/tf_poller.pid, log: tf_poller.log.
Kept alive by supervisor.sh. Paper only: this never touches orders, positions or the balance.
Usage: tf_poller.py [--dir D] [--once] [--dry-run] [--file local_tf.json]   (--file/--dry-run are for tests)"""
import argparse, base64, json, os, subprocess, sys, time, urllib.request
from datetime import datetime, timezone, timedelta
REPO = "dedkovic441-hub/ssl-hybrid-dashboard"; PATH = "control/tf.json"
RAW = f"https://raw.githubusercontent.com/{REPO}/main/{PATH}"
VALID = ("15m", "1h", "4h"); WIB = timezone(timedelta(hours=7))

def now_wib(): return datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S")
def wjson(p, o):
    t = p + ".tmp"
    with open(t, "w") as f: json.dump(o, f)
    os.replace(t, p)
def rjson(p, d=None):
    try:
        with open(p) as f: return json.load(f)
    except Exception: return d

def fetch(local=None):
    if local: return rjson(local), "file"
    try:
        out = subprocess.run(["gh", "api", f"repos/{REPO}/contents/{PATH}?ref=main", "--jq", ".content"], capture_output=True, text=True, timeout=30)
        if out.returncode == 0 and out.stdout.strip():
            return json.loads(base64.b64decode(out.stdout.strip())), "gh"
        err = (out.stderr or "").strip()[:200]
    except Exception as e: err = str(e)[:200]
    try:
        with urllib.request.urlopen(f"{RAW}?cb={int(time.time())}", timeout=20) as r: return json.loads(r.read()), f"raw (gh failed: {err})"
    except Exception as e: raise RuntimeError(f"gh: {err}; raw: {e}")

class Poller:
    def __init__(self, d, dry=False, local=None):
        self.d = d; self.dry = dry; self.local = local
        self.state_p = os.path.join(d, "run", "tf_poller_state.json"); self.req_p = os.path.join(d, "run", "tf_request.json")
        self.log_p = os.path.join(d, "tf_poller.log"); os.makedirs(os.path.join(d, "run"), exist_ok=True)
    def log(self, m):
        line = f"{now_wib()} WIB [tf_poller]{' [dry-run]' if self.dry else ''} {m}"
        print(line, flush=True)
        try:
            if os.path.exists(self.log_p) and os.path.getsize(self.log_p) > 1_000_000:
                ls = open(self.log_p).readlines()[-2000:]; open(self.log_p, "w").writelines(ls)
            with open(self.log_p, "a") as f: f.write(line + "\n")
        except Exception: pass
    def active_tf(self):
        s = rjson(os.path.join(self.d, "summary.json"), {}) or {}
        return s.get("active_tf")
    def step(self):
        """returns one of: 'init', 'noop', 'applied', 'would-apply', 'invalid', 'error'"""
        st = rjson(self.state_p, None)
        try: ctl, src = fetch(self.local)
        except Exception as e: self.log(f"fetch failed: {e}"); return "error"
        if not isinstance(ctl, dict): self.log(f"control/tf.json is not an object: {ctl!r}"[:300]); return "invalid"
        tf, ra = ctl.get("tf"), ctl.get("requested_at")
        ra = None if ra is None else str(ra)
        cur = self.active_tf()
        base = {"last_seen_tf": tf, "last_seen_requested_at": ra, "last_check_wib": now_wib(), "source": src.split(" ")[0], "engine_tf": cur}
        if st is None:   # first start: whatever is there now counts as already applied
            st = dict(base, applied_requested_at=ra, applied_tf=tf, applied_wib=None, initialized_wib=now_wib())
            wjson(self.state_p, st); self.log(f"first start: recorded requested_at={ra} (tf={tf}) as applied; engine tf={cur}; no switch"); return "init"
        st.update(base)
        res = "noop"
        if tf not in VALID:
            if st.get("last_invalid") != [tf, ra]: self.log(f"ignoring invalid tf {tf!r} (requested_at={ra}); only {'/'.join(VALID)} are accepted")
            st["last_invalid"] = [tf, ra]; res = "invalid"
        elif cur not in VALID:
            self.log(f"engine active_tf unknown ({cur!r}), not switching"); res = "error"
        elif tf != cur and ra != st.get("applied_requested_at"):
            req = {"tf": tf, "at_ms": int(time.time() * 1000)}
            if self.dry:
                self.log(f"WOULD write {self.req_p}: {json.dumps(req)} (control tf={tf} requested_at={ra}, engine tf={cur})"); res = "would-apply"
            else:
                wjson(self.req_p, req)
                st.update(applied_requested_at=ra, applied_tf=tf, applied_wib=now_wib(), applied_at_ms=req["at_ms"])
                self.log(f"switch requested {cur} -> {tf} (requested_at={ra}): wrote run/tf_request.json {json.dumps(req)}"); res = "applied"
        if not self.dry or res != "would-apply": wjson(self.state_p, st)
        return res

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=os.path.dirname(os.path.abspath(__file__)))
    ap.add_argument("--once", action="store_true"); ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--file", default=None, help="read the control JSON from a local file instead of GitHub (tests)")
    ap.add_argument("--every", type=float, default=float(os.environ.get("TF_POLL_EVERY_S", 60)))
    a = ap.parse_args()
    p = Poller(a.dir, a.dry_run, a.file)
    if a.once: print(p.step()); return
    run = os.path.join(a.dir, "run")
    with open(os.path.join(run, "tf_poller.pid"), "w") as f: f.write(str(os.getpid()))
    import signal
    stop = {"v": False}
    def term(*_): stop["v"] = True
    signal.signal(signal.SIGTERM, term); signal.signal(signal.SIGINT, term)
    p.log(f"poller up pid {os.getpid()} (every {a.every:g}s) <- {REPO}/{PATH}")
    last_res = None
    while not stop["v"]:
        try:
            r = p.step()
            if r == "noop" and last_res != "noop": st = rjson(p.state_p, {}) or {}; p.log(f"watching: control tf={st.get('last_seen_tf')} requested_at={st.get('last_seen_requested_at')}, engine tf={p.active_tf()}, nothing to do")
            last_res = r
        except Exception as e: p.log(f"step error: {e}")
        open(os.path.join(run, "tf_poller.hb"), "a").close(); os.utime(os.path.join(run, "tf_poller.hb"))
        t = time.time() + a.every
        while not stop["v"] and time.time() < t: time.sleep(1)
    p.log("poller stopping")
    try: os.remove(os.path.join(run, "tf_poller.pid"))
    except Exception: pass

if __name__ == "__main__": main()
