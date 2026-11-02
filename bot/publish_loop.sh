#!/usr/bin/env bash
# Publisher: every 10 min (at :02, :12, :22 ... WIB, just after the bot processes a bar) export bot.json into the
# GitHub Pages repo and, if it changed, commit + push. Max 6 pushes per rolling hour (Pages soft limit = 10 builds/h).
# Push failures: retry with backoff (30/60/120/240 s), rebase on a non-fast-forward; never force-push.
# Kept alive by supervisor.sh (run/publisher.pid); stopped by stop.sh --all. Log: publish.log
cd "$(dirname "$0")"; D=$PWD; R=$D/run; REPO=$D/ghpages-repo; mkdir -p "$R"
LOG=$D/publish.log; PUSHES=$R/publish_pushes.txt; MAXH=${PUBLISH_MAX_PER_HOUR:-6}; EVERY=${PUBLISH_EVERY_S:-600}; OFFSET=${PUBLISH_OFFSET_S:-120}
log() { echo "$(TZ=Asia/Jakarta date '+%F %T') WIB [publish] $*" >> "$LOG"; }
exec 9> "$R/publisher.lock"
flock -n 9 || { log "another publisher holds the lock, exiting (pid $$)"; exit 0; }
echo $$ > "$R/publisher.pid"
trap 'log "publisher stopping (pid $$)"; kill $! 2>/dev/null; rm -f "$R/publisher.pid"; exit 0' TERM INT
# keep the log bounded (~2 MB)
trim_log() { [ -f "$LOG" ] && [ "$(stat -c %s "$LOG")" -gt 2000000 ] && { tail -n 5000 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"; }; }
pushes_last_hour() { local now; now=$(date +%s); [ -f "$PUSHES" ] || { echo 0; return; }
  awk -v n="$now" '$1 > n-3600' "$PUSHES" > "$PUSHES.tmp"; mv "$PUSHES.tmp" "$PUSHES"; wc -l < "$PUSHES"; }
push_with_retry() {
  local delay=30 i out
  for i in 1 2 3 4 5; do
    if out=$(git -C "$REPO" push -q origin HEAD:main 2>&1); then date +%s >> "$PUSHES"; log "pushed $(git -C "$REPO" rev-parse --short HEAD) (attempt $i)"; return 0; fi
    log "push attempt $i failed: $(echo "$out" | tr '\n' ' ' | cut -c1-300)"
    if echo "$out" | grep -qiE 'non-fast-forward|fetch first|rejected'; then
      git -C "$REPO" pull -q --rebase -X theirs origin main >> "$LOG" 2>&1 || { git -C "$REPO" rebase --abort >> "$LOG" 2>&1; log "rebase failed"; }
    fi
    [ $i -lt 5 ] && { sleep $delay 9>&- & wait $!; delay=$((delay * 2)); }
  done
  log "push failed after 5 attempts; the commit stays local and goes out with the next cycle"; return 1
}
cycle() {
  # stay in sync with origin first (user edits control/tf.json / index.html on GitHub); only bot.json is ever committed here,
  # so a rebase never touches control/tf.json; on any trouble abort and carry on (the push path rebases again)
  git -C "$REPO" pull -q --rebase --autostash origin main >> "$LOG" 2>&1 || { git -C "$REPO" rebase --abort >> "$LOG" 2>&1; log "pre-cycle pull failed (continuing)"; }
  local o; o=$(python3 "$D/export_public.py" "$REPO/bot.json" 2>&1) || { log "export failed: $(echo "$o" | tail -n 3 | tr '\n' ' ')"; return; }
  git -C "$REPO" add bot.json
  local ahead; ahead=$(git -C "$REPO" rev-list --count origin/main..HEAD 2>/dev/null || echo 0)
  if git -C "$REPO" diff --cached --quiet -- bot.json && [ "$ahead" = 0 ]; then log "bot.json unchanged, nothing to push"; return; fi
  local n; n=$(pushes_last_hour)
  if [ "$n" -ge "$MAXH" ]; then log "rate limit: $n pushes in the last hour (max $MAXH), skipping this cycle"; return; fi
  if ! git -C "$REPO" diff --cached --quiet -- bot.json; then
    git -C "$REPO" commit -q -m "bot.json $(TZ=Asia/Jakarta date '+%F %H:%M') WIB" -- bot.json >> "$LOG" 2>&1 || { log "commit failed"; return; }
  fi
  push_with_retry
}
log "publisher up pid $$ (every ${EVERY}s, offset ${OFFSET}s, max $MAXH pushes/h) -> $(git -C "$REPO" remote get-url origin)"
[ "${PUBLISH_NOW:-1}" = 1 ] && cycle
while true; do
  now=$(date +%s); next=$(( (now - OFFSET) / EVERY * EVERY + EVERY + OFFSET )); [ $next -le $now ] && next=$((next + EVERY))
  sleep $((next - now)) 9>&- & wait $!
  touch "$R/publisher.hb"; trim_log; cycle
done
