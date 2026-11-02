#!/usr/bin/env bash
# Cron wrapper (every 5 min + @reboot): makes sure the watchdog (bot + dashboard + publisher + tf poller) is up.
# Respects an intentional stop: if run/STOPPED_BY_USER exists it does nothing (start.sh would delete that file).
# Usage: cron_ensure.sh [--boot]   (--boot only adds a log tag; the STOPPED_BY_USER check still applies)
cd "$(dirname "$0")"; D=$PWD; R=$D/run; mkdir -p "$R"
export PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin HOME=${HOME:-/home/box} TZ=Asia/Jakarta
log() { echo "$(date '+%F %T') WIB [cron_ensure${BOOT}] $*" >> "$D/cron_ensure.log"; }
BOOT=""; [ "$1" = "--boot" ] && BOOT=" @reboot"
touch "$R/cron_ensure.hb"   # proof that cron runs this (mtime = last run)
[ -f "$R/STOPPED_BY_USER" ] && { [ -n "$BOOT" ] && log "STOPPED_BY_USER present, not starting"; exit 0; }
if [ -f "$R/supervisor.pid" ] && kill -0 "$(cat "$R/supervisor.pid")" 2>/dev/null; then [ -n "$BOOT" ] && log "watchdog already up ($(cat "$R/supervisor.pid"))"; exit 0; fi
log "watchdog not running -> start.sh: $(bash "$D/start.sh" 2>&1 | tr '\n' ' ')"
