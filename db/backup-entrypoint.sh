#!/bin/sh
# pg_dump loop: full custom-format dump every BACKUP_INTERVAL_HOURS,
# keeping the BACKUP_KEEP newest files in /backups.
set -u
DUMP_DIR=/backups
INTERVAL_HOURS=${BACKUP_INTERVAL_HOURS:-6}
KEEP=${BACKUP_KEEP:-28}
PGHOST=${PGHOST:-postgres}
PGUSER=${PGUSER:-crawler}
PGDATABASE=${PGDATABASE:-corpus}

mkdir -p "$DUMP_DIR"
echo "[backup] loop started: every ${INTERVAL_HOURS}h, keep ${KEEP}, target ${PGUSER}@${PGHOST}/${PGDATABASE}"
while true; do
  ts=$(date +%Y%m%d_%H%M%S)
  out="$DUMP_DIR/corpus_${ts}.dump"
  if pg_dump -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -Fc -f "$out"; then
    echo "[backup] ok: $out ($(du -h "$out" | cut -f1))"
  else
    echo "[backup] FAILED: pg_dump exited nonzero, removing partial file"
    rm -f "$out"
  fi
  ls -1t "$DUMP_DIR"/corpus_*.dump 2>/dev/null | tail -n +$((KEEP + 1)) | while read -r f; do
    echo "[backup] pruning old: $f"
    rm -f "$f"
  done
  sleep $((INTERVAL_HOURS * 3600))
done
