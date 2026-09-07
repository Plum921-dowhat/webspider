#!/bin/sh
# pg_dump loop:
#   - every BACKUP_INTERVAL_HOURS: articles-only dump (vectors excluded — they
#     are regenerable from text + the embedding API and would bloat backups)
#   - once every 7 days: full dump including the vector table (keep FULL_KEEP)
set -u
DUMP_DIR=/backups
INTERVAL_HOURS=${BACKUP_INTERVAL_HOURS:-6}
KEEP=${BACKUP_KEEP:-28}
FULL_KEEP=${BACKUP_FULL_KEEP:-4}
FULL_EVERY_DAYS=${BACKUP_FULL_EVERY_DAYS:-7}
PGHOST=${PGHOST:-postgres}
PGUSER=${PGUSER:-crawler}
PGDATABASE=${PGDATABASE:-corpus}
PG_DUMPCMD="pg_dump -h $PGHOST -U $PGUSER -d $PGDATABASE -Fc"
# Vector table data is excluded from the frequent dumps: it is regenerable
# from text + the embedding API and would bloat every backup file.
HOURLY_EXCLUDE="--exclude-table-data=article_chunks"
FULL_MARKER=$DUMP_DIR/.last_full

mkdir -p "$DUMP_DIR"
echo "[backup] loop started: every ${INTERVAL_HOURS}h (no vectors), full every ${FULL_EVERY_DAYS}d, target ${PGUSER}@${PGHOST}/${PGDATABASE}"

prune() {
  prefix=$1
  keep=$2
  ls -1t "$DUMP_DIR"/${prefix}_*.dump 2>/dev/null | tail -n +$((keep + 1)) | while read -r f; do
    echo "[backup] pruning old: $f"
    rm -f "$f"
  done
}

dump() {
  prefix=$1
  extra=$2
  ts=$(date +%Y%m%d_%H%M%S)
  out="$DUMP_DIR/${prefix}_${ts}.dump"
  if $PG_DUMPCMD $extra -f "$out"; then
    echo "[backup] ok: $out ($(du -h "$out" | cut -f1))"
  else
    echo "[backup] FAILED: pg_dump exited nonzero, removing partial file"
    rm -f "$out"
  fi
}

while true; do
  dump corpus "$HOURLY_EXCLUDE"
  prune corpus "$KEEP"

  now=$(date +%s)
  last=$(cat "$FULL_MARKER" 2>/dev/null || echo 0)
  if [ $((now - last)) -ge $((FULL_EVERY_DAYS * 86400)) ]; then
    dump corpus_full ""
    prune corpus_full "$FULL_KEEP"
    date +%s > "$FULL_MARKER"
  fi

  sleep $((INTERVAL_HOURS * 3600))
done
