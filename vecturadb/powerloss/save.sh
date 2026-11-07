#!/bin/bash
# $1 rounds. Each round on an empty folder: batches of 100 records in one collection, across
# the saves at 1000 and 2000 records; after the batch $k a power cut (kill -9, LazyFS drops what was not synced) at a
# delay 0-400 ms, then every acknowledged record read back with its embedding.
set -u
cat > /lz.toml <<T
[faults]
fifo_path="/tmp/faults.fifo"
[cache]
apply_eviction=false
[cache.simple]
custom_size="1gb"
blocks_per_page=1
[filesystem]
log_all_operations=false
logfile=""
T
echo "allow_other" >> /etc/fuse.conf
echo 'persist_path: "/data"' > /cfg.yaml
mount_lz(){ /lazyfs/lazyfs/build/lazyfs /data --config-path /lz.toml -o allow_other -o modules=subdir -o subdir=/real/$r >/tmp/lz.log 2>&1 & sleep 2; }
up(){ (cd / && /vecturadb run /cfg.yaml >>/tmp/srv.log 2>&1 &); for i in $(seq 60); do curl -s 127.0.0.1:8000/api/v2/heartbeat >/dev/null && return 0; sleep 1; done; echo "server not up"; tail -20 /tmp/srv.log; return 1; }

short_read(){ (cd /data && find . -type f) | while read -r f; do [ "$(cat "/data/$f" | wc -c)" -lt "$(stat -c %s "/data/$f")" ] && echo "$f"; done; }
fail=0
for r in $(seq "$1"); do
  mkdir -p /real/$r /data; mount_lz; : > /tmp/srv.log
  k=$((8 + RANDOM % 18)); d=$((RANDOM % 400))
  up || { echo "round $r: start on an empty folder failed"; pkill -9 -f "vecturadb run"; sleep 1; fusermount3 -u /data; sleep 1; fail=$((fail+1)); continue; }
  python3 /client_save.py write "$k" > /tmp/acks || { echo "round $r: write failed"; }
  sleep "0.$(printf %03d $d)"
  pkill -9 -f "vecturadb run"; sleep 1
  echo "lazyfs::clear-cache" > /tmp/faults.fifo; sleep 1
  fusermount3 -u /data; sleep 1; mount_lz
  up || { echo "round $r k=$k d=$d: FAIL server; short read: $(short_read | tr '\n' ' ')"; fail=$((fail+1)); }
  out=$(python3 /client_save.py read "$(tail -1 /tmp/acks)" 2>&1) && echo "round $r k=$k d=$d: ok" || { echo "round $r k=$k d=$d: FAIL $(echo "$out" | head -3 | tr '\n' ' ')"; echo "round $r short read: $(short_read | tr '\n' ' ')"; fail=$((fail+1)); }
  pkill -9 -f "vecturadb run"; sleep 1; fusermount3 -u /data; sleep 1
done
echo "failed $fail of $1"; [ "$fail" = 0 ] || { tail -40 /tmp/srv.log; exit 1; }
