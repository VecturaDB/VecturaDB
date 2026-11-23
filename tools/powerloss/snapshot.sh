#!/bin/bash
# $1 rounds. A snapshot of the disk while the server writes, as the backup of a volume: under
# LazyFS what was not synced stays in its cache, so the folder below it is what a snapshot takes. Each round: batches of
# 100 records across the saves, at every 1000 records (sync_threshold), after the batches 10 and 20; after the batch $k,
# with the writes going on, that folder copied; the copy started by a second server on a plain folder, and every record
# acknowledged before the copy read back with its embedding.
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

echo 'persist_path: "/data"' > /cfg.yaml; echo 'persist_path: "/snap"' > /cfg2.yaml
up(){ (cd / && /vecturadb run $1 >>/tmp/srv.log 2>&1 &); for i in $(seq 60); do curl -s 127.0.0.1:8000/api/v2/heartbeat >/dev/null && return 0; sleep 1; done; echo "server not up"; return 1; }
fail=0
for r in $(seq "$1"); do
  rm -rf /real /snap; mkdir -p /real /data; : > /tmp/srv.log
  /lazyfs/lazyfs/build/lazyfs /data --config-path /lz.toml -o allow_other -o modules=subdir -o subdir=/real >/tmp/lz.log 2>&1 & sleep 2
  up /cfg.yaml || exit 1
  k=$((8 + RANDOM % 15))
  python3 /client_save.py write 25 > /tmp/acks & w=$!
  until [ "$(tail -1 /tmp/acks 2>/dev/null || echo 0)" -ge "$k" ] 2>/dev/null; do sleep 0.05; done
  n=$(tail -1 /tmp/acks); cp -a /real /snap
  kill $w 2>/dev/null; wait $w 2>/dev/null; pkill -9 -f "vecturadb run"; sleep 1; fusermount3 -u /data; sleep 1
  up /cfg2.yaml || { echo "round $r k=$k n=$n: FAIL server"; fail=$((fail+1)); continue; }
  out=$(python3 /client_save.py read "$n" 2>&1) && echo "round $r k=$k n=$n: ok" || { echo "round $r k=$k n=$n: FAIL $(echo "$out" | head -2 | tr '\n' ' ')"; fail=$((fail+1)); }
  pkill -9 -f "vecturadb run"; sleep 1
done
echo "failed $fail of $1"; [ "$fail" = 0 ] || { tail -40 /tmp/srv.log; exit 1; }
