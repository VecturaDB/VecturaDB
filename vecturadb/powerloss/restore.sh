#!/bin/bash
# $1 rounds. Each round: a collection of 250 records, a power cut (kill -9, then LazyFS drops what was not synced), the
# server started again, every collection read back; then a restore as a template does it: server stopped, data folder
# emptied and copied back with cp -a, with no sync. Fails at the first round that does not find every collection.
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
mkdir -p /real /data
mount_lz(){ /lazyfs/lazyfs/build/lazyfs /data --config-path /lz.toml -o allow_other -o modules=subdir -o subdir=/real >/tmp/lz.log 2>&1 & sleep 2; }
start(){ (cd / && /vecturadb run /cfg.yaml >>/tmp/srv.log 2>&1 &); for i in $(seq 60); do curl -s 127.0.0.1:8000/api/v2/heartbeat >/dev/null && return; sleep 1; done; echo "server not up"; tail -20 /tmp/srv.log; exit 1; }
mount_lz
for r in $(seq "$1"); do
  start
  python3 /client.py write "rep-$r" || exit 1
  pkill -9 -f "/vecturadb run"; sleep 1
  echo "lazyfs::clear-cache" > /tmp/faults.fifo; sleep 1
  fusermount3 -u /data; sleep 1; mount_lz
  start
  python3 /client.py read "$r" || { tail -20 /tmp/srv.log; exit 1; }
  pkill -f "/vecturadb run"; sleep 2
  rm -rf /tmp/bk; cp -a /data /tmp/bk
  find /data -mindepth 1 -maxdepth 1 -exec rm -rf {} +; cp -a /tmp/bk/. /data/
done
