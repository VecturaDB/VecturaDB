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
up(){ (cd / && /vecturadb run /cfg.yaml >>/tmp/srv.log 2>&1 &); for i in $(seq 60); do curl -s 127.0.0.1:8000/api/v2/heartbeat >/dev/null && return 0; sleep 1; done; pkill -9 -f "/vecturadb run"; return 1; }
start(){ up && return; echo "server not up"; tail -20 /tmp/srv.log; exit 1; }
# LazyFS can read back fewer bytes than the size it gives for a file not synced (1 start in about 25 after a copy, at
# d93e54a, on chroma.sqlite3-wal): a real disk cannot. Only then the copy, kept outside LazyFS, is made again.
short_read(){ (cd /data && find . -type f) | while read -r f; do [ "$(cat "/data/$f" | wc -c)" -lt "$(stat -c %s "/data/$f")" ] && echo "$f"; done; }
start_after_copy(){
  for try in 1 2 3; do
    up && return
    s=$(short_read); [ -n "$s" ] || break
    echo "round $r: LazyFS read back fewer bytes than the size of $s; the copy made again"
    find /data -mindepth 1 -maxdepth 1 -exec rm -rf {} +; cp -a /tmp/bk/. /data/
  done
  echo "server not up"; tail -20 /tmp/srv.log; exit 1
}
mount_lz
for r in $(seq "$1"); do
  if [ "$r" = 1 ]; then start; else start_after_copy; fi
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
