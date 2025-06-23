set dotenv-load

image arch='amd64':
    docker buildx build --tag=hardcoreeng/hulygram:latest --platform=linux/{{arch}} .

bytehound:
    cargo build
    LD_PRELOAD=~/.local/lib/libbytehound.so target/debug/hulygram

bytehound_server:
    bytehound server -p 8888 memory-profiling_hulygram*.dat

pprof:
    ~/go/bin/pprof -no_browser -http 127.0.0.1:8888 target/debug/hulygram heap.pb.gz


redis-cli:
    redis-cli -n 10 -h 127.0.0.1

redis-flush:
    python3 _hidden/redis_flush.py   