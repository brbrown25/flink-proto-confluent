#!/usr/bin/env bash
# Re-records the casts in ../content/assets/casts from a clean stack. Needs: docker, asciinema.
set -euo pipefail
cd "$(dirname "$0")"
OUT=../content/assets/casts
mkdir -p "$OUT"
./setup.sh >/dev/null
docker compose down -v >/dev/null 2>&1
docker compose up -d >/dev/null 2>&1
echo "waiting for Schema Registry..."
until docker compose exec -T schema-registry curl -sf localhost:8081/subjects >/dev/null 2>&1; do sleep 2; done
until docker compose exec -T jobmanager curl -sf localhost:8081/taskmanagers 2>/dev/null | grep -q '"id"'; do sleep 2; done
for name in quickstart write-back dead-letter; do
  asciinema rec --overwrite --quiet --cols 110 --rows 32 --idle-time-limit 2 \
    --command "./casts/$name.sh" "$OUT/$name.cast"
done
echo "recorded:"; ls -l "$OUT"
