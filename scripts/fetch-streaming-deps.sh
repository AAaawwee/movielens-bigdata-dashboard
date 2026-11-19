#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
dest="$root/pipeline/streaming/lib"
mkdir -p "$dest"
for artifact in \
  org/apache/spark/spark-sql-kafka-0-10_2.12/3.1.2/spark-sql-kafka-0-10_2.12-3.1.2.jar \
  org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.1.2/spark-token-provider-kafka-0-10_2.12-3.1.2.jar \
  org/apache/kafka/kafka-clients/2.6.0/kafka-clients-2.6.0.jar \
  org/apache/commons/commons-pool2/2.6.2/commons-pool2-2.6.2.jar; do
  file="$dest/${artifact##*/}"
  curl -fsSL --retry 2 "https://repo.maven.apache.org/maven2/$artifact" -o "$file.download"
  mv "$file.download" "$file"
done
