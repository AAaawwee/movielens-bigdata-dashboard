#!/usr/bin/env bash
set -euo pipefail

mode="${1:---run}"
if [[ $# -gt 1 || ( "$mode" != --check && "$mode" != --run ) ]]; then
  printf 'Usage: %s [--check|--run]\n' "$0" >&2
  exit 2
fi

root="$(cd "$(dirname "$0")/.." && pwd)"
export HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop-3.2.0}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
export SPARK_HOME="${SPARK_HOME:-$HOME/spark-3.1.2-bin-hadoop3.2}"
export HIVE_HOME="${HIVE_HOME:-$HOME/software/apache-hive-3.1.2-bin}"
export MOVIELENS_HDFS_BASE="${MOVIELENS_HDFS_BASE:-hdfs://namenode:9000/movie}"
base="$MOVIELENS_HDFS_BASE"
hdfs="$HADOOP_HOME/bin/hdfs"
stream="$root/pipeline/streaming/run-stage5.sh"

# Read each input status first. An unreachable NameNode must never be mistaken for a fresh cluster.
"$hdfs" dfs -ls "$base/raw" "$base/clean"
for file in movies ratings users; do
  if ! "$hdfs" dfs -test -s "$base/raw/$file.dat" ||
     ! "$hdfs" dfs -test -e "$base/clean/$file/_SUCCESS"; then
    printf 'Missing raw file or completed clean output for %s. Use the matching three-disk set.\n' "$file" >&2
    exit 1
  fi
done
printf 'RAW_AND_CLEAN_OK %s\n' "$base"
if [[ "$mode" == --check ]]; then
  exit 0
fi

"$hdfs" dfs -mkdir -p "$base/result"
"$HIVE_HOME/bin/hive" --hivevar movie_root="$base" -f "$root/pipeline/sql/tables.sql"
bash "$root/pipeline/offline/run-stage3-offline.sh"

run_als() {
  MOVIELENS_ALS_ACTION="$1" "$SPARK_HOME/bin/spark-shell" --master yarn --deploy-mode client \
    --driver-memory 1g --num-executors 1 --executor-cores 1 --executor-memory 1g \
    --conf spark.task.maxDirectResultSize=16m --conf spark.sql.shuffle.partitions=8 \
    --conf spark.sql.catalogImplementation=in-memory -i "$root/pipeline/als/train_als.scala" </dev/null
}

if ! "$hdfs" dfs -test -e "$base/model/als/_training_metrics/_SUCCESS"; then
  run_als train
fi
if ! "$hdfs" dfs -test -e "$base/result/recommendations/_SUCCESS"; then
  run_als recommend
fi
if ! "$hdfs" dfs -test -s "$base/result/realtime/latest.json"; then
  if "$hdfs" dfs -test -e "$base/checkpoint/rating_windows" ||
     "$hdfs" dfs -test -e "$base/result/realtime/batches"; then
    printf 'Existing Streaming state needs its matching Kafka topic. See the VMware handoff guide.\n' >&2
    exit 1
  fi
  bash "$root/scripts/fetch-streaming-deps.sh"
  bash "$stream" all
fi

"$hdfs" dfs -test -e "$base/result/recommendations/_SUCCESS"
"$hdfs" dfs -test -s "$base/result/realtime/latest.json"
printf 'DASHBOARD_RESULTS_OK %s/result\n' "$base"
