#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 1 ]]; then
  printf 'Usage: %s /path/to/extracted/ml-1m\n' "$0" >&2
  exit 2
fi

hdfs="${HADOOP_HOME:-$HOME/hadoop-3.2.0}/bin/hdfs"
base="${MOVIELENS_HDFS_BASE:-hdfs://namenode:9000/movie}"
for file in movies.dat ratings.dat users.dat; do
  test -s "$1/$file"
done
"$hdfs" dfs -mkdir -p "$base/raw" "$base/result"
for file in movies.dat ratings.dat users.dat; do
  path="$base/raw/$file"
  if "$hdfs" dfs -test -f "$path"; then
    "$hdfs" dfs -cat "$path" | cmp - "$1/$file"
  else
    "$hdfs" dfs -put "$1/$file" "$path"
  fi
done
"$hdfs" dfs -ls -h "$base/raw"
