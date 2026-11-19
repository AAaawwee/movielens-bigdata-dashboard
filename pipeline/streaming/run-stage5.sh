#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
stage_dir="$PWD"
export HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop-3.2.0}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
if [[ -z "${JAVA_HOME:-}" && -x /usr/java/jdk1.8.0_51/bin/javac ]]; then
  export JAVA_HOME=/usr/java/jdk1.8.0_51
fi
if [[ -n "${JAVA_HOME:-}" ]]; then
  export PATH="$JAVA_HOME/bin:$PATH"
fi
spark_home="${SPARK_HOME:-$HOME/spark-3.1.2-bin-hadoop3.2}"
kafka_version=3.9.2
kafka_dist="kafka_2.13-$kafka_version"
kafka_home="${MOVIELENS_KAFKA_HOME:-$stage_dir/$kafka_dist}"
topic=movie_rating_events
export MOVIELENS_HDFS_BASE="${MOVIELENS_HDFS_BASE:-hdfs://namenode:9000/movie}"
raw_ratings="$MOVIELENS_HDFS_BASE/raw/ratings.dat"
expected=1000209

cluster_ip="${MOVIELENS_BROKER_IP:-$(ip -4 -o addr show scope global | awk 'NR == 1 { split($4, addr, "/"); print addr[1] }')}"
bootstrap="${MOVIELENS_KAFKA_BOOTSTRAP:-$cluster_ip:9092}"
export MOVIELENS_KAFKA_BOOTSTRAP="$bootstrap"
export MOVIELENS_KAFKA_TOPIC="$topic"

install_kafka() {
  if [[ -x "$kafka_home/bin/kafka-topics.sh" ]]; then
    return
  fi
  if [[ "$kafka_home" != "$stage_dir/$kafka_dist" ]]; then
    printf 'MOVIELENS_KAFKA_HOME lacks bin/kafka-topics.sh: %s\n' "$kafka_home" >&2
    return 1
  fi

  local archive="$stage_dir/$kafka_dist.tgz"
  local source="https://archive.apache.org/dist/kafka/$kafka_version/$kafka_dist.tgz"
  if [[ ! -f "$archive" ]]; then
    curl -fsSL --retry 2 "$source" -o "$archive.download"
    mv "$archive.download" "$archive"
  fi
  if [[ ! -f "$archive.sha512" ]]; then
    curl -fsSL --retry 2 "$source.sha512" -o "$archive.sha512"
  fi
  local expected_sha actual_sha
  expected_sha=$(sed '1s/^[^:]*://' "$archive.sha512" | tr -d '[:space:]' | tr 'A-F' 'a-f')
  actual_sha=$(sha512sum "$archive" | awk '{print $1}')
  if [[ "$expected_sha" != "$actual_sha" ]]; then
    printf 'Kafka archive checksum mismatch: %s\n' "$archive" >&2
    return 1
  fi
  tar -xzf "$archive" -C "$stage_dir"
}

topic_cli() {
  KAFKA_HEAP_OPTS='-Xms64m -Xmx256m' timeout 20s \
    "$kafka_home/bin/kafka-topics.sh" --bootstrap-server "$bootstrap" "$@"
}

start_broker() {
  if [[ -z "$cluster_ip" || "$bootstrap" != "$cluster_ip:9092" || "$kafka_home" != "$stage_dir/$kafka_dist" ]]; then
    printf 'Kafka broker is unreachable at %s\n' "$bootstrap" >&2
    return 1
  fi
  local configuration="$stage_dir/kafka-stage5.properties"
  local storage="$stage_dir/kafka-data"
  mkdir -p "$storage" "$stage_dir/kafka-logs"
  cat > "$configuration" <<CONFIG
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:19093
listeners=PLAINTEXT://$cluster_ip:9092,CONTROLLER://127.0.0.1:19093
advertised.listeners=PLAINTEXT://$cluster_ip:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs=$storage
num.partitions=1
auto.create.topics.enable=false
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
CONFIG
  if [[ ! -f "$storage/meta.properties" ]]; then
    local cluster_id
    cluster_id=$("$kafka_home/bin/kafka-storage.sh" random-uuid)
    "$kafka_home/bin/kafka-storage.sh" format -t "$cluster_id" -c "$configuration"
  fi
  LOG_DIR="$stage_dir/kafka-logs" KAFKA_HEAP_OPTS='-Xms256m -Xmx384m' \
    "$kafka_home/bin/kafka-server-start.sh" -daemon "$configuration"

  for attempt in $(seq 1 30); do
    if (exec 3<>"/dev/tcp/$cluster_ip/9092") 2>/dev/null; then
      return
    fi
    sleep 1
  done
  printf 'Kafka did not start, inspect %s\n' "$stage_dir/kafka-logs" >&2
  return 1
}

prepare() {
  install_kafka
  if [[ "$bootstrap" == "$cluster_ip:9092" ]] && ! (exec 3<>"/dev/tcp/$cluster_ip/9092") 2>/dev/null; then
    start_broker
  fi
  topic_cli --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1
  topic_cli --describe --topic "$topic"
}

replay() {
  local classes="$stage_dir/classes"
  mkdir -p "$classes"
  exec 9>"$stage_dir/replay.lock"
  flock -n 9
  javac -source 8 -target 8 -cp "$kafka_home/libs/*" -d "$classes" "$stage_dir/ReplayRatings.java"
  "$HADOOP_HOME/bin/hdfs" dfs -cat "$raw_ratings" |
    LC_ALL=C sort -t ':' -k7,7n -k1,1n -k3,3n |
    java -Xms64m -Xmx256m -cp "$classes:$kafka_home/libs/*" ReplayRatings "$bootstrap" "$topic" "$expected"
}

stream() {
  local connector_jars
  exec 8>"$stage_dir/stream.lock"
  flock -n 8
  local jars=("$stage_dir"/lib/*.jar)
  if [[ ${#jars[@]} -ne 4 || ! -f "${jars[0]}" ]]; then
    printf 'The four Spark Kafka connector jars are required under %s/lib\n' "$stage_dir" >&2
    return 1
  fi
  connector_jars=$(IFS=,; printf '%s' "${jars[*]}")
  MOVIELENS_EXPECTED_EVENTS="$1" "$spark_home/bin/spark-shell" \
    --master yarn --deploy-mode client --name MovieLens-Kafka-Windows \
    --driver-memory 1g --num-executors 1 --executor-cores 1 --executor-memory 1g \
    --conf spark.task.maxDirectResultSize=16m --conf spark.sql.shuffle.partitions=6 \
    --conf spark.sql.catalogImplementation=in-memory --jars "$connector_jars" \
    -i "$stage_dir/stream_ratings.scala" </dev/null 2>&1 | tee "$stage_dir/stream.log"
  if [[ -n "$1" ]]; then
    grep -aq 'KAFKA_STREAM_OK events=' "$stage_dir/stream.log"
  fi
}

case "${1:-all}" in
  all)
    prepare
    replay
    stream "$expected"
    ;;
  prepare) prepare ;;
  replay) replay ;;
  stream) stream "$expected" ;;
  follow) stream '' ;;
  *) printf 'Usage: %s {all|prepare|replay|stream|follow}\n' "$0" >&2; exit 2 ;;
esac
