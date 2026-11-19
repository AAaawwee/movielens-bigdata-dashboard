#!/usr/bin/env bash
set -euo pipefail

export HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop-3.2.0}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
export HIVE_HOME="${HIVE_HOME:-$HOME/software/apache-hive-3.1.2-bin}"
spark_home="${SPARK_HOME:-$HOME/spark-3.1.2-bin-hadoop3.2}"
hdfs="$HADOOP_HOME/bin/hdfs"
hive="$HIVE_HOME/bin/hive"
export MOVIELENS_HDFS_BASE="${MOVIELENS_HDFS_BASE:-hdfs://namenode:9000/movie}"
output="$MOVIELENS_HDFS_BASE/result/offline"

cd "$HOME"
"$hdfs" dfs -test -d "$MOVIELENS_HDFS_BASE/result"

if ! "$hdfs" dfs -test -e "$output"; then
  scala_file=$(mktemp /tmp/movielens-offline.XXXXXX.scala)
  trap 'rm -f -- "$scala_file"' EXIT

  cat > "$scala_file" <<'SCALA'
import org.apache.hadoop.fs.{FileContext, Options, Path}
import org.apache.spark.sql.functions.{asc, desc}

def runOffline(): Unit = {
  val base = sys.env("MOVIELENS_HDFS_BASE")
  val target = new Path(s"$base/result/offline")
  val hadoopConf = spark.sparkContext.hadoopConfiguration
  require(!target.getFileSystem(hadoopConf).exists(target), s"Output exists: $target")
  val staging = new Path(target.getParent, s".offline-${java.util.UUID.randomUUID()}")

  spark.conf.set("spark.sql.session.timeZone", "UTC")
  spark.conf.set("spark.sql.shuffle.partitions", "6")
  spark.read.parquet(s"$base/clean/ratings").createOrReplaceTempView("ratings")
  spark.read.parquet(s"$base/clean/movies").createOrReplaceTempView("movies")
  spark.sql("""
    SELECT user_id, movie_id, rating,
           CAST(from_unixtime(`timestamp`) AS TIMESTAMP) AS event_time
    FROM ratings
  """).createOrReplaceTempView("rating_events")

  def save(name: String, query: String): Unit = {
    spark.sql(query).coalesce(1).write.parquet(new Path(staging, name).toString)
  }

  save("movie_stats", """
    SELECT m.movie_id, m.title, m.genres, COUNT(*) AS rating_count,
           COUNT(DISTINCT r.user_id) AS rater_count, ROUND(AVG(r.rating), 4) AS avg_rating
    FROM ratings r JOIN movies m ON r.movie_id = m.movie_id
    GROUP BY m.movie_id, m.title, m.genres
  """)

  save("genre_stats", """
    SELECT genre, COUNT(DISTINCT movie_id) AS movie_count,
           COUNT(*) AS rating_count, ROUND(AVG(rating), 4) AS avg_rating
    FROM (
      SELECT r.movie_id, r.rating, EXPLODE(SPLIT(m.genres, '[|]')) AS genre
      FROM ratings r JOIN movies m ON r.movie_id = m.movie_id
    ) expanded
    WHERE genre <> ''
    GROUP BY genre
  """)

  save("user_stats", """
    SELECT user_id, COUNT(*) AS rating_count, ROUND(AVG(rating), 4) AS avg_rating
    FROM ratings GROUP BY user_id
  """)

  save("daily_trend", """
    SELECT TO_DATE(event_time) AS rating_date, COUNT(*) AS rating_count,
           COUNT(DISTINCT user_id) AS active_users, ROUND(AVG(rating), 4) AS avg_rating
    FROM rating_events GROUP BY TO_DATE(event_time)
  """)

  save("rating_distribution", """
    SELECT CAST(rating AS INT) AS stars, COUNT(*) AS rating_count
    FROM ratings GROUP BY CAST(rating AS INT)
  """)

  save("hourly_stats", """
    SELECT HOUR(event_time) AS rating_hour, COUNT(*) AS rating_count,
           ROUND(AVG(rating), 4) AS avg_rating
    FROM rating_events GROUP BY HOUR(event_time)
  """)

  for ((name, key) <- Seq("movie_stats" -> "movie_id", "genre_stats" -> "genre")) {
    spark.read.parquet(new Path(staging, name).toString)
      .orderBy(desc("rating_count"), asc(key)).limit(20).coalesce(1)
      .write.json(new Path(staging, s"flask_snapshot/$name").toString)
  }

  FileContext.getFileContext(target.toUri, hadoopConf).rename(staging, target, Options.Rename.NONE)
}

runOffline()
SCALA

  "$spark_home/bin/spark-shell" \
    --master yarn --num-executors 1 --executor-cores 1 --executor-memory 1g \
    --conf spark.sql.catalogImplementation=in-memory \
    -i "$scala_file" </dev/null
fi

for name in movie_stats genre_stats user_stats daily_trend rating_distribution hourly_stats \
    flask_snapshot/movie_stats flask_snapshot/genre_stats; do
  if ! "$hdfs" dfs -test -e "$output/$name/_SUCCESS"; then
    printf 'Missing Spark output: %s\n' "$output/$name/_SUCCESS" >&2
    exit 1
  fi
done

printf 'OFFLINE_OK=%s\n' "$output"
"$hdfs" dfs -ls "$output"
"$hive" -e '
USE movielens;
SELECT movie_id, title, rating_count, avg_rating FROM dws_movie_stats LIMIT 5;
SELECT genre, movie_count, rating_count, avg_rating FROM dws_genre_stats LIMIT 5;
'
