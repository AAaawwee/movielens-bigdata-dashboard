import argparse
import json
import os
from functools import reduce
from time import perf_counter
from uuid import uuid4

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import SparkSession, Window, functions as F


HDFS_BASE = os.environ.get("MOVIELENS_HDFS_BASE", "hdfs://namenode:9000/movie")
RATINGS_PATH = HDFS_BASE + "/clean/ratings"
MOVIES_PATH = HDFS_BASE + "/clean/movies"
MODEL_PATH = HDFS_BASE + "/model/als"
RECOMMENDATIONS_PATH = HDFS_BASE + "/result/recommendations"
KEYS = ["user_id", "movie_id"]
RESULT_COLUMNS = KEYS + ["predicted_rating", "recommendation_rank"]


def publish_directory(spark, target, write, allow_empty=False):
    """Publish a complete HDFS directory after all Spark writes finish."""
    context = spark.sparkContext
    jvm = context._jvm
    hadoop_conf = context._jsc.hadoopConfiguration()
    destination = jvm.org.apache.hadoop.fs.Path(target)
    filesystem = destination.getFileSystem(hadoop_conf)
    file_context = jvm.org.apache.hadoop.fs.FileContext.getFileContext(
        destination.toUri(), hadoop_conf
    )
    staging = jvm.org.apache.hadoop.fs.Path(
        destination.getParent(), ".{}-{}".format(destination.getName(), uuid4().hex)
    )

    def is_empty_directory():
        return filesystem.isDirectory(destination) and len(filesystem.listStatus(destination)) == 0

    if filesystem.exists(destination) and not (allow_empty and is_empty_directory()):
        raise FileExistsError("Output already exists: " + target)

    try:
        write(staging.toString())
        if allow_empty and is_empty_directory():
            filesystem.delete(destination, False)
        options = context._gateway.new_array(jvm.org.apache.hadoop.fs.Options.Rename, 1)
        options[0] = jvm.org.apache.hadoop.fs.Options.Rename.NONE
        file_context.rename(staging, destination, options)
    finally:
        if filesystem.exists(staging):
            filesystem.delete(staging, True)


def load_ratings(spark, path):
    return spark.read.parquet(path).select(*KEYS, "rating", "timestamp").dropna()


def train(spark, args):
    ratings = load_ratings(spark, args.ratings_path).persist()
    cutoffs = ratings.approxQuantile("timestamp", [0.8], 0.001)
    if not cutoffs:
        raise ValueError("No usable ratings found at " + args.ratings_path)
    cutoff = int(cutoffs[0])
    training = ratings.where(F.col("timestamp") <= cutoff)
    test = ratings.where(F.col("timestamp") > cutoff)
    train_rows = training.count()
    test_rows = test.count()
    if not train_rows or not test_rows:
        raise ValueError("The chronological split must have both train and test ratings")

    estimator = ALS(
        userCol="user_id",
        itemCol="movie_id",
        ratingCol="rating",
        rank=args.rank,
        maxIter=args.max_iter,
        regParam=0.1,
        numUserBlocks=4,
        numItemBlocks=4,
        implicitPrefs=False,
        coldStartStrategy="drop",
        seed=42,
    )
    started = perf_counter()
    evaluation_model = estimator.fit(training)
    eval_seconds = perf_counter() - started
    predictions = evaluation_model.transform(test).select("rating", "prediction").persist()
    evaluated_rows = predictions.count()
    if not evaluated_rows:
        raise ValueError("No test ratings have both a trained user factor and a trained movie factor")

    evaluator = RegressionEvaluator(labelCol="rating", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
    mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
    predictions.unpersist()

    # Refit after evaluation so the saved model covers users in the entire ratings snapshot.
    started = perf_counter()
    model = estimator.fit(ratings)
    final_seconds = perf_counter() - started
    metrics = {
        "application_id": spark.sparkContext.applicationId,
        "split_timestamp": cutoff,
        "train_rows": train_rows,
        "test_rows": test_rows,
        "evaluated_test_rows": evaluated_rows,
        "test_coverage": evaluated_rows / test_rows,
        "rmse": rmse,
        "mae": mae,
        "evaluation_fit_seconds": eval_seconds,
        "final_fit_seconds": final_seconds,
        "total_fit_seconds": eval_seconds + final_seconds,
        "rank": args.rank,
        "max_iter": args.max_iter,
        "reg_param": 0.1,
    }
    metrics_json = json.dumps(metrics, allow_nan=False, sort_keys=True)

    def save_model(path):
        model.save(path)
        text = spark.range(1).select(F.lit(metrics_json).alias("value"))
        text.coalesce(1).write.text(path + "/_training_metrics")

    publish_directory(spark, args.model_path, save_model)
    print("ALS_TRAINING_METRICS " + metrics_json, flush=True)
    ratings.unpersist()


def top_unrated(model, users, rated, top_n):
    movie_count = model.itemFactors.count()
    candidate_count = min(movie_count, max(50, top_n * 4))
    ordering = Window.partitionBy("user_id").orderBy(
        F.desc("predicted_rating"), F.asc("movie_id")
    )
    pending = users
    completed = []
    cached = []
    while candidate_count:
        candidates = model.recommendForUserSubset(pending, candidate_count)
        scores = candidates.select("user_id", F.explode("recommendations").alias("item")).select(
            "user_id",
            F.col("item.movie_id").alias("movie_id"),
            F.col("item.rating").cast("float").alias("predicted_rating"),
        )
        ranked = (
            scores.join(rated, KEYS, "left_anti")
            .withColumn("recommendation_rank", F.row_number().over(ordering))
            .where(F.col("recommendation_rank") <= top_n)
            .select(*RESULT_COLUMNS)
            .persist()
        )
        cached.append(ranked)
        if candidate_count == movie_count:
            completed.append(ranked)
            break

        satisfied = ranked.where(F.col("recommendation_rank") == top_n).select("user_id")
        remaining = pending.join(satisfied, "user_id", "left_anti").persist()
        cached.append(remaining)
        if remaining.count() == 0:
            completed.append(ranked)
            break

        # Expand the candidate list only for users who still need unseen movies.
        completed.append(ranked.join(remaining, "user_id", "left_anti"))
        pending = remaining
        candidate_count = min(movie_count, candidate_count * 2)

    if not completed:
        raise ValueError("The saved model has no movie factors")
    return reduce(lambda left, right: left.unionByName(right), completed), cached


def recommend(spark, args):
    model = ALSModel.load(args.model_path)
    rated = spark.read.parquet(args.ratings_path).select(*KEYS).distinct().persist()
    users = model.userFactors.select(F.col("id").alias("user_id"))
    if args.command == "query":
        users = users.where(F.col("user_id") == args.user_id)
        if not users.take(1):
            raise ValueError("User {} has no saved factor".format(args.user_id))
        rated = rated.where(F.col("user_id") == args.user_id)

    result, cached = top_unrated(model, users, rated, args.top_n)
    try:
        if args.command == "query":
            movies = spark.read.parquet(args.movies_path).select("movie_id", "title")
            result.join(movies, "movie_id", "left").orderBy("recommendation_rank").select(
                *RESULT_COLUMNS, "title"
            ).show(args.top_n, truncate=False)
        else:
            publish_directory(
                spark, args.result_path, lambda path: result.write.parquet(path), allow_empty=True
            )
    finally:
        for frame in cached:
            frame.unpersist()


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("Expected a positive integer")
    return number


def parse_args():
    parser = argparse.ArgumentParser(description="MovieLens Stage 4 ALS on Spark")
    parser.add_argument("--ratings-path", default=RATINGS_PATH)
    parser.add_argument("--model-path", default=MODEL_PATH)
    commands = parser.add_subparsers(dest="command")
    train_parser = commands.add_parser("train")
    train_parser.add_argument("--rank", type=positive_int, default=20)
    train_parser.add_argument("--max-iter", type=positive_int, default=10)
    batch_parser = commands.add_parser("recommend")
    batch_parser.add_argument("--top-n", type=positive_int, default=10)
    batch_parser.add_argument("--result-path", default=RECOMMENDATIONS_PATH)
    query_parser = commands.add_parser("query")
    query_parser.add_argument("--user-id", type=positive_int, required=True)
    query_parser.add_argument("--top-n", type=positive_int, default=10)
    query_parser.add_argument("--movies-path", default=MOVIES_PATH)
    args = parser.parse_args()
    if args.command is None:
        parser.error("Choose train, recommend or query")
    return args


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("MovieLens-ALS-" + args.command).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if args.command == "train":
            train(spark, args)
        else:
            recommend(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
