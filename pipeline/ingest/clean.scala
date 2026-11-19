import java.nio.charset.StandardCharsets
import java.util.UUID

import org.apache.hadoop.fs.{FileContext, Options, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.util.control.NonFatal

def cleanMovieLens(): Unit = {
  val base = sys.env.getOrElse("MOVIELENS_HDFS_BASE", "hdfs://namenode:9000/movie")
  val target = new Path(s"$base/clean")
  val staging = new Path(target.getParent, s".clean-${UUID.randomUUID()}")
  val conf = spark.sparkContext.hadoopConfiguration
  val fs = target.getFileSystem(conf)
  require(!fs.exists(target), s"Clean output exists: $target")

  def movieLines = spark.read.format("binaryFile").load(s"$base/raw/movies.dat")
      .select(decode(col("content"), StandardCharsets.ISO_8859_1.name()).as("decoded"))
      .select(explode(split(col("decoded"), "\\r?\\n")).as("line"))

  def read(name: String): DataFrame = {
    val lines = if (name == "movies") movieLines else spark.read.text(s"$base/raw/$name.dat").toDF("line")
    lines.where(length(col("line")) > 0).select(split(col("line"), "::").as("parts"))
  }

  def publish(frame: DataFrame, name: String, keys: Seq[String]): Unit = {
    val cleaned = frame.na.drop().dropDuplicates(keys)
    val count = cleaned.count()
    require(count > 0, s"No valid $name")
    cleaned.write.parquet(new Path(staging, name).toString)
    println(s"CLEAN_ROWS $name=$count")
  }

  def field(index: Int) = col("parts").getItem(index)
  try {
    val ratings = read("ratings").where(size(col("parts")) === 4)
      .select(field(0).cast("int").as("user_id"), field(1).cast("int").as("movie_id"),
        field(2).cast("double").as("rating"), field(3).cast("long").as("timestamp"))
      .where(col("user_id") > 0 && col("movie_id") > 0 && col("rating").between(1, 5) && col("timestamp") > 0)
    val movies = read("movies").where(size(col("parts")) === 3)
      .select(field(0).cast("int").as("movie_id"), field(1).as("title"), field(2).as("genres"))
      .where(col("movie_id") > 0 && length(col("title")) > 0 && length(col("genres")) > 0)
    val users = read("users").where(size(col("parts")) === 5)
      .select(field(0).cast("int").as("user_id"), field(1).as("gender"), field(2).cast("int").as("age_group"),
        field(3).cast("int").as("occupation"), field(4).as("zip_code"))
      .where(col("user_id") > 0)
    publish(ratings, "ratings", Seq("user_id", "movie_id"))
    publish(movies, "movies", Seq("movie_id"))
    publish(users, "users", Seq("user_id"))
    FileContext.getFileContext(target.toUri, conf).rename(staging, target, Options.Rename.NONE)
  } finally {
    if (fs.exists(staging)) fs.delete(staging, true)
  }
}

val cleanExit = try {
  spark.sparkContext.setLogLevel("WARN")
  cleanMovieLens()
  0
} catch {
  case NonFatal(error) => error.printStackTrace(); 1
} finally spark.stop()
sys.exit(cleanExit)
