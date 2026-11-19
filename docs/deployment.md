# 部署与复现

原集群使用 Hadoop 3.2.0、Spark 3.1.2（Scala 2.12）、Hive 3.1.2 和 Kafka 3.9.2，计算任务由 YARN 调度。Flask 已在 Python 3.11 验证。准备能访问彼此的 NameNode、DataNode 和 YARN，检查 SSH、主机名和时间同步，并确保 Hadoop 客户端配置与集群一致。

## 在集群上生成数据与结果

在仓库根目录执行。调整 Hadoop、Spark 和 Hive 的安装路径，下面的例子使用 NameNode 上常见的安装目录。

```bash
export HADOOP_HOME="$HOME/hadoop-3.2.0"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_HOME="$HOME/spark-3.1.2-bin-hadoop3.2"
export HIVE_HOME="$HOME/software/apache-hive-3.1.2-bin"
export MOVIELENS_HDFS_BASE=hdfs://namenode:9000/movie

"$HADOOP_HOME/bin/hdfs" dfsadmin -report
"$HADOOP_HOME/bin/yarn" node -list
```

从 [GroupLens](https://grouplens.org/datasets/movielens/1m/) 下载并解压 MovieLens 1M。命令中的 `/path/to/ml-1m` 是包含 `ratings.dat`、`movies.dat` 和 `users.dat` 的目录。

```bash
bash pipeline/ingest/load_raw.sh /path/to/ml-1m
"$SPARK_HOME/bin/spark-shell" --master yarn --deploy-mode client \
  --driver-memory 1g --num-executors 1 --executor-cores 1 --executor-memory 1g \
  --conf spark.sql.shuffle.partitions=8 --conf spark.sql.catalogImplementation=in-memory \
  -i pipeline/ingest/clean.scala </dev/null
"$HIVE_HOME/bin/hive" --hivevar movie_root="$MOVIELENS_HDFS_BASE" -f pipeline/sql/tables.sql
bash pipeline/offline/run-stage3-offline.sh
```

继续在同一 Bash 会话执行 ALS。第一次依次使用 `train` 和 `recommend`；已有模型和推荐时可用 `query` 检查指定用户。

```bash
run_als() {
  MOVIELENS_ALS_ACTION="$1" "$SPARK_HOME/bin/spark-shell" --master yarn --deploy-mode client \
    --driver-memory 1g --num-executors 1 --executor-cores 1 --executor-memory 1g \
    --conf spark.task.maxDirectResultSize=16m --conf spark.sql.shuffle.partitions=8 \
    --conf spark.sql.catalogImplementation=in-memory -i pipeline/als/train_als.scala </dev/null
}
run_als train
run_als recommend
MOVIELENS_ALS_USER_ID=1 run_als query
```

Kafka 脚本支持 `prepare`、`replay`、`stream`、`follow`。`stream` 在历史消息消费完成后退出，`follow` 用于持续等待新消息。已有 broker 时可用 `MOVIELENS_KAFKA_BOOTSTRAP` 指定地址。首次安装 Kafka 的机器需要访问 Apache 存档，connector 从 Maven Central 获取。

```bash
bash scripts/fetch-streaming-deps.sh
bash pipeline/streaming/run-stage5.sh prepare
bash pipeline/streaming/run-stage5.sh replay
bash pipeline/streaming/run-stage5.sh stream
bash verification/verify_hive.sh
```

输出路径默认为 `/movie/clean`、`/movie/model/als`、`/movie/result/offline`、`/movie/result/recommendations` 和 `/movie/result/realtime`。清洗和模型训练会检查目标目录是否已存在。保留原环境结果时，先核验已有产物，重跑请使用独立环境。用于看板的 HDFS 项目目录为 `/movie`。

## 验证看板

按 [README](../README.md) 启动 Flask，在浏览器确认总览、用户 1 推荐和快照。WebHDFS 会把文件请求重定向到 DataNode，因此 Flask 所在机器要能访问 NameNode 和实际 DataNode 地址。使用 `MOVIELENS_DATANODES` 配置 DataNode 名称到地址的映射。

完整浏览器回归需要 Edge、Node.js 和 Playwright Core，以及一个数据量与原 MovieLens 1M 相同的项目环境。先启动用户看板服务，再运行以下命令。需要时使用 `MOVIELENS_PYTHON` 指向安装 Flask 依赖的 Python 可执行文件。脚本另起 5067 端口模拟 Kafka 断开。

```bash
npm install --no-save playwright-core
node verification/verify_browser.cjs
```

采集 Kafka 校验摘要可使用 Kafka 发行版客户端库和 JDK 17，运行 `java -cp "/path/to/kafka/libs/*" verification/VerifyKafka.java namenode:9092`。GitHub Actions 独立检查源码语法和 Flask 路由，集群联调按此文档执行。
