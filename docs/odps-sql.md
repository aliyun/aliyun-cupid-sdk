# Load ODPS Data via SQL
> See Detail Demo @ OdpsTableReadWriteViaSQL.scala

## Demo Explain

```
# spark.sql.catalogImplementation=odps in spark-defaults.conf
val spark = SparkSession
      .builder()
      .appName("OdpsTableReadWriteViaSQL")
      .getOrCreate()

val projectName = spark.sparkContext.getConf.get("odps.project.name")
val tableName = "cupid_wordcount"

// get a ODPS table as a DataFrame
val df = spark.table(tableName)
println(s"df.count: ${df.count()}")

// Just do some query
spark.sql(s"select * from $tableName limit 10").show(10, 200)
spark.sql(s"select id, count(id) from $tableName group by id").show(10, 200)

// any table exists under project could be use
// productRevenue
spark.sql(
  """
    |SELECT product,
    |       category,
    |       revenue
    |FROM
    |  (SELECT product,
    |          category,
    |          revenue,
    |          dense_rank() OVER (PARTITION BY category
    |                             ORDER BY revenue DESC) AS rank
    |   FROM productRevenue) tmp
    |WHERE rank <= 2
  """.stripMargin).show(10, 200)

spark.stop()
```

## Run Demo

```
# Steps to run Demo

1. build aliyun-cupid-sdk
2. properly set spark.defaults.conf
3. run command:

bin/spark-submit \
--master yarn-cluster \
--class com.aliyun.odps.spark.examples.OdpsTableReadWriteViaSQL \
/path/to/aliyun-cupid-sdk/examples/spark-examples/target/spark-examples_2.11-1.0.0-SNAPSHOT-shaded.jar
```
