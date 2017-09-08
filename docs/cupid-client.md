# Cupid Client Mode
> See Detail Demo @ SparkClientNormalFT.scala

## Demo Explain

A Full `ClientMode Demo` include following files:

* Client Control
    - spark-examples/src/main/scala/com/aliyun/odps/spark/examples/SparkClientNormalFT.scala
    - spark-examples/src/main/scala/com/aliyun/odps/spark/examples/SparkClientNormalApp.scala
* SparkJob Unit
    - client-jobexamples/src/main/scala/com/aliyun/odps/cupid/client/spark/CacheRddJob.scala
    - client-jobexamples/src/main/scala/com/aliyun/odps/cupid/client/spark/ParallRddJob.scala
    - client-jobexamples/src/main/scala/com/aliyun/odps/cupid/client/spark/UseCacheRdd.scala
    - client-jobexamples/src/main/scala/com/aliyun/odps/cupid/client/spark/SparkJobKill.scala

```
# Client Control try to get a SparkContext handle via following API
# with which user can interative with the sparkContext in ODPS Cluster

val cupidSparkClient = CupidSparkClientRunner.getReadyCupidSparkClient()

# Users need to implement their own SparkJobUnit so as to submit it into the cluster
# CacheRddJob、ParallRddJob、UseCacheRdd、SparkJobKill, in fact are SparkJobUnit which
# will be submitted into the cluster
# implement interface SparkJob

object CacheRddJob extends SparkJob {

  override def runJob(jobContext: JobContext, jobConf: Config): Array[Byte] = {
    val parallRdd = jobContext.sc().parallelize(Seq[Int](1, 2, 3), 1)
    val cacheRDDName = jobConf.getString("cacheRDDName")
    jobContext.putNamedObject(cacheRDDName, parallRdd)
    JavaSerializerInstance.getInstance.serialize(cacheRDDName).array()
  }
}

# SparkJobUnit should be managed by cupidSparkClient with following API
1. addJar
2. startJob
3. getJobStatus
4. getJobResult
5. killJob

# And via JavaSerializer user can get result from the SparkJobUnit
JavaSerializerInstance.getInstance.serialize("HelloWorld").array()
```

## Run Demo

```
# Steps to run Demo

1. build aliyun-cupid-sdk
2. properly set spark.defaults.conf
3. export SPARK_HOME in env
4. run command:

java -cp \
/path/to/aliyun-cupid-sdk/examples/spark-examples/target/spark-examples_2.11-1.0.0-SNAPSHOT-shaded.jar:$SPARK_HOME/jars/* \
com.aliyun.odps.spark.examples.SparkClientNormalFT /path/to/aliyun-cupid-sdk/examples/client-jobexamples/target/client-jobexamples_2.11-1.0.0-SNAPSHOT.jar
```
