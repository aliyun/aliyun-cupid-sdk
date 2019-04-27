## pom.xml 须知

请注意 用户构建Spark应用的时候，由于是用MaxCompute提供的Spark客户端去提交应用，故需要注意一些依赖scope的定义

* spark-core spark-sql等所有spark社区发布的包，用provided scope
* cupid-sdk 用provided scope
* odps-spark-datasource 用默认的compile scope

```
<properties>
    <spark.version>1.6.3</spark.version>
    <cupid.sdk.version>3.3.3-public</cupid.sdk.version>
    <scala.version>2.10.4</scala.version>
    <scala.binary.version>2.10</scala.binary.version>
</properties>

<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-core_${scala.binary.version}</artifactId>
    <version>${spark.version}</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_${scala.binary.version}</artifactId>
    <version>${spark.version}</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-mllib_${scala.binary.version}</artifactId>
    <version>${spark.version}</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-streaming_${scala.binary.version}</artifactId>
    <version>${spark.version}</version>
    <scope>provided</scope>
</dependency>

<dependency>
    <groupId>com.aliyun.odps</groupId>
    <artifactId>cupid-sdk</artifactId>
    <version>${cupid.sdk.version}</version>
    <scope>provided</scope>
</dependency>

<dependency>
    <groupId>com.aliyun.odps</groupId>
    <artifactId>hadoop-fs-oss</artifactId>
    <version>${cupid.sdk.version}</version>
</dependency>

<dependency>
    <groupId>com.aliyun.odps</groupId>
    <artifactId>odps-spark-datasource_${scala.binary.version}</artifactId>
    <version>${cupid.sdk.version}</version>
</dependency>

<dependency>
    <groupId>org.scala-lang</groupId>
    <artifactId>scala-library</artifactId>
    <version>${scala.version}</version>
</dependency>
<dependency>
    <groupId>org.scala-lang</groupId>
    <artifactId>scala-actors</artifactId>
    <version>${scala.version}</version>
</dependency>
```

## 案例说明

### SparkPi

[详细代码](src/main/scala/com/aliyun/odps/spark/examples/SparkPi.scala)

提交方式

```
Step 1. build example
Step 2. properly set spark.defaults.conf
Step 3. bin/spark-submit --master yarn-cluster --class \
      com.aliyun.odps.spark.examples.SparkPi \
      ${path to spark-examples}/target/spark-examples_2.10-3.3.3-public-shaded.jar
```


### WordCount

[详细代码](src/main/scala/com/aliyun/odps/spark/examples/WordCount.scala)

提交方式

```
Step 1. build example
Step 2. properly set spark.defaults.conf
Step 3. bin/spark-submit --master yarn-cluster --class \
      com.aliyun.odps.spark.examples.WordCount \
      ${path to spark-examples}/target/spark-examples_2.10-3.3.3-public-shaded.jar
```

### GraphX PageRank

[详细代码](src/main/scala/com/aliyun/odps/spark/examples/graphx/PageRank.scala)

提交方式

```
Step 1. build example
Step 2. properly set spark.defaults.conf
Step 3. bin/spark-submit --master yarn-cluster --class \
      com.aliyun.odps.spark.examples.graphx.PageRank \
      ${path to spark-examples}/target/spark-examples_2.10-3.3.3-public-shaded.jar
```

### Mllib Kmeans-ON-OSS

KmeansModelSaveToOss

[详细代码](src/main/scala/com/aliyun/odps/spark/examples/mllib/KmeansModelSaveToOss.scala)

提交方式

```
# 代码中的OSS账号信息相关需要填上，再编译提交
conf.set("spark.hadoop.fs.oss.accessKeyId", "***")
conf.set("spark.hadoop.fs.oss.accessKeySecret", "***")
conf.set("spark.hadoop.fs.oss.endpoint", "oss-cn-hangzhou-zmf.aliyuncs.com")


Step 1. build aliyun-cupid-sdk
Step 2. properly set spark.defaults.conf
Step 3. bin/spark-submit --master yarn-cluster --class \
      com.aliyun.odps.spark.examples.mllib.KmeansModelSaveToOss \
      ${path to spark-examples}/target/spark-examples_2.10-3.3.3-public-shaded.jar
```

### OSS UnstructuredData

SparkUnstructuredDataCompute

[详细代码](src/main/scala/com/aliyun/odps/spark/examples/oss/SparkUnstructuredDataCompute.scala)

提交方式

```
# 代码中的OSS账号信息相关需要填上，再编译提交
conf.set("spark.hadoop.fs.oss.accessKeyId", "***")
conf.set("spark.hadoop.fs.oss.accessKeySecret", "***")
conf.set("spark.hadoop.fs.oss.endpoint", "oss-cn-hangzhou-zmf.aliyuncs.com")


Step 1. build example
Step 2. properly set spark.defaults.conf
Step 3. bin/spark-submit --master yarn-cluster --class \
      com.aliyun.odps.spark.examples.oss.SparkUnstructuredDataCompute \
      ${path to spark-examples}/target/spark-examples_2.10-3.3.3-public-shaded.jar
```

### Spark-SQL on MaxCompute Table

[详细代码](src/main/scala/com/aliyun/odps/spark/examples/sparksql/SparkSQL.scala)

提交方式

```
# 运行可能会报Table Not Found的异常，因为用户的MaxCompute Project中没有代码中指定的表
# 可以参考代码中的各种接口，实现对应Table的SparkSQL应用

Step 1. build example
Step 2. properly set spark.defaults.conf
Step 3. bin/spark-submit --master yarn-cluster --class \
      com.aliyun.odps.spark.examples.sparksql.SparkSQL \
      ${path to spark-examples}/target/spark-examples_2.10-3.3.3-public-shaded.jar
```
