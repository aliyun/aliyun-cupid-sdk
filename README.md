* [1. aliyun-cupid-sdk简介](#1)
* [2. AliSpark环境准备](#2)
	+ [2.1 下载并解压spark包](#2.1)
	+ [2.2 设置环境变量](#2.2)
	+ [2.3 设置Spark-defaults.conf](#2.3)
* [3. 主要模块以及Maven依赖说明](#3)
* [4. 应用开发](#4)
* [5. AliSpark Roadmap](#5)
* [6. AliSpark安全访问OSS](#6)

<h1 id="1">1. aliyun-cupid-sdk简介</h1>

[MaxCompute Alispark](https://github.com/aliyun/aliyun-cupid-sdk) 是阿里云提供的Spark集成方案，可以与MaxCompute的生态数据无缝集成，也能完美的支持Spark社区大部分功能。aliyun-cupid-sdk则提供了集成所需的API说明以及相关功能Demo，用户可以基于项目提供的Spark-1.x以及Spark-2.x的example项目构建自己的应用，并且提交到MaxCompute集群上。

<h1 id="2">2. AliSpark环境准备</h1>

<h2 id="2.1">2.1 下载并解压AliSpark包</h2>

下载 [Spark on MaxCompute](https://github.com/aliyun/aliyun-cupid-sdk) 安装包

* [spark-1.6.3](http://repo.aliyun.com/download/spark-1.6.3-public.tar.gz)

* [spark-2.3.0](http://repo.aliyun.com/download/spark-2.3.0-odps0.30.0.tar.gz)

<h2 id="2.2">2.2 设置环境变量</h2>

JAVA_HOME设置

```
# 尽量使用 JDK 1.7+
# 1.8+ 最佳
export JAVA_HOME=/path/to/jdk
export CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar
export PATH=$JAVA_HOME/bin:$PATH
```

SPARK_HOME设置

```
export SPARK_HOME=/path/to/spark_extracted_package
export PATH=$SPARK_HOME/bin:$PATH
```

<h2 id="2.3">2.3 设置Spark-defaults.conf</h2>

在 **$SPARK_HOME/conf** 路径创建spark-defaults.conf文件，在该文件中填入以下配置，将空白部分根据实际的账号信息填上即可，其余配置保持不变。

```
# OdpsAccount Info Setting
spark.hadoop.odps.project.name=
spark.hadoop.odps.access.id=
spark.hadoop.odps.access.key=
spark.hadoop.odps.task.major.version=cupid_v2
spark.hadoop.odps.cupid.container.image.enable=true
spark.hadoop.odps.cupid.container.vm.engine.type=hyper
spark.hadoop.odps.end.point=http://service.cn.maxcompute.aliyun.com/api
spark.hadoop.odps.runtime.end.point=http://service.cn.maxcompute.aliyun-inc.com/api
spark.sql.catalogImplementation=odps
```

<h1 id="3">3 主要模块以及Maven依赖说明</h1>

项目中Spark相关的依赖模块主要有三个:

* cupid-sdk **[开源应用接入MaxCompute SDK]**
* odps-spark-datasource_2.11 **[Spark-2.x MaxCompute数据访问API]**
* odps-spark-datasource_2.10 **[Spark-1.x MaxCompute数据访问API]**

```
# scope请设置为provided
# 另外如依赖spark-core spark-sql等模块，也请统一设置为provided
<dependency>
	<groupId>com.aliyun.odps</groupId>
	<artifactId>cupid-sdk</artifactId>
	<version>3.3.3-public</version>
	<scope>provided</scope>
</dependency>

# Spark-2.x请依赖此模块
<dependency>
  	<groupId>com.aliyun.odps</groupId>
  	<artifactId>odps-spark-datasource_2.11</artifactId>
  	<version>3.3.3-public</version>
</dependency>

# Spark-1.x请依赖此模块
<dependency>
	<groupId>com.aliyun.odps</groupId>
	<artifactId>odps-spark-datasource_2.10</artifactId>
	<version>3.3.3-public</version>
</dependency>
```

<h1 id="4">4. 应用开发</h1>

本项目提供两个应用构建模版，用户可以基于此模版进行开发，最后统一构建整个项目后用生成的应用包即可直接提交到MaxCompute集群上运行Spark应用。

* [Spark-Examples for Spark-2.x](spark/spark-2.x/spark-examples/)
* [Spark-Examples for Spark-1.x](spark/spark-1.x/spark-examples/)
* [PySpark支持 for Spark-2.x](spark/spark-2.x/datasource/src/main/python)
* [PySpark支持 for Spark-1.x](spark/spark-1.x/datasource/src/main/python)

命令如下:
```
# 编译example
cd ${path to aliyun-cupid-sdk}/spark/spark-2.x/spark-examples/
mvn clean package

# 提交作业
cd $SPAKR_HOME
./bin/spark-submit --master yarn-cluster \
    --class com.aliyun.odps.spark.examples.SparkPi \
    ${path to aliyun-cupid-sdk}/spark/spark-2.x/spark-examples/target/spark-examples_2.11-3.3.3-public-shaded.jar
```
注意：要用shaded包提交

<h1 id="5">5. AliSpark Roadmap</h1>

目前AliSpark支持:
	
* Java/Scala 所有离线场景, Spark-SQL/RDD/GraphX/Mllib/PySpark等
* 能够读写 MaxCompute Table
* OSS 非结构化存储支持

后续将会支持:

* Streaming场景与MaxCompute深度集成 [TT、DataHub、Kafka]
* 交互式类需求 Spark-Shell Spark-SQL-Shell PySpark-Shell等
* Spark as Service需求 MaxCompute自研Client模式

<h1 id="6">6. AliSpark安全访问OSS</h1>

详细文档见 [OssStsToken](docs/ossStsToken.md)

具体Demo请参考:

* [Spark-1.x 安全OSS访问Demo](spark/spark-1.x/spark-examples/src/main/scala/com/aliyun/odps/spark/examples/oss/SparkUnstructuredDataCompute.scala)
* [Spark-2.x 安全OSS访问Demo](spark/spark-2.x/spark-examples/src/main/scala/com/aliyun/odps/spark/examples/oss/SparkUnstructuredDataCompute.scala)
