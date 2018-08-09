* [1. aliyun-cupid-sdk简介](#1)
* [2. 项目编译须知](#2)
* [3. AliSpark环境准备](#3)
	+ [3.1 下载并解压spark包](#3.1)
	+ [3.2 设置环境变量](#3.2)
	+ [3.3 设置Spark-defaults.conf](#3.3)
* [4. 主要模块以及Maven依赖说明](#4)
* [5. 应用开发](#5)
* [6. AliSpark Roadmap](#6)
* [7. AliSpark安全访问OSS](#7)
* [8. AliSpark-1.x Quick-Start](#8)
* [9. AliSpark-2.x Quick-Start](#9)


<h1 id="1">1. aliyun-cupid-sdk简介</h1>

[MaxCompute Alispark](https://github.com/aliyun/aliyun-cupid-sdk) 是阿里云提供的Spark集成方案，可以与MaxCompute的生态数据无缝集成，也能完美的支持Spark社区大部分功能。aliyun-cupid-sdk则提供了集成所需的API说明以及相关功能Demo，用户可以基于项目提供的Spark-1.x以及Spark-2.x的example项目构建自己的应用，并且提交到MaxCompute集群上。

<h1 id="2">2. 项目编译须知</h1>

```
# 需要指定 public profile
mvn -X -T 1C clean package -Ppublic -DskipTests
```

<h1 id="3">3. AliSpark环境准备</h1>

<h2 id="3.1">3.1 下载并解压AliSpark包</h2>

下载 [Spark on MaxCompute](https://github.com/aliyun/aliyun-cupid-sdk) 安装包

<h2 id="3.2">3.2 设置环境变量</h2>

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

<h2 id="3.3">3.3 设置Spark-defaults.conf</h2>

在 **$SPARK_HOME/conf** 路径下存在spark-defaults.conf文件，需要在该文件中设置MaxCompute相关的账号信息后，才可以提交Spark任务到MaxCompute。默认配置内容如下，将空白部分根据实际的账号信息填上即可。

```
# OdpsAccount Info Setting
spark.hadoop.odps.project.name=
spark.hadoop.odps.access.id=
spark.hadoop.odps.access.key=
spark.hadoop.odps.end.point=
# spark.hadoop.odps.moye.trackurl.host=
# spark.hadoop.odps.cupid.webproxy.endpoint=

# Integrate with MaxCompute Projects and Tables
spark.sql.catalogImplementation=odps

# Cupid Longtime Job
# spark.hadoop.odps.cupid.engine.running.type=longtime
# spark.hadoop.odps.cupid.job.capability.duration.hours=8640
# spark.hadoop.odps.moye.trackurl.dutation=8640

spark.hadoop.odps.cupid.security.enable=true
spark.hadoop.odps.cupid.bearer.token.enable=true
```

<h1 id="4">4 主要模块以及Maven依赖说明</h1>

项目中Spark相关的依赖模块主要有三个:

* cupid-sdk **[开源应用接入MaxCompute SDK]**
* odps-spark-datasource_2.10 **[Spark-1.x MaxCompute数据访问API]**
* odps-spark-datasource_2.11 **[Spark-2.x MaxCompute数据访问API]**

```
# scope请设置为provided
# 另外如依赖spark-core spark-sql等模块，也请统一设置为provided
<dependency>
	<groupId>com.aliyun.odps</groupId>
	<artifactId>cupid-sdk</artifactId>
	<version>TBD</version>
	<scope>provided</scope>
</dependency>

# Spark-1.x请依赖此模块
<dependency>
	<groupId>com.aliyun.odps</groupId>
	<artifactId>odps-spark-datasource_2.10</artifactId>
	<version>TBD</version>
</dependency>

# Spark-2.x请依赖此模块
<dependency>
  	<groupId>com.aliyun.odps</groupId>
  	<artifactId>odps-spark-datasource_2.11</artifactId>
  	<version>TBD</version>
</dependency>
```

<h1 id="5">5. 应用开发</h1>

本项目提供两个应用构建模版，用户可以基于此模版进行开发，最后统一构建整个项目后用生成的应用包即可直接提交到MaxCompute集群上运行Spark应用。

* [Spark-Examples for Spark-1.x](spark/spark-1.x/spark-examples/)
* [Spark-Examples for Spark-2.x](spark/spark-2.x/spark-examples/)

<h1 id="6">6. AliSpark Roadmap</h1>

目前AliSpark支持:
	
* Java/Scala 所有离线场景, GraphX、Mllib、RDD、Spark-SQL, PySpark等
* 能够读写 MaxCompute Table
* OSS 非结构化存储支持

下个迭代上线支持:

* Streaming场景与MaxCompute深度集成 [TT、DataHub、Kafka]
* 交互式类需求 Spark-Shell Spark-SQL-Shell PySpark-Shell等
* Spark as Service需求 MaxCompute自研Client模式

<h1 id="7">7. AliSpark安全访问OSS</h1>

详细文档见 [OssStsToken](docs/ossStsToken.md)

具体Demo请参考:

* [Spark-1.x 安全OSS访问Demo](spark/spark-1.x/spark-examples/src/main/scala/com/aliyun/odps/spark/examples/oss/SparkUnstructuredDataCompute.scala)
* [Spark-2.x 安全OSS访问Demo](spark/spark-2.x/spark-examples/src/main/scala/com/aliyun/odps/spark/examples/oss/SparkUnstructuredDataCompute.scala)

<h1 id="8">8. Quick-Start AliSpark-1.x Demo</h1>

可通过 [Create-AliSpark-1.x-APP.sh](archetypes/Create-AliSpark-1.x-APP.sh) 脚本快速场景一个用于QuickStart的Maven Project

```

# Usage: sh Create-AliSpark-1.x-APP.sh <app_name> <target_path>
sh Create-AliSpark-1.x-APP.sh spark-1.x-demo /tmp/
cd /tmp/spark-1.x-demo
mvn clean package

# 冒烟测试 
# 1 利用编译出来的 shaded jar包
# 2 按照文档3.1所示 下载AliSpark客户端
# 3 填写文档3.3中 应该填写的相关配置项

# 执行spark-submit命令 如下
$SPARK_HOME/bin/spark-submit \
        --master yarn-cluster \
        --class SparkPi \
        /tmp/spark-1.x-demo/target/AliSpark-1.x-quickstart-1.0-SNAPSHOT-shaded.jar
```

<h1 id="9">9. Quick-Start AliSpark-2.x Demo</h1>

可通过 [Create-AliSpark-2.x-APP.sh](archetypes/Create-AliSpark-2.x-APP.sh) 脚本快速场景一个用于QuickStart的Maven Project

```

# Usage: sh Create-AliSpark-2.x-APP.sh <app_name> <target_path>
sh Create-AliSpark-2.x-APP.sh spark-2.x-demo /tmp/
cd /tmp/spark-2.x-demo
mvn clean package

# 冒烟测试 
# 1 利用编译出来的 shaded jar包
# 2 按照文档3.1所示 下载AliSpark客户端
# 3 填写文档3.3中 应该填写的相关配置项

# 执行spark-submit命令 如下
$SPARK_HOME/bin/spark-submit \
        --master yarn-cluster \
        --class SparkPi \
        /tmp/spark-2.x-demo/target/AliSpark-2.x-quickstart-1.0-SNAPSHOT-shaded.jar
```