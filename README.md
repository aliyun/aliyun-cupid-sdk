# Aliyun Cupid SDK

## Requirements

- Spark-2.1.0 (AliSpark Client)

## Introduction

This SDK supports:
* Interact with ODPS-Cupid platform
* Read/Write ODPS Table Data within ODPS Cluster (Best data localitiy)
* Cupid client-mode to support interactive communication with sparkContext

## Maven 

```
    <dependency>
        <groupId>com.aliyun.odps</groupId>
        <artifactId>cupid-core_2.11</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </dependency>

    <dependency>
        <groupId>com.aliyun.odps</groupId>
        <artifactId>cupid-datasource_2.11</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </dependency>

    <dependency>
        <groupId>com.aliyun.odps</groupId>
        <artifactId>cupid-client_2.11</artifactId>
        <version>1.0.0-SNAPSHOT</version>
    </dependency>
```

## Demonstration
> How to run Demo Examples under ${aliyun-cupid-sdk-ROOT}/examples

### Environment Setup

* Fetch AliSpark-PreBuild Binary `Contact leanken.lin@gmail.com`
* Extract the spark Tar package to wherever you want

```
# Content under extracted spark pacakge
.
..
|-- RELEASE
|-- bin
|-- conf
|-- examples
|-- jars
|-- python
|-- sbin
|-- yarn
|-- __spark_libs__.zip
```

* Set Env

```
# Set env in ~/.bashrc JAVA_HOME & SPARK_HOME
export JAVA_HOME=/path/to/jdk
export CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar
export PATH=$JAVA_HOME/bin:$PATH

export SPARK_HOME=/path/to/spark_extracted_package
export PATH=$SPARK_HOME/bin:$PATH
```

* Set spark-defaults.conf

```
# OdpsAccount Info Setting
spark.hadoop.odps.project.name =
spark.hadoop.odps.access.id = 
spark.hadoop.odps.access.key = 
spark.hadoop.odps.end.point = 

# Spark-shell Setting
spark.hadoop.odps.cupid.interaction.proxy.endpoint = ws://proxy.odps.aliyun-inc.com/interaction/
spark.driver.extraJavaOptions -Dscala.repl.reader=com.aliyun.odps.spark_repl.OdpsInteractiveReader -Dscala.usejavacp=true

# Cupid Longtime Job
# spark.hadoop.odps.cupid.engine.running.type = longtime
# spark.hadoop.odps.cupid.job.capability.duration.hours = 8640
# spark.hadoop.odps.moye.trackurl.dutation = 8640

# Sql catalog for odps table
spark.sql.catalogImplementation = odps
spark.hadoop.odps.cupid.bearer.token.enable = false

# Resource settings
spark.executor.instances = 1
spark.executor.cores = 2
spark.executor.memory = 6g
spark.driver.cores = 2
spark.driver.memory = 4g
spark.master = yarn-cluster
```

* Compile current project

```
git clone **current-project**
cd aliyun-cupid-sdk
mvn -T 1C clean install -DskipTests
```

### Case by Case

* [Load ODPS Data to rdd (scala)](docs/rdd-scala.md)
* [Load ODPS Data to rdd (java)](docs/rdd-java.md)
* [Load ODPS Data via SQL](docs/odps-sql.md)
* [Cupid Client Mode](docs/cupid-client.md)


## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
