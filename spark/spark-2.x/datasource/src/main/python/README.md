# SPARK on cupid PySpark 支持

## 原理
1. 已经存在Java／Scala版 Odps Spark Plugin Support
2. 通过py4j去反射调用已有的org.apache.spark.odps.OdpsDataFrame的相关方法
3. 通过OdpsDataFrame的以下的两个方法，就可以直接读取以及写入ODPS表了; createOdpsDataFrame、saveODPS
4. spark-submit在启动driver的时候，把plugin的jar加载进来，在通过py4j去反射调用
5. 需要同时把plugin的jar以及python版本的sdk加入到spark-submit启动参数里

## 需要文件
1. http://repo.aliyun.com/download/odps-spark-datasource-1.0.3.jar  (这个是最新的plugin jar包地址)
2. odps_sdk.py

## 运行命令

> 比如，以下我们写了一个spark app，是用pyspark写的

```

from odps_sdk import OdpsOps
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, DataFrame

if __name__ == '__main__':
    conf = SparkConf().setAppName("pyspark_odps_test")
    sc = SparkContext(conf=conf)
    sql_context = SQLContext(sc)

    # Normal ODPS Table READ
    # Params: partitions [default value]
    # Params: num_partition [default value]
    normal_df = OdpsOps.read_odps_table(sql_context, "cupid_testa1", "cupid_wordcount")
    for i in normal_df.sample(False, 0.01).collect():
        print i
    print "Read normal odps table finished"

    # Partition ODPS Table READ
    # Params: partitions ["pt1='part1',pt2='part1'", "pt1='part1',pt2='part2'"]
    # Params: num_partition [default value]
    partition_df = OdpsOps.read_odps_table(sql_context, "cupid_testa1", "cupid_partition_table1",
                                 partitions=["pt1='part1',pt2='part1'", "pt1='part1',pt2='part2'"])
    for i in partition_df.sample(False, 0.01).collect():
        print i
    print "Read partition odps table finished"

    # Normal ODPS Table Write
    # Params: partition_str [default value]
    # Params: is_overwrite [default value]
    OdpsOps.write_odps_table(sql_context, normal_df.sample(False, 0.001), "cupid_testa1", "cupid_wordcount_res")
    print "Write normal odps table finished"

    # Partition ODPS Table Write
    # Params: partition_str "pt1='part3',pt2='part4'"
    # Params: is_overwrite [default value]
    OdpsOps.write_odps_table(sql_context, partition_df.sample(False, 0.001), "cupid_testa1", "cupid_partition_table1",
                             "pt1='part1',pt2='part3'")
    print "Write partition odps table finished"

```

此时，你应该有以下的这些准备:
1. spark on odps CLIENT
2. 设置好SPARK_HOME以及对应的PATH
3. 在spark-defaults.conf里配置好相应的odps配置
4. 下载好odps-spark-datasource-1.0.3.jar
5. 下载好odps_sdk.py
6. 准备好自己的spark app，也就是上面的example.py

### Local模式运行

> 运行命令如下:

```

spark-submit --master local[4] --driver-class-path odps-spark-datasource-1.0.3.jar --py-files odps_sdk.py example.py

PS. local模式运行一定要用driver-class-path，而不能用--jars, 所有文件均在当前路径

```

### Cluster模式运行

```

spark-submit --master yarn-cluster --jars odps-spark-datasource-1.0.3.jar --py-files odps_sdk.py example.py


```
