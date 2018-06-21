#!/usr/bin/python
# -*- coding: utf-8 -*-

from odps.odps_sdk import OdpsOps
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, DataFrame

if __name__ == '__main__':
    """
    跑通pyspark on odps的例子需要满足以下条件
    1. SPARK on odps client已经下载完毕，并配置好SPARK_HOME以及对应的ODPS配置
    2. 找到odps.zip, 就在同一目录下
    3. 下载好odps-spark-datasource-x.y.z.jar, 请使用最新版本的jar包
    4. 运行命令如下:
      spark-submit --master yarn-cluster --jars odps-spark-datasource-x.y.z.jar --py-files odps.zip odps_partition_table_rw.py
    """

    conf = SparkConf().setAppName("odps_pyspark")
    sc = SparkContext(conf=conf)
    sql_context = SQLContext(sc)

    project_name = "cupid_testa1"
    in_table_name = "cupid_partition_table1"
    out_table_name = "cupid_partition_table1_py"

    # 读取odps partition table 返回Python DataFrame
    # OdpsOps.read_odps_table签名如下:
    #   sql_context: Python SQLContext
    #   project_name: odps project name(string)
    #   table_name: odps table name(string)
    #   partitions: partition definitions(array[string]), for example
    #       ["pt1='part1',pt2='part1'", "pt1='part1',pt2='part2'"], default []
    #   num_partition: spark partition(int), default 0
    partition_df = OdpsOps.read_odps_table(sql_context, project_name, in_table_name,
                                           partitions=["pt1='part1',pt2='part1'", "pt1='part1',pt2='part2'"])

    # df的operation请参考 https://spark.apache.org/docs/1.5.1/api/python/pyspark.sql.html#pyspark.sql.DataFrame
    # 为了不让console打印太多，用了sample函数
    for i in partition_df.sample(False, 0.01).collect():
        print i
    print "Read partition odps table finished"

    # 将Python DataFrame写入odps partition table
    # 不接受其他类型参数，必须是Python DataFrame，并且schema必须与odps table一致
    # OdpsOps.write_odps_table签名如下:
    #   sql_context: Python SQLContext
    #   py_df: Python DataFrame
    #   project_name: odps project name(string)
    #   table_name: odps table name(string)
    #   partition_str: partition definitions(string), for example
    #       "pt1='part1',pt2='part3'", default ""
    #   is_overwrite: is_overwrite(boolean), default False
    OdpsOps.write_odps_table(sql_context, partition_df.sample(False, 0.001), project_name, out_table_name,
                             partition_str="pt1='part1',pt2='part3'", is_overwrite=True)
    print "Write partition odps table finished"
