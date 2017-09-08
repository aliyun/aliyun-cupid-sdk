#!/usr/bin/python
# -*- coding: utf-8 -*-

from odps.odps_sdk import OdpsOps
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, DataFrame

if __name__ == '__main__':
    """
    跑通pyspark on odps的例子需要满足以下条件
    1. SPARK on odps client已经下载完毕，并配置好SPARK_HOME以及对应的ODPS配置
    2. 找到odps.zip, 就在同一个目录下
    3. 下载好cupid-datasource_2.11-x.y.z.jar, 请使用最新版本的jar包
    4. 运行命令如下:
      spark-submit --master yarn-cluster --jars cupid-datasource_2.11-x.y.z.jar --py-files odps.zip odps_table_rw.py
    """

    conf = SparkConf().setAppName("odps_pyspark")
    sc = SparkContext(conf=conf)
    sql_context = SQLContext(sc)

    project_name = "cupid_testa1"
    in_table_name = "cupid_wordcount"
    out_table_name = "cupid_wordcount_py"

    # 读取odps table 返回Python DataFrame
    # OdpsOps.read_odps_table签名如下:
    #   sql_context: Python SQLContext
    #   project_name: odps project name(string)
    #   table_name: odps table name(string)
    #   partitions: partition definitions(array[string]), for example
    #       ["pt1='part1',pt2='part1'", "pt1='part1',pt2='part2'"], default []
    #   num_partition: spark partition(int), default 0
    normal_df = OdpsOps.read_odps_table(sql_context, project_name, in_table_name)

    # df的operation请参考 https://spark.apache.org/docs/1.5.1/api/python/pyspark.sql.html#pyspark.sql.DataFrame
    # 为了不让console打印太多，用了sample函数
    for i in normal_df.sample(False, 0.01).collect():
        print i
    print "Read normal odps table finished"

    # 将Python DataFrame写入odps table
    # 不接受其他类型参数，必须是Python DataFrame，并且schema必须与odps table一致
    # OdpsOps.write_odps_table签名如下:
    #   sql_context: Python SQLContext
    #   py_df: Python DataFrame
    #   project_name: odps project name(string)
    #   table_name: odps table name(string)
    #   partition_str: partition definitions(string), for example
    #       "pt1='part1',pt2='part3'", default ""
    #   is_overwrite: is_overwrite(boolean), default False
    OdpsOps.write_odps_table(sql_context, normal_df.sample(False, 0.001), project_name, out_table_name)
    print "Write normal odps table finished"


