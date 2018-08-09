#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
    OdpsOps.write_odps_table(sql_context, normal_df.sample(False, 0.001), "cupid_testa1", "cupid_wordcount_py")
    print "Write normal odps table finished"

    # Partition ODPS Table Write
    # Params: partition_str "pt1='part3',pt2='part4'"
    # Params: is_overwrite [default value]
    OdpsOps.write_odps_table(sql_context, partition_df.sample(False, 0.001), "cupid_testa1", "cupid_partition_table1_py",
                             "pt1='part1',pt2='part3'")
    print "Write partition odps table finished"

