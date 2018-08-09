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

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, DataFrame
from py4j.java_gateway import java_import


class OdpsOps(object):
    """
    This class provided read/write odps helper for cupid python end user to operate on odps table
    """

    # global var
    odps_read_func = None
    odps_write_func = None
    is_initial = False

    def __init__(self):
        pass

    @staticmethod
    def ensure_initialized(sql_context):
        # Get SparkContext from SQLContext
        sc = sql_context._sc
        java_import(sc._jvm, 'org.apache.spark.odps.OdpsDataFrame')

        # func pointer
        OdpsOps.odps_read_func = sc._jvm.OdpsDataFrame.createOdpsDataFrame
        OdpsOps.odps_write_func = sc._jvm.OdpsDataFrame.saveODPS
        OdpsOps.is_initial = True


    @staticmethod
    def read_odps_table(sql_context, project_name, table_name, partitions=[], num_partition=0):
        if not OdpsOps.is_initial:
            OdpsOps.ensure_initialized(sql_context)

        partitions_len = len(partitions)
        # create empty array <> partitions
        sc = sql_context._sc
        string_class = sc._jvm.java.lang.String
        string_array = sc._gateway.new_array(string_class, partitions_len)

        for index in range(0, partitions_len):
            string_array[index] = partitions[index]

        # java dataframe from py4j
        jdf = OdpsOps.odps_read_func(sql_context._ssql_ctx, project_name, table_name, string_array, num_partition)

        # transform into python df
        df = DataFrame(jdf, sql_context)    
        return df

    @staticmethod
    def write_odps_table(sql_context, py_df, project_name, table_name, partition_str="", is_overwrite=False):
        if not OdpsOps.is_initial:
            OdpsOps.ensure_initialized(sql_context)

        OdpsOps.odps_write_func(sql_context._ssql_ctx, py_df._jdf, project_name, table_name, partition_str, is_overwrite)
