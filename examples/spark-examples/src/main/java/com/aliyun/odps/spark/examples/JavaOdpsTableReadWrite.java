/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.spark.examples;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaOdpsOps;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction3;
import org.apache.spark.api.java.function.VoidFunction4;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.util.Random;


/**
 * 1. build aliyun-cupid-sdk
 * 2. properly set spark.defaults.conf
 * 3. bin/spark-submit --master yarn-cluster --class com.aliyun.odps.spark.examples.JavaOdpsTableReadWrite \
 * /path/to/aliyun-cupid-sdk/examples/spark-examples/target/spark-examples_2.11-1.0.0-SNAPSHOT-shaded.jar
 */
public class JavaOdpsTableReadWrite {

    /**
     * This Demo represent the ODPS Table Read/Write Usage in following scenarios
     * This Demo will fail if you don't have such Table inside such Project
     * 1. read from normal table via rdd api
     * 2. read from single partition column table via rdd api
     * 3. read from multi partition column table via rdd api
     * 4. read with multi partitionSpec definition via rdd api
     * 5. save rdd into normal table
     * 6. save rdd into partition table with single partition spec
     * 7. dynamic save rdd into partition table with multiple partition spec
     */
    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("JavaOdpsTableReadWrite");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaOdpsOps javaOdpsOps = new JavaOdpsOps(ctx);
        String projectName = ctx.getConf().get("odps.project.name");

        /**
         *  read from normal table via rdd api
         *  desc cupid_wordcount;
         *  +------------------------------------------------------------------------------------+
         *  | Field           | Type       | Label | Comment                                     |
         *  +------------------------------------------------------------------------------------+
         *  | id              | string     |       |                                             |
         *  | value           | string     |       |                                             |
         *  +------------------------------------------------------------------------------------+
         */
        JavaRDD<Tuple2<String, String>> rdd_0 = javaOdpsOps.readTable(
                projectName,
                "cupid_wordcount",
                new Function2<Record, TableSchema, Tuple2<String, String>>() {
                    public Tuple2<String, String> call(Record v1, TableSchema v2) throws Exception {
                        return new Tuple2<String, String>(v1.getString(0), v1.getString(1));
                    }
                },
                0
        );

        System.out.println("rdd_0 count: " + rdd_0.count());

        /**
         *  read from single partition column table via rdd api
         *  desc dftest_single_parted;
         *  +------------------------------------------------------------------------------------+
         *  | Field           | Type       | Label | Comment                                     |
         *  +------------------------------------------------------------------------------------+
         *  | id              | string     |       |                                             |
         *  | value           | string     |       |                                             |
         *  +------------------------------------------------------------------------------------+
         *  | Partition Columns:                                                                 |
         *  +------------------------------------------------------------------------------------+
         *  | pt              | string     |                                                     |
         *  +------------------------------------------------------------------------------------+
         */
        JavaRDD<Tuple3<String, String, String>> rdd_1 = javaOdpsOps.readTable(
                projectName,
                "dftest_single_parted",
                "pt=20160101",
                new Function2<Record, TableSchema, Tuple3<String, String, String>>() {
                    public Tuple3<String, String, String> call(Record v1, TableSchema v2) throws Exception {
                        return new Tuple3<String, String, String>(v1.getString(0), v1.getString(1), v1.getString("pt"));
                    }
                },
                0
        );

        System.out.println("rdd_1 count: " + rdd_1.count());

        /**
         *  read from multi partition column table via rdd api
         *  desc dftest_parted;
         *  +------------------------------------------------------------------------------------+
         *  | Field           | Type       | Label | Comment                                     |
         *  +------------------------------------------------------------------------------------+
         *  | id              | string     |       |                                             |
         *  | value           | string     |       |                                             |
         *  +------------------------------------------------------------------------------------+
         *  | Partition Columns:                                                                 |
         *  +------------------------------------------------------------------------------------+
         *  | pt              | string     |                                                     |
         *  | hour            | string     |                                                     |
         *  +------------------------------------------------------------------------------------+
         */
        JavaRDD<Tuple4<String, String, String, String>> rdd_2 = javaOdpsOps.readTable(
                projectName,
                "dftest_parted",
                "pt=20160101,hour=12",
                new Function2<Record, TableSchema, Tuple4<String, String, String, String>>() {
                    public Tuple4<String, String, String, String> call(Record v1, TableSchema v2) throws Exception {
                        return new Tuple4<String, String, String, String>(v1.getString(0), v1.getString(1), v1.getString("pt"), v1.getString(3));
                    }
                },
                0
        );

        System.out.println("rdd_2 count: " + rdd_2.count());

        /**
         *  read with multi partitionSpec definition via rdd api
         *  desc cupid_partition_table1;
         *  +------------------------------------------------------------------------------------+
         *  | Field           | Type       | Label | Comment                                     |
         *  +------------------------------------------------------------------------------------+
         *  | id              | string     |       |                                             |
         *  | value           | string     |       |                                             |
         *  +------------------------------------------------------------------------------------+
         *  | Partition Columns:                                                                 |
         *  +------------------------------------------------------------------------------------+
         *  | pt1             | string     |                                                     |
         *  | pt2             | string     |                                                     |
         *  +------------------------------------------------------------------------------------+
         */
        JavaRDD<Tuple4<String, String, String, String>> rdd_3 = javaOdpsOps.readTable(
                projectName,
                "cupid_partition_table1",
                new String[]{"pt1=part1,pt2=part1", "pt1=part1,pt2=part2", "pt1=part2,pt2=part3"},
                new Function2<Record, TableSchema, Tuple4<String, String, String, String>>() {
                    public Tuple4<String, String, String, String> call(Record v1, TableSchema v2) throws Exception {
                        return new Tuple4<String, String, String, String>(v1.getString(0), v1.getString(1), v1.getString("pt1"), v1.getString("pt2"));
                    }
                },
                0
        );

        System.out.println("rdd_3 count: " + rdd_3.count());

        /**
         *  save rdd into normal table
         *  desc cupid_wordcount_empty;
         *  +------------------------------------------------------------------------------------+
         *  | Field           | Type       | Label | Comment                                     |
         *  +------------------------------------------------------------------------------------+
         *  | id              | string     |       |                                             |
         *  | value           | string     |       |                                             |
         *  +------------------------------------------------------------------------------------+
         */
        VoidFunction3<Tuple2<String, String>, Record, TableSchema> transfer_0 =
                new VoidFunction3<Tuple2<String, String>, Record, TableSchema>() {
                    @Override
                    public void call(Tuple2<String, String> v1, Record v2, TableSchema v3) throws Exception {
                        v2.set("id", v1._1());
                        v2.set(1, v1._2());
                    }
                };
        javaOdpsOps.saveToTable(projectName, "cupid_wordcount_empty", rdd_0.rdd(), transfer_0, true);

        /**
         *  save rdd into partition table with single partition spec
         *  desc cupid_partition_table1;
         *  +------------------------------------------------------------------------------------+
         *  | Field           | Type       | Label | Comment                                     |
         *  +------------------------------------------------------------------------------------+
         *  | id              | string     |       |                                             |
         *  | value           | string     |       |                                             |
         *  +------------------------------------------------------------------------------------+
         *  | Partition Columns:                                                                 |
         *  +------------------------------------------------------------------------------------+
         *  | pt1             | string     |                                                     |
         *  | pt2             | string     |                                                     |
         *  +------------------------------------------------------------------------------------+
         */
        VoidFunction3<Tuple2<String, String>, Record, TableSchema> transfer_1 =
                new VoidFunction3<Tuple2<String, String>, Record, TableSchema>() {
                    @Override
                    public void call(Tuple2<String, String> v1, Record v2, TableSchema v3) throws Exception {
                        v2.set("id", v1._1());
                        v2.set("value", v1._2());
                    }
                };
        javaOdpsOps.saveToTable(projectName, "cupid_partition_table1", "pt1=test,pt2=dev", rdd_0.rdd(), transfer_1, true);

        /**
         *  dynamic save rdd into partition table with multiple partition spec
         *  desc cupid_partition_table1;
         *  +------------------------------------------------------------------------------------+
         *  | Field           | Type       | Label | Comment                                     |
         *  +------------------------------------------------------------------------------------+
         *  | id              | string     |       |                                             |
         *  | value           | string     |       |                                             |
         *  +------------------------------------------------------------------------------------+
         *  | Partition Columns:                                                                 |
         *  +------------------------------------------------------------------------------------+
         *  | pt1             | string     |                                                     |
         *  | pt2             | string     |                                                     |
         *  +------------------------------------------------------------------------------------+
         */
        VoidFunction4<Tuple2<String, String>, Record, PartitionSpec, TableSchema> transfer_2 =
                new VoidFunction4<Tuple2<String, String>, Record, PartitionSpec, TableSchema>() {
                    @Override
                    public void call(Tuple2<String, String> v1, Record v2, PartitionSpec v3, TableSchema v4) throws Exception {
                        v2.set("id", v1._1());
                        v2.set("value", v1._2());

                        String pt1_value = new Random().nextInt(10) % 2 == 0 ? "even" : "odd";
                        String pt2_value = new Random().nextInt(10) % 2 == 0 ? "even" : "odd";
                        v3.set("pt1", pt1_value);
                        v3.set("pt2", pt2_value);
                    }
                };
        javaOdpsOps.saveToTableForMultiPartition(projectName, "cupid_partition_table1", rdd_0.rdd(), transfer_2, true);
    }
}
