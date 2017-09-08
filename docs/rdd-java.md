# Load ODPS Data to rdd (java)
> See Detail Demo @ JavaOdpsTableReadWrite.java

## Read Non-Partitoned Table

```
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
JavaSparkContext ctx = new JavaSparkContext(sparkConf);
JavaOdpsOps javaOdpsOps = new JavaOdpsOps(ctx);
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
```

## Read Single-Column-Partitioned Table

```
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
JavaSparkContext ctx = new JavaSparkContext(sparkConf);
JavaOdpsOps javaOdpsOps = new JavaOdpsOps(ctx);
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
```

## Read Multi-Column-Partitioned Table

```
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
JavaSparkContext ctx = new JavaSparkContext(sparkConf);
JavaOdpsOps javaOdpsOps = new JavaOdpsOps(ctx);
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
```

## Read Multi-PartitionSpec for Table

```
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
JavaSparkContext ctx = new JavaSparkContext(sparkConf);
JavaOdpsOps javaOdpsOps = new JavaOdpsOps(ctx);
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
```

## Save rdd into Non-Partitioned Table

```
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
JavaSparkContext ctx = new JavaSparkContext(sparkConf);
JavaOdpsOps javaOdpsOps = new JavaOdpsOps(ctx);
VoidFunction3<Tuple2<String, String>, Record, TableSchema> transfer_0 =
        new VoidFunction3<Tuple2<String, String>, Record, TableSchema>() {
            @Override
            public void call(Tuple2<String, String> v1, Record v2, TableSchema v3) throws Exception {
                v2.set("id", v1._1());
                v2.set(1, v1._2());
            }
        };
javaOdpsOps.saveToTable(projectName, "cupid_wordcount_empty", rdd_0.rdd(), transfer_0, true);
```

## Save rdd into Partitioned table with single partition spec

```
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
JavaSparkContext ctx = new JavaSparkContext(sparkConf);
JavaOdpsOps javaOdpsOps = new JavaOdpsOps(ctx);
VoidFunction3<Tuple2<String, String>, Record, TableSchema> transfer_1 =
        new VoidFunction3<Tuple2<String, String>, Record, TableSchema>() {
            @Override
            public void call(Tuple2<String, String> v1, Record v2, TableSchema v3) throws Exception {
                v2.set("id", v1._1());
                v2.set("value", v1._2());
            }
        };
javaOdpsOps.saveToTable(projectName, "cupid_partition_table1", "pt1=test,pt2=dev", rdd_0.rdd(), transfer_1, true);
```

## Dynamic Save rdd into Partitioned table with multiple partition spec

```
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
JavaSparkContext ctx = new JavaSparkContext(sparkConf);
JavaOdpsOps javaOdpsOps = new JavaOdpsOps(ctx);
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
```
