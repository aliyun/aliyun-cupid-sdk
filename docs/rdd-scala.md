# Load ODPS Data to rdd (scala)
> See Detail Demo @ OdpsTableReadWrite.scala

## Read Non-Partitoned Table

```
/**
  * read from normal table via rdd api
  * desc cupid_wordcount;
  * +------------------------------------------------------------------------------------+
  * | Field           | Type       | Label | Comment                                     |
  * +------------------------------------------------------------------------------------+
  * | id              | string     |       |                                             |
  * | value           | string     |       |                                             |
  * +------------------------------------------------------------------------------------+
  */
val sc = new SparkContext(conf)
val odpsOps = new OdpsOps(sc)
val rdd_0 = odpsOps.readTable(
  projectName,
  "cupid_wordcount",
  (r: Record, schema: TableSchema) => (r.getString(0), r.getString(1))
)
```

## Read Single-Column-Partitioned Table

```
/**
  * read from single partition column table via rdd api
  * desc dftest_single_parted;
  * +------------------------------------------------------------------------------------+
  * | Field           | Type       | Label | Comment                                     |
  * +------------------------------------------------------------------------------------+
  * | id              | string     |       |                                             |
  * | value           | string     |       |                                             |
  * +------------------------------------------------------------------------------------+
  * | Partition Columns:                                                                 |
  * +------------------------------------------------------------------------------------+
  * | pt              | string     |                                                     |
  * +------------------------------------------------------------------------------------+
  */
val sc = new SparkContext(conf)
val odpsOps = new OdpsOps(sc)
val rdd_1 = odpsOps.readTable(
  projectName,
  "dftest_single_parted",
  Array("pt=20160101"),
  (r: Record, schema: TableSchema) => (r.getString(0), r.getString(1), r.getString("pt"))
)
```

## Read Multi-Column-Partitioned Table

```
/**
  * read from multi partition column table via rdd api
  * desc dftest_parted;
  * +------------------------------------------------------------------------------------+
  * | Field           | Type       | Label | Comment                                     |
  * +------------------------------------------------------------------------------------+
  * | id              | string     |       |                                             |
  * | value           | string     |       |                                             |
  * +------------------------------------------------------------------------------------+
  * | Partition Columns:                                                                 |
  * +------------------------------------------------------------------------------------+
  * | pt              | string     |                                                     |
  * | hour            | string     |                                                     |
  * +------------------------------------------------------------------------------------+
  */
val sc = new SparkContext(conf)
val odpsOps = new OdpsOps(sc)
val rdd_2 = odpsOps.readTable(
  projectName,
  "dftest_parted",
  Array("pt=20160101,hour=12"),
  (r: Record, schema: TableSchema) => (r.getString(0), r.getString(1), r.getString("pt"), r.getString(3))
)
```

## Read Multi-PartitionSpec for Table

```
/**
  * read with multi partitionSpec definition via rdd api
  * desc cupid_partition_table1;
  * +------------------------------------------------------------------------------------+
  * | Field           | Type       | Label | Comment                                     |
  * +------------------------------------------------------------------------------------+
  * | id              | string     |       |                                             |
  * | value           | string     |       |                                             |
  * +------------------------------------------------------------------------------------+
  * | Partition Columns:                                                                 |
  * +------------------------------------------------------------------------------------+
  * | pt1             | string     |                                                     |
  * | pt2             | string     |                                                     |
  * +------------------------------------------------------------------------------------+
  */
val sc = new SparkContext(conf)
val odpsOps = new OdpsOps(sc)
val rdd_3 = odpsOps.readTable(
  projectName,
  "cupid_partition_table1",
  Array("pt1=part1,pt2=part1", "pt1=part1,pt2=part2", "pt1=part2,pt2=part3"),
  (r: Record, schema: TableSchema) => (r.getString(0), r.getString(1), r.getString("pt1"), r.getString("pt2"))
)
```

## Save rdd into Non-Partitioned Table

```
/**
  * save rdd into normal table
  * desc cupid_wordcount_empty;
  * +------------------------------------------------------------------------------------+
  * | Field           | Type       | Label | Comment                                     |
  * +------------------------------------------------------------------------------------+
  * | id              | string     |       |                                             |
  * | value           | string     |       |                                             |
  * +------------------------------------------------------------------------------------+
  */
val sc = new SparkContext(conf)
val odpsOps = new OdpsOps(sc)
val transfer_0 = (v: Tuple2[String, String], record: Record, schema: TableSchema) => {
  record.set("id", v._1)
  record.set(1, v._2)
}
odpsOps.saveToTable(projectName, "cupid_wordcount_empty", rdd_0, transfer_0, true)
```

## Save rdd into Partitioned table with single partition spec

```
/**
  * save rdd into partition table with single partition spec
  * desc cupid_partition_table1;
  * +------------------------------------------------------------------------------------+
  * | Field           | Type       | Label | Comment                                     |
  * +------------------------------------------------------------------------------------+
  * | id              | string     |       |                                             |
  * | value           | string     |       |                                             |
  * +------------------------------------------------------------------------------------+
  * | Partition Columns:                                                                 |
  * +------------------------------------------------------------------------------------+
  * | pt1             | string     |                                                     |
  * | pt2             | string     |                                                     |
  * +------------------------------------------------------------------------------------+
  */
val sc = new SparkContext(conf)
val odpsOps = new OdpsOps(sc)
val transfer_1 = (v: Tuple2[String, String], record: Record, schema: TableSchema) => {
  record.set("id", v._1)
  record.set("value", v._2)
}
odpsOps.saveToTable(projectName, "cupid_partition_table1", "pt1=test,pt2=dev", rdd_0, transfer_1, true)
```

## Dynamic Save rdd into Partitioned table with multiple partition spec

```
/**
  * dynamic save rdd into partition table with multiple partition spec
  * desc cupid_partition_table1;
  * +------------------------------------------------------------------------------------+
  * | Field           | Type       | Label | Comment                                     |
  * +------------------------------------------------------------------------------------+
  * | id              | string     |       |                                             |
  * | value           | string     |       |                                             |
  * +------------------------------------------------------------------------------------+
  * | Partition Columns:                                                                 |
  * +------------------------------------------------------------------------------------+
  * | pt1             | string     |                                                     |
  * | pt2             | string     |                                                     |
  * +------------------------------------------------------------------------------------+
  */
val sc = new SparkContext(conf)
val odpsOps = new OdpsOps(sc)
val transfer_2 = (v: Tuple2[String, String], record: Record, part: PartitionSpec, schema: TableSchema) => {
  record.set("id", v._1)
  record.set("value", v._2)

  val pt1_value = if (new Random().nextInt(10) % 2 == 0) "even" else "odd"
  val pt2_value = if (new Random().nextInt(10) % 2 == 0) "even" else "odd"
  part.set("pt1", pt1_value)
  part.set("pt2", pt2_value)
}
odpsOps.saveToTableForMultiPartition(projectName, "cupid_partition_table1", rdd_0, transfer_2, true)
```
