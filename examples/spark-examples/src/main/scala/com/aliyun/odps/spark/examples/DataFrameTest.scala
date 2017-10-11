package com.aliyun.odps.spark.examples

import org.apache.spark.odps.OdpsDataFrame
import org.apache.spark.sql.SparkSession

object DataFrameTest {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("DataFrameTest")
      .getOrCreate()

    try {
      val projectName = spark.sparkContext.getConf.get("odps.project.name")
      var tableName = "cupid_wordcount"
      val partitions = Array[String]()
      val cupid_wordcount_df =
        OdpsDataFrame.createOdpsDataFrame(spark.sqlContext, projectName, tableName, partitions, 2)
      val count1 = 10000
      val isOverWrite = true

      println("read DataFrame starts.")
      val count2 = cupid_wordcount_df.count()
      println(s"count for table $tableName + :" + count2)
      assert(count1 == count2)
      println("read DataFrame successful!\n")

      println("testOdpsSave starts")
      cupid_wordcount_df.printSchema()
      val destTableName = "cupid_wordcount_backup"
      OdpsDataFrame.saveODPS(spark.sqlContext, cupid_wordcount_df, projectName, destTableName, "", isOverWrite)
      val destDF = OdpsDataFrame.createOdpsDataFrame(spark.sqlContext, projectName, destTableName, partitions)
      val count3 = destDF.count()
      assert(count1 == count3)
      println("testOdpsSave ends")

      println("testSQL starts")
      val window_function_source_df =
        OdpsDataFrame.createOdpsDataFrame(spark.sqlContext, projectName, "window_function_source", partitions)
      window_function_source_df.createOrReplaceTempView("window_function_source")
      val small_df = spark.sql("select * from window_function_source limit 10")
      assert(small_df.count() == 10)
      println("testSQL ends")
    } catch {
      case ex: Exception => {
        throw ex
      }
    } finally {
      spark.stop
    }
  }
}
