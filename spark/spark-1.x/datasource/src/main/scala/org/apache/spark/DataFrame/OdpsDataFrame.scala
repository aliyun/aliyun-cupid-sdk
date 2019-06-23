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

package org.apache.spark.odps

import java.io.IOException

import com.aliyun.odps.cupid.CupidSession
import com.aliyun.odps.cupid.tools.Logging
import com.aliyun.odps.data.Record
import com.aliyun.odps.{OdpsType, TableSchema}
import org.apache.spark.SparkException
import org.apache.spark.rdd.{OdpsRDD, RDD}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.JavaConverters._

object OdpsDataFrame extends Logging {

  private final val partseperator = ","

  def readOdpsTableForR(sqlContext: SQLContext,
                        projectName: String,
                        tabelName: String,
                        partitions: Array[String],
                        numPartition: Int = 0): RDD[Row] = {
    readOdpsTable(sqlContext, projectName, tabelName, partitions, Array[String](), numPartition)
  }

  private def readOdpsTable(sqlContext: SQLContext,
                            projectName: String,
                            tabelName: String,
                            partitions: Array[String],
                            columns: Array[String],
                            numSplits: Int): RDD[Row] = {
    val odpsOps = OdpsOps(sqlContext.sparkContext)
    lazy val date = new java.sql.Date(System.currentTimeMillis())
    val odpsRdd: RDD[Row] = odpsOps.readTable(projectName, tabelName, partitions, columns,
      (record: Record, schema: TableSchema) => {
        Row.fromSeq(schema.getColumns().asScala.map(
          (col: com.aliyun.odps.Column) => {
            val colname: String = col.getName()
            val coltype: OdpsType = col.getType()
            coltype match {
              case OdpsType.BIGINT => record.getBigint(colname)
              case OdpsType.DATETIME => {
                Option(record.getDatetime(colname)).map(
                  time => {
                    date.setTime(time.getTime);
                    date
                  }).getOrElse(null)
              }
              case OdpsType.STRING => record.getString(colname)
              case OdpsType.BOOLEAN => record.getBoolean(colname)
              case OdpsType.DOUBLE => record.getDouble(colname)
              case _ => throw new IOException("unsupported schema type: " + coltype)
            }
          }
        ))
      }, numSplits)
    odpsRdd
  }

  private def OdpsRDD2DF(sqlContext: SQLContext, odpsRdd: RDD[Row], columnNames: Array[String]): DataFrame = {
    if (odpsRdd.isInstanceOf[OdpsRDD[Row]]) {
      val rdd = odpsRdd.asInstanceOf[OdpsRDD[Row]]
      //val sqlContext = createSQLContext(jsc)
      val projectName = rdd.project
      val tableName = rdd.table
      val odpsSchema = CupidSession.get.odps().tables().get(projectName, tableName).getSchema()
      val columns = if (columnNames.isEmpty) {
        odpsSchema.getColumns().asScala.toList
      } else {
        columnNames.map(odpsSchema.getColumn(_)).toList
      }
      val schema = StructType(columns.map(
          (col: com.aliyun.odps.Column) => {
            val colname: String = col.getName()
            val coltype: OdpsType = col.getType()
            coltype match {
              case OdpsType.BIGINT => StructField(colname, LongType, true)
              case OdpsType.DATETIME => StructField(colname, DateType, true)
              case OdpsType.STRING => StructField(colname, StringType, true)
              case OdpsType.BOOLEAN => StructField(colname, BooleanType, true)
              case OdpsType.DOUBLE => StructField(colname, DoubleType, true)
              case _ => throw new IOException("unsupported schema type: " + coltype)
            }
          }
        ))

      // this is used to implicitly convert an RDD to a DataFrame.
      sqlContext.createDataFrame(rdd, schema)
    } else {
      throw new SparkException(s"Not InstanceOf OdpsRDD[Row]] failed!")
    }
  }

  /**
   * convert an odpsdf to a odpsRDD[row],then convert save as an odps table
   */
  private def saveRdd2ODPS(sqlContext: SQLContext,
                           odpsRdd: RDD[Row],
                           projectName: String,
                           tableName: String,
                           partitionstr: String,
                           isOverWrite: Boolean = false): Unit = {
    val odpsOps = OdpsOps(sqlContext.sparkContext)
    logInfo("before partition in saveRdd2ODPS:" + partitionstr)
    var partitionOut = partitionstr
    if (partitionstr != null && partitionstr != "") {
      partitionOut = partitionstr.replaceAll("/", partseperator)
    }
    logInfo("partition in saveRdd2ODPS:" + partitionOut)
    def transeferDate(date: java.sql.Date): java.util.Date = {
      Option(date).map(d => {
        new java.util.Date(d.getTime)
      }).getOrElse(null)
    }
    odpsOps.saveToTable(projectName, tableName, partitionOut, odpsRdd,
      (row: Row, record: Record, schema: TableSchema) => {
        val cols = schema.getColumns().asScala
        (0 to cols.size - 1).map(
          (index: Int) => {
            val col: com.aliyun.odps.Column = cols(index)
            val colname: String = col.getName()
            val coltype: OdpsType = col.getType()
            coltype match {
              case OdpsType.BIGINT => record.setBigint(index, row.getLong(index))
              case OdpsType.DATETIME => record.setDatetime(index, transeferDate(row.getDate(index)))
              case OdpsType.STRING => record.setString(index, row.getString(index))
              case OdpsType.BOOLEAN => record.setBoolean(index, row.getBoolean(index))
              case OdpsType.DOUBLE => record.setDouble(index, row.getDouble(index))
              case _ => throw new IOException("unsupported schema type: " + coltype)
            }
          }
        )
        ()
      }, isOverWrite)
  }

  def createOdpsDataFrame(sqlContext: SQLContext,
                          projectName: String,
                          tableName: String,
                          partitions: Array[String] = Array[String](),
                          numSplits: Int = 0): DataFrame = {
    createOdpsDataFrame(sqlContext, projectName, tableName, partitions, Array[String](), numSplits)
  }


  /**
   * This method is used to create dataframe from projectname.tablename
   * partitions optionally
   *
   * @param sqlContext sparkContext
   * @param projectName
   * @param tableName
   * @param partitions an array that holds partitions,
   *                    new Array[String]() when table not parted
   * @param columnNames columns of the table
   * @param numSplits splits number for RDD to execute, 0 by default
   * @return the dataframe created
   **/
  def createOdpsDataFrame(sqlContext: SQLContext,
                          projectName: String,
                          tableName: String,
                          partitions: Array[String],
                          columnNames: Array[String],
                          numSplits: Int): DataFrame = {
    val odpsRdd: RDD[Row] = readOdpsTable(sqlContext, projectName, tableName, partitions, columnNames, numSplits)
    val odpsdf: DataFrame = OdpsRDD2DF(sqlContext, odpsRdd, columnNames)
    odpsdf
  }

  //type is double,because R does not have Integer type
  def createOdpsDataFrameForR(sqlContext: SQLContext,
                              projectName: String,
                              tableName: String,
                              partitions: Array[String] = Array[String](),
                              numPartition: Double = 0): DataFrame = {
    createOdpsDataFrame(sqlContext,
      projectName,
      tableName,
      partitions,
      numPartition.toInt)
  }

  /**
   * This method is used to save dataframe to a projectname.tablename
   * partitions optionally
   * @param sqlContext sqlContext
   * @param df DataFrame
   * @param projectName
   * @param tableName
   * @param  partitionStr a string that holds partitions whith commas seperated,
   *                      empty string if table not parted
   * @param isOverWrite whether to overwite when writing a table,
   *                    default false
   * @return unit
   **/
  def saveODPS(sqlContext: SQLContext,
               df: DataFrame,
               projectName: String,
               tableName: String,
               partitionStr: String = "",
               isOverWrite: Boolean = false): Unit = {
    logInfo("converting dataframe to RDD")
    val rdd: RDD[Row] = df.javaRDD
    logInfo("saveODPS projectName:" + projectName + " tableName:" + tableName)
    saveRdd2ODPS(sqlContext,
      rdd,
      projectName,
      tableName,
      partitionStr,
      isOverWrite)
  }

  def saveODPSForR(sqlContext: SQLContext,
                   df: DataFrame,
                   projectName: String,
                   tableName: String,
                   partitionStr: String = "",
                   isOverWrite: Boolean = false): Unit = {
    saveODPS(sqlContext, df, projectName, tableName, partitionStr, isOverWrite)
  }
}
