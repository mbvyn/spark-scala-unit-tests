package org.unit.batch

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.unit.SparkSessionTestWrapper

import java.sql.{Date, Timestamp}

/**
 * Parent class for all batch tests.
 *
 * It contains common data for the batch tests.
 *
 * See [[org.unit.batch.StraightforwardTest]] and [[org.unit.batch.ModularTest]] for more details.
 */
class BatchTest extends SparkSessionTestWrapper {
  // Data that should be created after select statement
  val interestedData = Seq(
    Row(2762, Timestamp.valueOf("2017-07-01 00:01:09"), "W21474"),
    Row(2763, Timestamp.valueOf("2017-07-01 00:01:24"), "W22042"),
    Row(690, Timestamp.valueOf("2017-07-02 00:01:45"), "W01182"),
    Row(134, Timestamp.valueOf("2017-07-02 00:01:46"), "W22829"),
    Row(587, Timestamp.valueOf("2017-07-02 00:02:05"), "W22223"),
    Row(586, Timestamp.valueOf("2017-07-02 00:02:06"), "W00191")
  )
  val interestedColumnSchema: StructType = StructType(Seq(
    StructField("Duration", IntegerType, nullable = true),
    StructField("Start date", TimestampType, nullable = true),
    StructField("Bike number", StringType, nullable = true),
  ))
  val interestedDF: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(interestedData), interestedColumnSchema)

  // Data that should be created after window statement
  val rankedData = Seq(
    Row(Date.valueOf("2017-07-01"), "W22042", 2763L, 1),
    Row(Date.valueOf("2017-07-01"), "W21474", 2762L, 2),
    Row(Date.valueOf("2017-07-02"), "W01182", 690L, 1),
    Row(Date.valueOf("2017-07-02"), "W22223", 587L, 2),
    Row(Date.valueOf("2017-07-02"), "W00191", 586L, 3),
    Row(Date.valueOf("2017-07-02"), "W22829", 134L, 4)

  )
  val rankedSchema: StructType = StructType(Seq(
    StructField("Date", DateType, nullable = true),
    StructField("Bike number", StringType, nullable = true),
    StructField("Duration", LongType, nullable = true),
    StructField("rank", IntegerType, nullable = false)
  ))
  val rankedDF: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(rankedData), rankedSchema)

  // Data that should be created after all statements
  val finalData = Seq(
    Row(Date.valueOf("2017-07-01"), "W22042", 2763L),
    Row(Date.valueOf("2017-07-02"), "W01182", 690L),
  )
  // The result schema of the spark job, which differs from rankedDFSchema only in the absence of one column "rank"
  val topForEachDaySchema: StructType = StructType(rankedSchema.dropRight(1))
}
