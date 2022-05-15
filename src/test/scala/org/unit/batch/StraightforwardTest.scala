package org.unit.batch

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, sum}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.unit.SparkSessionUnit
import org.unit.schema.trip

import java.sql.{Date, Timestamp}

/**
 * Straightforward unit test for batch processing
 * each test case is a test case for a single batch processing
 *
 * This trait extends SparkSessionUnit, which is a trait that provides a SparkSession
 */
class StraightforwardTest extends SparkSessionUnit {

  // Data that just mock up data from csv file
  val tripData = Seq(
    Row("2762", "2017-07-01 00:01:09", "2017-07-01 00:47:11", "31289", "Henry Bacon Dr & Lincoln Memorial Circle NW", "31289", "Henry Bacon Dr & Lincoln Memorial Circle NW", "W21474", "Casual"),
    Row("2763", "2017-07-01 00:01:24", "2017-07-01 00:47:27", "31289", "Henry Bacon Dr & Lincoln Memorial Circle NW", "31289", "Henry Bacon Dr & Lincoln Memorial Circle NW", "W22042", "Member"),
    Row("690", "2017-07-02 00:01:45", "2017-07-02 00:13:16", "31122", "16th & Irving St NW", "31299", "Connecticut Ave & R St NW", "W01182", "Member"),
    Row("134", "2017-07-02 00:01:46", "2017-07-02 00:04:00", "31201", "15th & P St NW", "31267", "17th St & Massachusetts Ave NW", "W22829", "Member"),
    Row("587", "2017-07-02 00:02:05", "2017-07-02 00:11:52", "31099", "Madison & N Henry St", "31907", "Franklin & S Washington St", "W22223", "Casual"),
    Row("586", "2017-07-02 00:02:06", "2017-07-02 00:11:53", "31099", "Madison & N Henry St", "31907", "Franklin & S Washington St", "W00191", "Casual")
  )
  val tripDF: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(tripData), trip)

  // Data that should be created after select statement
  val interestedData = Seq(
    Row("2762", Timestamp.valueOf("2017-07-01 00:01:09"), "W21474"),
    Row("2763", Timestamp.valueOf("2017-07-01 00:01:24"), "W22042"),
    Row("690", Timestamp.valueOf("2017-07-02 00:01:45"), "W01182"),
    Row("134", Timestamp.valueOf("2017-07-02 00:01:46"), "W22829"),
    Row("587", Timestamp.valueOf("2017-07-02 00:02:05"), "W22223"),
    Row("586", Timestamp.valueOf("2017-07-02 00:02:06"), "W00191")
  )
  val interestedColumnSchema: StructType = StructType(Seq(
    StructField("Duration", IntegerType, nullable = true),
    StructField("Start date", TimestampType, nullable = true),
    StructField("Bike number", StringType, nullable = true),
  ))
  val interestedDF: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(interestedData), interestedColumnSchema)

  // Data that should be created after window statement
  val rankedData = Seq(
    Row(Date.valueOf("2017-07-01"), "W21474", 2762L, 1),
    Row(Date.valueOf("2017-07-01"), "W22042", 2763L, 2),
    Row(Date.valueOf("2017-07-02"), "W01182", 690L, 1),
    Row(Date.valueOf("2017-07-02"), "W22829", 134L, 2),
    Row(Date.valueOf("2017-07-02"), "W22223", 587L, 3),
    Row(Date.valueOf("2017-07-02"), "W00191", 586L, 4)
  )
  val rankedDFSchema: StructType = StructType(Seq(
    StructField("Date", DateType, nullable = true),
    StructField("Bike number", StringType, nullable = true),
    StructField("Duration", LongType, nullable = true),
    StructField("rank", IntegerType, nullable = false)
  ))
  val rankedDF: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(rankedData), rankedDFSchema)

  // The result schema of the spark job, which differs from rankedDFSchema only in the absence of one column "rank"
  val topForEachDaySchema: StructType = StructType(rankedDFSchema.dropRight(1))

  test("should create DataFrame from CSV file") {
    val df = spark.read.schema(trip).option("header", value = true)
      .csv("src/test/resources")

    assert(df.count() == 10, "DataFrame should have 10 rows")
  }

  test("should leave only interested columns") {
    val df = tripDF.select("Duration", "Start date", "Bike number")
    assert(df.schema.equals(interestedColumnSchema), "Schema should be equal")
  }

  test("should cast column to correct type") {
    val df = interestedDF.withColumn("Date", col("Start date").cast(DateType)).drop("Start date")
    assert(df.schema("Date").dataType == DateType, "Date column should be DateType")
  }

  test("should create DataFrame with rank column") {
    val windowsSpec = Window.partitionBy("Date").orderBy(col("Duration").desc)

    val df = interestedDF
      .withColumn("Date", col("Start date").cast(DateType))
      .drop("Start date")
      .groupBy("Date", "Bike number")
      .agg(sum("Duration").as("Duration"))
      .withColumn("rank", row_number().over(windowsSpec))

    assert(df.schema.equals(rankedDFSchema), "Schema should be equal")
  }

  test("should create DataFrame with top column") {
    val df = rankedDF
      .filter(col("rank") === 1)
      .select("Date", "Bike number", "Duration")
      .orderBy("Date")

    assert(df.schema.equals(topForEachDaySchema), "Schema should be equal")
    assert(df.count() == 2, "DataFrame should have 2 rows, because there is only one top bike for each day")
  }
}
