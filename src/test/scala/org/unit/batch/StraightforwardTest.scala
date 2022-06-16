package org.unit.batch

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, sum}
import org.apache.spark.sql.types._
import org.unit.schema.trip

/**
 * This class is used to test the correctness of the batch processing in the [[Straightforward]] class.
 *
 * The class extends the [[BatchTest]] which has all necessary data and schemes.
 *
 * Spark session is provided by the [[org.unit.SparkSessionTestWrapper]] class
 */
class StraightforwardTest extends BatchTest {

  test("should create DataFrame from CSV file") {
    val df = spark.read.schema(trip).option("header", value = true)
      .csv("src/test/resources")

    assert(isDfEqual(df, tripData, trip),
      "DataFrame should have expected trip data and schema")
  }

  test("should leave only interested columns") {
    val df = tripDF.select("Duration", "Start_date", "Bike_number")

    assert(isDfEqual(df, interestedData, interestedColumnSchema),
      "DataFrame should have expected interested data and schema")
  }

  test("should cast column to correct type") {
    val df = interestedDF.withColumn("Date", col("Start_date")
      .cast(DateType)).drop("Start date")

    assert(df.schema("Date").dataType == DateType,
      "Date column should be DateType")
  }

  test("should create DataFrame with rank column") {
    val windowsSpec = Window.partitionBy("Date").orderBy(col("Duration").desc)

    val df = interestedDF
      .withColumn("Date", col("Start_date").cast(DateType))
      .drop("Start date")
      .groupBy("Date", "Bike_number")
      .agg(sum("Duration").as("Duration"))
      .withColumn("rank", row_number().over(windowsSpec))

    assert(isDfEqual(df, rankedData, rankedSchema),
      "DataFrame should have expected rank data and schema")
  }

  test("should create DataFrame with top column") {
    val df = rankedDF
      .filter(col("rank") === 1)
      .select("Date", "Bike_number", "Duration")
      .orderBy("Date")

    assert(isDfEqual(df, finalData, topForEachDaySchema),
      "DataFrame should have expected top bikes data and schema")
  }
}
