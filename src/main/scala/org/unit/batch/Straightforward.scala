package org.unit.batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, sum}
import org.apache.spark.sql.types.DateType
import org.unit.schema.trip

/**
 * Spark batch job to calculate the best bike and its duration for each day
 *
 * The same job as Modular but without a modular approach
 *
 * @see [[Modular]]
 */
object Straightforward extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Straightforward")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val tripDF = spark.read
    .schema(trip)
    .option("header", "true")
    .csv("src/main/resources")

  val interestedDF = tripDF.select("Duration", "Start date", "Bike number")

  val transformedDF = interestedDF.withColumn("Date", col("Start date").cast(DateType)).drop("Start date")

  val windowsSpec = Window.partitionBy("Date").orderBy(col("Duration").desc)

  val windowedDF = transformedDF
    .groupBy("Date", "Bike number")
    .agg(sum("Duration").as("Duration"))
    .withColumn("rank", row_number().over(windowsSpec))

  val topForEachDayDF = windowedDF
    .filter(col("rank") === 1)
    .select("Date", "Bike number", "Duration")
    .orderBy("Date")

  topForEachDayDF.show(false)
}
