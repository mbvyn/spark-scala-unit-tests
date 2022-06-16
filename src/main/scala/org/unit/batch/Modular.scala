package org.unit.batch

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, sum}
import org.apache.spark.sql.types.{DateType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.unit.schema.trip

/**
 * Spark batch job to calculate the best bike and its duration for each day
 *
 * The same job as Straightforward but with a modular approach
 *
 * @see [[Straightforward]]
 */
object Modular {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Modular")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val tripDF = createDfFromCSV(spark, trip, "src/main/resources")

    val interestedDF = leaveInterestingColumns(tripDF)

    val transformedDF = transformTimeStampToDate(interestedDF)

    val windowedDF = createDFWithRank(transformedDF)

    val topForEachDayDF = getTopBikesForEachDay(windowedDF)

    topForEachDayDF.show(false)
  }

  def createDfFromCSV(spark: SparkSession, schema: StructType, path: String): DataFrame = {
    spark.read
      .schema(schema)
      .option("header", "true")
      .csv(path)
  }

  def leaveInterestingColumns(df: DataFrame): DataFrame = {
    df.select("Duration", "Start_date", "Bike_number")
  }

  def transformTimeStampToDate(df: DataFrame): DataFrame = {
    df.withColumn("Date", col("Start_date").cast(DateType)).drop("Start_date")
  }

  def createDFWithRank(df: DataFrame): DataFrame = {
    val windowsSpec = Window.partitionBy("Date").orderBy(col("Duration").desc)

    df.groupBy("Date", "Bike_number")
      .agg(sum("Duration").as("Duration"))
      .withColumn("rank", row_number().over(windowsSpec))
  }

  def getTopBikesForEachDay(df: DataFrame): DataFrame = {
    df.filter(col("rank") === 1)
      .select("Date", "Bike_number", "Duration")
      .orderBy("Date")
  }
}
