package org.unit.stream

import org.apache.spark.sql.functions.{col, count, round, sum}
import org.apache.spark.sql.types.{DateType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.unit.schema.trip

object ModularStreaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Streaming")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val tripDF = createStreamFromCSV(spark, trip, "src/main/resources")

    val interestedDF = leaveInterestingColumns(tripDF)

    val transformedDF = transformTimeStampToDate(interestedDF)

    val groupedDF = groupDFByDateAndMemberType(transformedDF)

    groupedDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def createStreamFromCSV(spark: SparkSession, schema: StructType, path: String): DataFrame = {
    spark.readStream
      .schema(schema)
      .option("header", "true")
      .csv(path)
  }

  def leaveInterestingColumns(df: DataFrame): DataFrame = {
    df.select("Duration", "Start_date", "Member_type")
  }

  def transformTimeStampToDate(df: DataFrame): DataFrame = {
    df.withColumn("Date", col("Start_date").cast(DateType))
      .drop("Start_date")
  }

  def groupDFByDateAndMemberType(df: DataFrame): DataFrame = {
    df.groupBy("Date", "Member_type")
      .agg(
        sum("Duration").alias("Total_duration"),
        count("Duration").alias("Total_trips"),
      )
      .withColumn("Total_duration", round(col("Total_duration") / 120))
      .orderBy("Date")
  }
}
