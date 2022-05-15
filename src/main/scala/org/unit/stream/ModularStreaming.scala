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
    df.select("Duration", "Start date", "Member type")
  }

  def transformTimeStampToDate(df: DataFrame): DataFrame = {
    df.withColumn("Date", col("Start date").cast(DateType))
      .drop("Start date")
  }

  def groupDFByDateAndMemberType(df: DataFrame): DataFrame = {
    df.groupBy("Date", "Member type")
      .agg(
        sum("Duration").alias("Total duration"),
        count("Duration").alias("Total trips"),
      )
      .withColumn("Total duration", round(col("Total duration") / 120))
      .orderBy("Date")
  }
}
