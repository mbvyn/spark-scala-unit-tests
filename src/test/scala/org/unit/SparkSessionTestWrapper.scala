package org.unit

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.unit.schema.trip

import java.sql.Timestamp

/**
 * Parent class for all unit tests.
 *
 * It provides a SparkSession instance.
 *
 * It also provides a method to compare DataFrames with an expected data and schema
 *
 * And data that represents data from the csv file
 *
 * See [[org.unit.batch.BatchTest]] and [[org.unit.stream.StreamTest]] for more details.
 */
class SparkSessionTestWrapper extends AnyFunSuite with BeforeAndAfterAll {

  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .getOrCreate()

  // Data that just mock up data from csv file
  val tripData = Seq(
    Row(2762, Timestamp.valueOf("2017-07-01 00:01:09"), Timestamp.valueOf("2017-07-01 00:47:11"), 31289, "Henry Bacon Dr & Lincoln Memorial Circle NW", 31289, "Henry Bacon Dr & Lincoln Memorial Circle NW", "W21474", "Casual"),
    Row(2763, Timestamp.valueOf("2017-07-01 00:01:24"), Timestamp.valueOf("2017-07-01 00:47:27"), 31289, "Henry Bacon Dr & Lincoln Memorial Circle NW", 31289, "Henry Bacon Dr & Lincoln Memorial Circle NW", "W22042", "Member"),
    Row(690, Timestamp.valueOf("2017-07-02 00:01:45"), Timestamp.valueOf("2017-07-02 00:13:16"), 31122, "16th & Irving St NW", 31299, "Connecticut Ave & R St NW", "W01182", "Member"),
    Row(134, Timestamp.valueOf("2017-07-02 00:01:46"), Timestamp.valueOf("2017-07-02 00:04:00"), 31201, "15th & P St NW", 31267, "17th St & Massachusetts Ave NW", "W22829", "Member"),
    Row(587, Timestamp.valueOf("2017-07-02 00:02:05"), Timestamp.valueOf("2017-07-02 00:11:52"), 31099, "Madison & N Henry St", 31907, "Franklin & S Washington St", "W22223", "Casual"),
    Row(586, Timestamp.valueOf("2017-07-02 00:02:06"), Timestamp.valueOf("2017-07-02 00:11:53"), 31099, "Madison & N Henry St", 31907, "Franklin & S Washington St", "W00191", "Casual")
  )
  val tripDF: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(tripData), trip)

  /**
   * Compare dataframes with an expected data and schema.
   *
   * @param actualDF       The actual DataFrame.
   * @param expectedData   The expected data as a sequence of rows.
   * @param expectedSchema The expected schema of the DataFrame.
   */
  def isDfEqual(actualDF: DataFrame, expectedData: Seq[Row], expectedSchema: StructType): Boolean = {
    actualDF.schema.equals(expectedSchema) && actualDF.collect.toSeq.equals(expectedData)
  }

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
  }
}
