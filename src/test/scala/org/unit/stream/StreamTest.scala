package org.unit.stream

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}
import org.unit.SparkSessionTestWrapper

import java.sql.{Date, Timestamp}

/**
 * Parent class for streaming tests.
 *
 * It contains common data for the streaming tests.
 *
 * It also contains the methods for comparing streaming datasets with expected data,
 * writing dataframes to a memory sink, and getting the dataframe from a memory table.
 *
 * See [[org.unit.stream.ModularStreamingTest]] for more details.
 */
class StreamTest extends SparkSessionTestWrapper {
  // Data that should be created after select statement
  val expectedInterestedData = Seq(
    InterestedData(2762, Timestamp.valueOf("2017-07-01 00:01:09"), "Casual"),
    InterestedData(2763, Timestamp.valueOf("2017-07-01 00:01:24"), "Member"),
    InterestedData(690, Timestamp.valueOf("2017-07-02 00:01:45"), "Member"),
    InterestedData(134, Timestamp.valueOf("2017-07-02 00:01:46"), "Member"),
    InterestedData(587, Timestamp.valueOf("2017-07-02 00:02:05"), "Casual"),
    InterestedData(586, Timestamp.valueOf("2017-07-02 00:02:06"), "Casual")
  )
  val interestedDfSchema: StructType = Encoders.product[InterestedData].schema

  // Data that should be created after groupBy statement
  val expectedGroupedData: Seq[GroupedData] = Seq(
    GroupedData(Date.valueOf("2017-07-01"), "Casual", 23.0, 1L),
    GroupedData(Date.valueOf("2017-07-01"), "Member", 23.0, 1L),
    GroupedData(Date.valueOf("2017-07-02"), "Casual", 10.0, 2L),
    GroupedData(Date.valueOf("2017-07-02"), "Member", 7.0, 2L)
  )
  // This is the schema for the data defined above.
  // It looks like a laborious process because we need
  // to explicitly change the nullable value to true in the "Total_duration" field
  // to correspond to the actual schema.
  val groupedDfSchema: StructType = StructType(
    Encoders.product[GroupedData].schema.map(
      field => if (field.name == "Total_duration") field.copy(nullable = true) else field
    ))

  /**
   * Write the dataframe to a memory sink.
   *
   * @param df         The dataframe to write.
   * @param tableName  The name of the table to write to.
   * @param outputMode The output mode to write to a sink. Available modes: append, complete, update
   */
  def writeDfToMemory(df: DataFrame, tableName: String, outputMode: String): Unit = {
    df.writeStream
      .outputMode(outputMode)
      .format("memory")
      .queryName(tableName)
      .start
      .processAllAvailable()
  }

  /**
   * Compare dataset with an expected data and schema.
   *
   * @param actualDS       The actual DataSet[T].
   * @param expectedData   The expected data as a sequence of T.
   * @param expectedSchema The expected schema of the DataSet.
   */
  def isDsEqual[T](actualDS: Dataset[T], expectedData: => Seq[T], expectedSchema: StructType): Boolean = {
    actualDS.schema.equals(expectedSchema) && actualDS.collect.toSeq.equals(expectedData)
  }

  /**
   * Fetch data from the memory table
   *
   * @param tableName The name of the table to fetch from.
   */
  def getDF(tableName: String): DataFrame = {
    spark.sql(s"SELECT * FROM $tableName").toDF()
  }
}

case class TripData(
                     Duration: Int,
                     Start_date: Timestamp,
                     End_date: Timestamp,
                     Start_station_number: Int,
                     Start_station: String,
                     End_station_number: Int,
                     End_station: String,
                     Bike_number: String,
                     Member_type: String
                   )

case class InterestedData(Duration: Int,
                          Start_date: Timestamp,
                          Member_type: String)

case class GroupedData(Date: Date,
                       Member_type: String,
                       Total_duration: Double,
                       Total_trips: Long)