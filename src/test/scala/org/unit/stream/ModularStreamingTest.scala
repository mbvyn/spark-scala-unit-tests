package org.unit.stream

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.unit.schema.trip
import org.unit.stream.ModularStreaming._

class ModularStreamingTest extends StreamTest {

  import spark.implicits._

  implicit val sqlContext: SQLContext = spark.sqlContext

  test("should create stream DataFrame from CSV file") {
    val tableName = "trips"

    val streamTripDF = createStreamFromCSV(spark, trip, "src/test/resources")

    writeDfToMemory(streamTripDF, tableName, "append")

    val df = getDF(tableName)

    assert(streamTripDF.isStreaming, "DataFrame should be streaming")
    assert(isDfEqual(actualDF = df, expectedData = tripData, trip),
      "DataFrame should have expected trip data and schema")
  }

  test("should leave only interested columns") {
    val tableName = "interestedColumns"

    val source: MemoryStream[TripData] = MemoryStream[TripData]
    val tripData = tripDF.as[TripData].collect().toList
    source.addData(tripData)

    val streaming: DataFrame = source.toDF()

    val interestedDF = leaveInterestingColumns(streaming)

    writeDfToMemory(interestedDF, tableName, "append")

    val df = getDF(tableName).as[InterestedData]

    assert(isDsEqual(df, expectedInterestedData, interestedDfSchema),
      "DataFrame should have expected interested data and schema")
  }

  test("should cast column to correct type") {
    val tableName = "transformedColumns"

    val source: MemoryStream[InterestedData] = MemoryStream[InterestedData]
    source.addData(expectedInterestedData)

    val streaming: DataFrame = source.toDF()

    val transformedDF = transformTimeStampToDate(streaming)

    writeDfToMemory(transformedDF, tableName, "append")

    val df = getDF(tableName)

    assert(df.schema("Date").dataType == DateType,
      "Date column should be DateType")
  }

  test("should group data by date and member type") {
    val tableName = "groupedData"

    val source: MemoryStream[InterestedData] = MemoryStream[InterestedData]
    source.addData(expectedInterestedData)

    val streaming: DataFrame = source.toDF()
    val transformedDF = transformTimeStampToDate(streaming)
    val groupedDF = groupDFByDateAndMemberType(transformedDF)

    writeDfToMemory(groupedDF, tableName, "complete")

    val df = getDF(tableName).as[GroupedData]

    assert(isDsEqual(df, expectedGroupedData, groupedDfSchema),
      "DataFrame should have expected grouped data and schema")
  }
}
