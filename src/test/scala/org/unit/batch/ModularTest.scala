package org.unit.batch

import org.apache.spark.sql.types.DateType
import org.unit.batch.Modular._
import org.unit.schema.trip

/**
 * This class is used to test the correctness of each function in [[Modular]].
 *
 * The class extends the [[org.unit.batch.BatchTest]] which has all necessary data and schemes.
 *
 * Spark session is provided by the [[org.unit.SparkSessionTestWrapper]] class
 */
class ModularTest extends BatchTest {

  test("should create DataFrame from CSV file") {
    val df = createDfFromCSV(spark, trip, "src/test/resources")

    assert(isDfEqual(df, tripData, trip),
      "DataFrame should have expected trip data and schema")
  }

  test("should leave only interested columns") {
    val df = leaveInterestingColumns(tripDF)

    assert(isDfEqual(df, interestedData, interestedColumnSchema),
      "DataFrame should have expected interested data and schema")
  }

  test("should cast column to correct type") {
    val df = transformTimeStampToDate(interestedDF)

    assert(df.schema("Date").dataType == DateType,
      "Date column should be DateType")
  }

  test("should create DataFrame with rank column") {
    val df = createDFWithRank(transformTimeStampToDate(interestedDF))

    assert(isDfEqual(df, rankedData, rankedSchema),
      "DataFrame should have expected rank data and schema")
  }

  test("should create DataFrame with top column") {
    val df = getTopBikesForEachDay(rankedDF)

    assert(isDfEqual(df, finalData, topForEachDaySchema),
      "DataFrame should have expected top bikes data and schema")
  }
}

