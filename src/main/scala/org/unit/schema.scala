package org.unit

import org.apache.spark.sql.types._

object schema {
  val trip: StructType = StructType(Seq(
    StructField("Duration", IntegerType, nullable = true),
    StructField("Start_date", TimestampType, nullable = true),
    StructField("End_date", TimestampType, nullable = true),
    StructField("Start_station_number", IntegerType, nullable = true),
    StructField("Start_station", StringType, nullable = true),
    StructField("End_station_number", IntegerType, nullable = true),
    StructField("End_station", StringType, nullable = true),
    StructField("Bike_number", StringType, nullable = true),
    StructField("Member_type", StringType, nullable = true),
  ))
}
