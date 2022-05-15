package org.unit

import org.apache.spark.sql.types._

object schema {
  val trip: StructType = StructType(Seq(
    StructField("Duration", IntegerType, nullable = true),
    StructField("Start date", TimestampType, nullable = true),
    StructField("End date", TimestampType, nullable = true),
    StructField("Start station number", IntegerType, nullable = true),
    StructField("Start station", StringType, nullable = true),
    StructField("End station number", IntegerType, nullable = true),
    StructField("End station", StringType, nullable = true),
    StructField("Bike number", StringType, nullable = true),
    StructField("Member type", StringType, nullable = true),
  ))
}
