package org.unit

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

trait SparkSessionUnit extends AnyFunSuite with BeforeAndAfterAll{
  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .getOrCreate()

  override def beforeAll(): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }
}
