// src/test/scala/com/challenge/UserPurchaseAggregatorSpec.scala
package com.challenge

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, Assertions}
import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.types._

/**
 * Test suite for the UserPurchaseAggregator object.
 */
class UserPurchaseAggregatorSpec extends AnyFunSuite with BeforeAndAfterAll with Assertions {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("UserPurchaseAggregatorTest") // App name for context, useful if Spark UI were enabled for debugging.
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "1") // Optimize for faster local test execution.
      .config("spark.ui.enabled", "false")         // Disable Spark UI to speed up test runs.
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") // Minimize log verbosity during tests.
  }

  override def afterAll(): Unit = {
    if (spark != null) { // Ensure SparkSession was initialized before stopping.
      spark.stop()
    }
  }

  /**
   * Custom assertion to compare DataFrames.
   * Checks for schema equality and then data equality (order-insensitive via Set comparison).
   * .collect() is used, suitable for small test DataFrames.
   */
  def assertDataFramesEqual(expectedDF: DataFrame, actualDF: DataFrame): Unit = {
    assert(expectedDF.schema == actualDF.schema, 
      s"Schemas did not match:\nExpected: ${expectedDF.schema}\nActual:   ${actualDF.schema}")
    assert(expectedDF.collect().toSet == actualDF.collect().toSet, 
      s"Data did not match.\nExpected (Set of Rows):\n${expectedDF.collect().toSet}\nActual (Set of Rows):\n${actualDF.collect().toSet}")
  }

  // Utilize the input schema from the main application object for consistency.
  val testSchema: StructType = UserPurchaseAggregator.inputSchema 

  test("should process valid inputs correctly and filter by date") {
    val inputData = Seq(
      Row(101, "User A", "a@example.com", "06/15/2023 10:00:00 a. m.", 100.0f), // Expect to keep.
      Row(102, "User B", "b@example.com", "01/01/2024 10:00:00 a. m.", 50.0f),  // Expect to filter out (year 2024).
      Row(101, "User A", "a@example.com", "07/20/2023 10:00:00 p. m.", 200.0f)  // Expect to keep.
    )
    val inputDF = spark.createDataFrame(spark.sparkContext.parallelize(inputData), testSchema)

    val expectedData = Seq(
      Row(101, 300.0, "User A") // Aggregated result for User A (100.0 + 200.0).
    )
    // Schema for the aggregated output. Note: sum() typically promotes FloatType to DoubleType.
    val expectedSchema = StructType(Array(
      StructField(ColumnNames.USER_ID, IntegerType, nullable = true),
      StructField(ColumnNames.TOTAL_PURCHASE_AMOUNT, DoubleType, nullable = true), 
      StructField(ColumnNames.USER_NAME, StringType, nullable = true)
    ))
    val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), expectedSchema)

    val actualDF = UserPurchaseAggregator.processData(spark, inputDF)
    assertDataFramesEqual(expectedDF, actualDF)
  }

  test("should remove records with null values in critical columns") {
    val inputData = Seq(
      Row(101, "User A", "a@example.com", "06/15/2023 10:00:00 a. m.", 100.0f), // Keep.
      Row(null, "User B", "b@example.com", "07/15/2023 10:00:00 a. m.", 50.0f),  // Remove (null user_id).
      Row(103, "User C", null, "08/15/2023 10:00:00 a. m.", 75.0f),               // Remove (null email).
      Row(104, "User D", "d@example.com", null, 120.0f)                          // Remove (null purchase_datetime).
    )
    val inputDF = spark.createDataFrame(spark.sparkContext.parallelize(inputData), testSchema)

    val expectedData = Seq(
      Row(101, 100.0, "User A") // Only the first record is expected to pass cleaning and year filter.
    )
    val expectedSchema = StructType(Array(
      StructField(ColumnNames.USER_ID, IntegerType, nullable = true),
      StructField(ColumnNames.TOTAL_PURCHASE_AMOUNT, DoubleType, nullable = true),
      StructField(ColumnNames.USER_NAME, StringType, nullable = true)
    ))
    val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), expectedSchema)
    
    val actualDF = UserPurchaseAggregator.processData(spark, inputDF)
    assertDataFramesEqual(expectedDF, actualDF)
  }

  // Test handling of date strings that don't match the expected parsing format.
  // The current processing logic results in null timestamps for these,
  // which are then filtered out by the year-based filter.
  test("should handle incorrect date formats (they should be filtered out)") {
    val inputData = Seq(
      Row(101, "User A", "a@example.com", "06/15/2023 10:00:00 a. m.", 100.0f), // Keep.
      Row(102, "User B", "b@example.com", "INVALID_DATE_FORMAT", 50.0f),       // Invalid: results in null timestamp.
      Row(103, "User C", "c@example.com", "07/20/2023 XX:YY:ZZ q. z.", 75.0f)    // Invalid: results in null timestamp.
    )
    val inputDF = spark.createDataFrame(spark.sparkContext.parallelize(inputData), testSchema)

    val expectedData = Seq(
      Row(101, 100.0, "User A") // Only the validly dated record from 2023 should remain.
    )
    val expectedSchema = StructType(Array(
      StructField(ColumnNames.USER_ID, IntegerType, nullable = true),
      StructField(ColumnNames.TOTAL_PURCHASE_AMOUNT, DoubleType, nullable = true),
      StructField(ColumnNames.USER_NAME, StringType, nullable = true)
    ))
    val expectedDF = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), expectedSchema)
    
    val actualDF = UserPurchaseAggregator.processData(spark, inputDF)
    assertDataFramesEqual(expectedDF, actualDF)
  }

  // Test the behavior when the input DataFrame is empty.
  // Expects an empty DataFrame as output, but with the schema of the aggregated data.
  test("should handle an empty input DataFrame gracefully") {
    val emptyRDD = spark.sparkContext.emptyRDD[Row]
    val inputDF = spark.createDataFrame(emptyRDD, testSchema)

    // Schema for the expected empty output (post-aggregation).
    val expectedSchema = StructType(Array(
      StructField(ColumnNames.USER_ID, IntegerType, nullable = true),
      StructField(ColumnNames.TOTAL_PURCHASE_AMOUNT, DoubleType, nullable = true),
      StructField(ColumnNames.USER_NAME, StringType, nullable = true)
    ))
    val expectedDF = spark.createDataFrame(emptyRDD, expectedSchema)
    
    val actualDF = UserPurchaseAggregator.processData(spark, inputDF)
    assertDataFramesEqual(expectedDF, actualDF)
  }
}