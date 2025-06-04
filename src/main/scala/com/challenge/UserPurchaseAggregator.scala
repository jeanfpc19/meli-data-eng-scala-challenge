package com.challenge

import org.apache.spark.sql.SparkSession // For SparkSession
import org.apache.spark.sql.DataFrame    // For DataFrame type
import org.apache.spark.sql.functions._  // For col, sum, first, year, to_timestamp, regexp_replace etc.
import org.apache.spark.sql.types._      // For StructType, StructField, StringType, IntegerType, FloatType

// For S3 upload using Hadoop FileSystem API
import org.apache.hadoop.fs.{FileSystem, Path, FileUtil}
import org.apache.hadoop.conf.Configuration
import java.net.URI

/**
 * Defines constants for column names used throughout the application.
 */
object ColumnNames {
  val USER_ID = "user_id"
  val USER_NAME = "user_name"
  val EMAIL = "email"
  val PURCHASE_DATETIME = "purchase_datetime"
  val PURCHASE_AMOUNT = "purchase_amount"
  val PURCHASE_DATETIME_CLEANED = "purchase_datetime_cleaned" // Intermediate column for AM/PM normalization
  val PURCHASE_TIMESTAMP = "purchase_timestamp"             // Final converted timestamp column
  val TOTAL_PURCHASE_AMOUNT = "total_purchase_amount"
}

/**
 * Main application object for processing, aggregating user purchase data,
 * and uploading the result to AWS S3.
 */
object UserPurchaseAggregator {

  val inputSchema = StructType(Array(
    StructField(ColumnNames.USER_ID, IntegerType, nullable = true),
    StructField(ColumnNames.USER_NAME, StringType, nullable = true),
    StructField(ColumnNames.EMAIL, StringType, nullable = true),
    StructField(ColumnNames.PURCHASE_DATETIME, StringType, nullable = true), 
    StructField(ColumnNames.PURCHASE_AMOUNT, FloatType, nullable = true)
  ))

  def processData(spark: SparkSession, rawDF: DataFrame): DataFrame = {
    val cleanedDF = rawDF.na.drop(Seq(ColumnNames.USER_ID, ColumnNames.EMAIL, ColumnNames.PURCHASE_DATETIME))
    
    val withCleanedDateTimeDF = cleanedDF
      .withColumn(ColumnNames.PURCHASE_DATETIME_CLEANED,
        regexp_replace( 
          regexp_replace(col(ColumnNames.PURCHASE_DATETIME), " a\\..*$", " AM"), 
          " p\\..*$", " PM"
        )
      )

    val withTimestampDF = withCleanedDateTimeDF
      .withColumn(ColumnNames.PURCHASE_TIMESTAMP, 
        to_timestamp(col(ColumnNames.PURCHASE_DATETIME_CLEANED), "MM/dd/yyyy hh:mm:ss a")
      )

    val filteredDF = withTimestampDF
      .filter(year(col(ColumnNames.PURCHASE_TIMESTAMP)) === 2023)
      .drop(ColumnNames.PURCHASE_DATETIME_CLEANED) 
      
    val aggregatedDF = filteredDF
      .groupBy(ColumnNames.USER_ID)
      .agg(
        sum(ColumnNames.PURCHASE_AMOUNT).as(ColumnNames.TOTAL_PURCHASE_AMOUNT),
        first(ColumnNames.USER_NAME).as(ColumnNames.USER_NAME) 
      )
      .orderBy(desc(ColumnNames.TOTAL_PURCHASE_AMOUNT))

    aggregatedDF
  }

  /**
   * Uploads the contents of a local directory to an AWS S3 path.
   *
   * @param spark        The active SparkSession (used to get Hadoop configuration).
   * @param localPathStr The path to the local directory to upload.
   * @param s3Bucket     The target S3 bucket name.
   * @param s3KeyPrefix  The S3 key prefix (folder path) within the bucket.
   */
  def uploadDirectoryToS3(spark: SparkSession, localPathStr: String, s3Bucket: String, s3KeyPrefix: String): Unit = {
    println(s"Attempting to upload contents of local directory '$localPathStr' to S3 bucket '$s3Bucket' under prefix '$s3KeyPrefix'...")
    
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    // Ensure S3A filesystem is configured (credentials should ideally be set via environment variables or instance profiles)
    // For local testing with specific keys, you might need to set these:
    // hadoopConf.set("fs.s3a.access.key", "YOUR_AWS_ACCESS_KEY_ID")
    // hadoopConf.set("fs.s3a.secret.key", "YOUR_AWS_SECRET_ACCESS_KEY")
    // hadoopConf.set("fs.s3a.endpoint", "s3.amazonaws.com") // Or your specific region endpoint

    val localDir = new java.io.File(localPathStr)
    if (!localDir.exists() || !localDir.isDirectory) {
      println(s"Error: Local path '$localPathStr' does not exist or is not a directory. S3 upload skipped.")
      return
    }

    val localFS = FileSystem.getLocal(hadoopConf)
    val s3FS = FileSystem.get(new URI(s"s3a://$s3Bucket/"), hadoopConf)

    val localPath = new Path(localDir.getAbsolutePath) // Use absolute path for local
    
    // Construct the full destination path on S3, including the original local directory name
    // e.g., if localPathStr is "output/user_purchases", and s3KeyPrefix is "processed",
    // S3 path will be s3a://bucket/processed/user_purchases/
    val s3DestPath = new Path(s"s3a://$s3Bucket/$s3KeyPrefix/${localDir.getName}")

    try {
      // Create the S3 destination directory if it doesn't exist (FileUtil.copy doesn't always create parent dirs)
      if (!s3FS.exists(s3DestPath.getParent)) { // Check parent of the target dir
         s3FS.mkdirs(s3DestPath.getParent)
      }
      if (s3FS.exists(s3DestPath)) {
        println(s"Warning: S3 destination path $s3DestPath already exists. Deleting before copy to mimic overwrite behavior.")
        s3FS.delete(s3DestPath, true) // true for recursive delete
      }
      
      println(s"Copying from local: $localPath to S3: $s3DestPath")
      FileUtil.copy(localFS, localPath, s3FS, s3DestPath, false, hadoopConf) // deleteSource = false
      println(s"Successfully uploaded contents of '$localPathStr' to '$s3DestPath'")
    } catch {
      case e: Exception =>
        println(s"Error uploading to S3: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  def main(args: Array[String]): Unit = {
    // Now expecting 4 arguments: input, local_output, s3_bucket, s3_key_prefix
    if (args.length != 4) {
      println("Usage: UserPurchaseAggregator <input_csv_path> <local_output_parquet_path> <s3_bucket_name> <s3_key_prefix>")
      sys.exit(1)
    }
    val inputPath = args(0)
    val localOutputPath = args(1)
    val s3BucketName = args(2)
    val s3KeyPrefix = args(3)


    val spark = SparkSession.builder
      .appName("User Purchase Aggregator")
      .master("local[*]") 
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    val rawDF = spark.read
      .option("header", "true")
      .option("encoding", "UTF-8")
      .schema(inputSchema)
      .csv(inputPath)
    
    val finalDF = processData(spark, rawDF)

    println(s"Processed data schema for output:")
    finalDF.printSchema()
    println(s"Sample of processed data (first 5 rows):")
    finalDF.show(5, false)

    // Save locally first
    println(s"Saving aggregated data locally to $localOutputPath...")
    finalDF.write
      .mode("overwrite")
      .parquet(localOutputPath)
    println(s"✅ Data saved locally to $localOutputPath")

    // Then, upload the local directory to S3
    uploadDirectoryToS3(spark, localOutputPath, s3BucketName, s3KeyPrefix)
    
    println(s"✅ Processing and S3 upload attempt complete!")
    
    spark.stop()
  }
}
