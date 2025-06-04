# **Spark Purchase Data Aggregator Challenge**

This project is a Scala application built with Apache Spark and sbt. It processes a CSV file containing user purchase data, performs cleaning and transformations, aggregates the data to find total purchase amounts per user for the year 2023, saves the results locally in Parquet format, and then uploads the local output to an AWS S3 bucket.

## **Features**

* Reads purchase data from a CSV file.  
* Defines an explicit schema for robust data ingestion.  
* Cleans the dataset by removing records with null values in critical columns (user\_id, email, purchase\_datetime).  
* Normalizes and converts non-standard purchase\_datetime strings (with "a. m." or "p. m." suffixes) into proper Spark Timestamps.  
* Filters records to include only purchases made within the year 2023\.  
* Aggregates data to compute the total purchase amount for each user.  
* Saves the processed and aggregated data locally in Parquet format.  
* Uploads the locally saved Parquet directory to a specified AWS S3 bucket and prefix.  
* Includes unit tests for the core data processing logic.

## **Prerequisites**

To build and run this project locally, you will need the following installed:

* **Java Development Kit (JDK):** Version 8, 11, or 17 is recommended (the project was developed with JDK 11).  
* **Scala:** Version 2.12.x (specifically, this project uses Scala 2.12.18).  
* **sbt (Simple Build Tool):** Version 1.x (this project uses sbt 1.9.9).  
* **Apache Spark:** Version 3.5.x (this project uses 3.5.1).
* **AWS Credentials:** For the S3 upload functionality, you need to have AWS credentials configured in your environment. The most common ways are:  
  * Setting environment variables:  
    * AWS\_ACCESS\_KEY\_ID  
    * AWS\_SECRET\_ACCESS\_KEY  
    * AWS\_SESSION\_TOKEN (if using temporary credentials)  
    * AWS\_DEFAULT\_REGION (optional, but good practice, e.g., us-east-1)  
  * Using an AWS credentials file (\~/.aws/credentials or C:\\Users\\YourUser\\.aws\\credentials).  
  * If running on an EC2 instance, using an IAM Role with S3 permissions.


## **Project Structure**

* src/main/scala/com/challenge/UserPurchaseAggregator.scala: Contains the main application logic, including S3 upload.  
* src/test/scala/com/challenge/UserPurchaseAggregatorSpec.scala: Contains unit tests for the application.  
* build.sbt: sbt build definition file, including dependencies for Spark and AWS.  
* project/plugins.sbt: sbt plugin definitions (e.g., for sbt-assembly).  
* data/: Directory to place your input CSV file(s).  
* output/: Default directory where the Parquet output will be saved locally before S3 upload.

## **Setup**

1. **Clone the repository:**  
   git clone \<your-repository-url\>  
   cd \<your-repository-name\>

2. Place your input data:  
   Put your purchase data CSV file (e.g., Purchas\_data-data.csv) into the data/ directory within the project.  
3. Configure AWS Credentials:  
   Ensure your AWS credentials are configured as described in the "Prerequisites" section. The application uses the default AWS SDK credential provider chain.

## **Running the Application**

The application takes **four** command-line arguments:

1. Path to the input CSV file.  
2. Path to the *local* output directory for Parquet files.  
3. Your AWS S3 bucket name.  
4. The S3 key prefix (folder path) where the output directory will be copied within the bucket.

To run the application using sbt:

sbt "run \<path\_to\_input\_csv\> \<local\_output\_directory\> \<s3\_bucket\_name\> \<s3\_key\_prefix\>"

Example:  
If your input file is data/Purchas\_data-data.csv, you want local output in output/user\_purchases\_local, your S3 bucket is my-spark-challenge-bucket, and you want the data under the S3 prefix processed\_data:  
sbt "run data/purchase\_data.csv output/user\_purchases\_local spark-challenge-bucket processed\_data"

This will save Parquet files locally to output/user\_purchases\_local and then attempt to upload the entire user\_purchases\_local directory to s3a://spark-challenge-bucket/processed\_data/user\_purchases\_local/.

The application will print information to the console, including the schema of the processed data, a sample of the final aggregated output, and messages about the S3 upload status.

## **Running Tests**

Unit tests are included to verify the core data processing logic (they do **not** test the S3 upload functionality). To run the tests:

sbt test

You should see output indicating that all tests have passed.

## **Output**

The application first outputs the aggregated data locally in Parquet format to the specified local directory. It then **uploads this entire local output directory** (containing the Parquet files) to the specified AWS S3 bucket under the given key prefix. The final S3 path will be s3a://\<s3\_bucket\_name\>/\<s3\_key\_prefix\>/\<local\_output\_directory\_name\>/.

## **Code Overview**

The main logic is within the UserPurchaseAggregator.scala file:

* **ColumnNames object:** Defines string constants for column names.  
* **inputSchema val:** An explicitly defined StructType for the input CSV data.  
* **processData(spark: SparkSession, rawDF: DataFrame): DataFrame function:** Encapsulates core data transformations (cleaning, timestamp conversion, filtering, aggregation).  
* **uploadDirectoryToS3(...) function:** Handles the logic of copying the contents of a local directory to a specified S3 path using Hadoop's FileSystem API. It assumes AWS credentials are set in the environment.  
* **main method:** Handles command-line arguments (now including S3 parameters), initializes Spark, reads CSV, calls processData, saves the result locally to Parquet, and then calls uploadDirectoryToS3.

This structure separates the core logic (processData) from the application's entry point and I/O handling (main), which also facilitates unit testing.
