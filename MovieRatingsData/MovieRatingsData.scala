package SparkApp

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import spire.implicits.eqOps

/* Read the movies ratings data and perform different operations*/
object MovieRatingsData {

  /*Start of the program*/
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val log = LogManager.getRootLogger()

    //Create a spark session to run locally using the all cores of current machine
    val spark = SparkSession.builder().appName("MovieRatingsData").master("local[*]").getOrCreate()

    //create the schema format userID, movieID, rating, timestamp - Not used here - Just for reference
    val mySchema = new StructType(Array(
      new StructField("userID", StringType, true),
      new StructField("movieID", StringType, true),
      new StructField("rating", StringType, true),
      new StructField("timestamp", StringType, true)
    )
    )

    //Read the movies ratings data from file data/u.data
    val data = spark.read.format("text")
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .load("data/u.data")

    //Cast the data as per the datatype
    val dataFrame = data.select(split(col("value"), "\t").getItem(0).as("userID").cast(IntegerType),
      split(col("value"), "\t").getItem(1).as("movieID").cast(IntegerType),
      split(col("value"), "\t").getItem(2).as("rating").cast(IntegerType),
      split(col("value"), "\t").getItem(3).as("timestamp").cast(LongType))
      .drop("value")

    //Print the schema of data.
    dataFrame.printSchema()

    //Create a temporary view for this dataFrame.
    dataFrame.createOrReplaceTempView("dfTable")

    //Perform different SQl queries
    val dataFrameSql = spark.sql("select * from dfTable")

    log.info("-------------Display sorted data based on userId-------------")
    //Sort the dataFrame
    dataFrameSql.sort("userId").show(10, false)

    log.info("-------------Display groupBy data based on userId with ratings data aggregated-------------")
    // Group by the userId and aggregate the ratings value
    dataFrameSql.groupBy("userId").sum("rating").as("Sum").sort(desc("sum(rating)")).show(20, false)

    //Display only the some columns
    //dataFrameSql.select("userId", "rating").show(false)

    log.info("-------------Display data of particular userId 100-------------")
    //Find data of particular userId
    dataFrameSql.where(col("userId") === 100).where(col("rating") === 3 or col("rating") === 4).select("*").show(false)
  }

}
