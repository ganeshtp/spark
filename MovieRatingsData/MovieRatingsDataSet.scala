/*Perform movie rating analysis using the Spark DataSet*/
package SparkApp

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._


/* Read the movies ratings data and perform different operations*/
object MovieRatingsDataSet {

  // Create case class with schema of u.data
  case class MovieRatings(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  /*Start of the program*/
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val log = LogManager.getRootLogger()

    //Create a spark session to run locally using the all cores of current machine
    val spark = SparkSession.builder().appName("MovieRatingsDataSet").master("local[*]").getOrCreate()

    //create the schema format userID, movieID, rating, timestamp
    val movieRatingsSchema = new StructType(Array(
      new StructField("userID", IntegerType, true),
      new StructField("movieID", IntegerType, true),
      new StructField("rating", IntegerType, true),
      new StructField("timestamp", LongType, true)
    )
    )

    import spark.implicits._
    //Read the movies ratings data from file data/u.data to dataSet
    val movieDS = spark.read
      .option("sep", "\t")
      .schema(movieRatingsSchema)
      .csv("data/u.data")
      .as[MovieRatings]


    //Print the schema of data.
    movieDS.printSchema()

    //Create a temporary view for this dataFrame.
    movieDS.createOrReplaceTempView("dfTable")

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
