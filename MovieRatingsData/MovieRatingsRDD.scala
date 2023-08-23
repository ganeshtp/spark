package SparkApp

import org.apache.spark._
import org.apache.log4j._

object MovieRatingsRDD {

  def parseLines(line: String):(Integer,Integer, Integer, Long) = {
    val fields = line.split("\t")
    val userId = fields(0).toInt
    val movieId = fields(1).toInt
    val rating = fields(2).toInt
    val timeStamp = fields(3).toLong
    return (userId , movieId, rating, timeStamp)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val log = LogManager.getRootLogger()

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "MovieRatingsRDD")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("data/u.data")


    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(x => x.split("\t")(2))


    // Count up how many times each value (rating) occurs
    val results = ratings.countByValue()

    // Sort the resulting map of (rating, count) tuples
    val sortedResults = results.toSeq.sortBy(_._1)

    // Print each result on its own line.
    sortedResults.foreach(println)


  }

}
