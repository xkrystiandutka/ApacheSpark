import org.apache.spark.sql.functions.{col, explode, sha2}
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}

object AppNetfix {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Sparka")
      .master("local")
      .getOrCreate()

    val netflixDF: Dataset[Row] = spark.read
      .option("header", "true")
      .csv("netflix_titles.csv")

    netflixDF.show(false)
    netflixDF.printSchema()

    val netflixWithoutNullsDF: Dataset[Row] = netflixDF.na.fill("N.A.")

    netflixWithoutNullsDF.show(false)

    val listedInStatsDF: Dataset[Row] = netflixWithoutNullsDF.withColumn("listed_in", functions.split(col("listed_in"), ","))
      .select(col("show_id"), explode(col("listed_in")).as("singleListedIn"))
      .groupBy("singleListedIn")
      .count()

    listedInStatsDF.show(false)
    listedInStatsDF.printSchema()

    val hashedDirectorsDF: Dataset[Row] = netflixWithoutNullsDF.select("show_id", "director")
      .withColumn("hashed_director", sha2(col("director"), 512))

    hashedDirectorsDF.show(false)

    hashedDirectorsDF.write
      .parquet("hashed_netflix_directors.parquet")
  }
}
