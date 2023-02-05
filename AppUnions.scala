import org.apache.spark.sql.{Dataset, Row, SparkSession}

case object AppUnions {

  def main(args: Array[String]): Unit ={

    val spark: SparkSession = SparkSession.builder()
      .appName("Spark")
      .master("local")
      .getOrCreate()

   val pizza1DF: Dataset[Row] = spark.read
     .option("header", "true")
     .csv("pizza_data_half.csv")

    val pizza2DF: Dataset[Row] = spark.read
      .option("header", "true")
      .csv("pizza_data_half2.csv")

    val allDeliciousFoodTogetherDF: Dataset[Row] = pizza1DF.unionByName(pizza2DF)

    val pizza1Count : Long = pizza1DF.count()
    val pizza2Count : Long = pizza2DF.count()

    val AllTogetherCount : Long = allDeliciousFoodTogetherDF.count()
    println(s"Pizza 1 count: $pizza1Count")
    println(s"Pizza 2 count: $pizza2Count")
    println(s"All together count: $AllTogetherCount")

  }
}
