import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object AppDF {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Spark")
      .master("local")
      .getOrCreate()

  val pizzaDF: Dataset[Row] = spark.read
    .option("header", "true")
    .csv("pizza_data.csv")

  //  pizzaDF.show(false)  show - action
      pizzaDF.show()

    val pizzaWithFilterDF: Dataset[Row] = pizzaDF.filter(col("Type").contains(lit("Cheeses"))) // filter  transformation

    val pizzaAmount: Long = pizzaWithFilterDF.count() // count - action
    println(s"Pizza amount: $pizzaAmount")
  }
}