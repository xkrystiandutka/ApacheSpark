import org.apache.spark.sql.functions.{avg, col, regexp_replace}
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object AppPizza {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Spark")
      .master("local")
      .getOrCreate()

    val pizzaRawDF: Dataset[Row] = spark.read
      .option("header", "true")
      .csv("pizza_data.csv")

    pizzaRawDF.show()

    val pizzaCleanDF: Dataset[Row] = pizzaRawDF.withColumn("Price", regexp_replace(col("Price"),      "[$,]", "")
      .cast(DataTypes.DoubleType))

    import org.apache.spark.sql.functions._

    val companiesWithAvgPricesDF: Dataset[Row] = pizzaCleanDF
      .groupBy("Company")
      .agg(round(avg("Price"), 2).as("Avg Price"))

    val companiesWithSumPricesDF: Dataset[Row] = pizzaCleanDF
      .groupBy("Company")
      .agg(round(sum("Price"),2).as("Sum Price"))

    val companiesWithPizzaCountDF: Dataset[Row] = pizzaCleanDF.select("Company", "Pizza Name")
      .distinct()
      .groupBy("Company").count()
      .orderBy(col("count").desc)

    companiesWithAvgPricesDF.show()
    companiesWithSumPricesDF.show()
    companiesWithPizzaCountDF.show()

  }
}
