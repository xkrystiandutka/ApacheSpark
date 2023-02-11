import UDFs.InterestCapitalizationUDF
import org.apache.spark.sql.functions.{call_udf, col, lit}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object CapitalizationOfInterest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("fundament-sparka")
      .master("local")
      .getOrCreate()

    val interestCapitalizationUDF: InterestCapitalizationUDF = new InterestCapitalizationUDF()
    spark.udf.register("interestCapitalizationUDF", interestCapitalizationUDF, DataTypes.DoubleType)

    val peopleDF = spark.read
      .option("header", "true")
      .csv("money_saving.csv")

    val peopleWithMoneyDF: Dataset[Row] = peopleDF.withColumn("money", col("money").cast(DataTypes.LongType))
      .withColumn("interest", col("interest").cast(DataTypes.IntegerType))
      .withColumn("10years", call_udf("interestCapitalizationUDF", col("money"), lit(10), col("interest")))
      .withColumn("20years", call_udf("interestCapitalizationUDF", col("money"), lit(20), col("interest")))
      .withColumn("40years", call_udf("interestCapitalizationUDF", col("money"), lit(40), col("interest")))
      .withColumn("60years", call_udf("interestCapitalizationUDF", col("money"), lit(60), col("interest")))

    peopleWithMoneyDF.show()
  }

}
