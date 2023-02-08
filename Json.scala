import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.functions.{col, from_json, lit}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object Json {

  def main (args: Array[String]): Unit ={
    val spark: SparkSession = SparkSession.builder()
      .appName("fundament-sparka")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val dataSchema = StructType(
      List(
        StructField("name", DataTypes.StringType, false),
        StructField("time", DataTypes.TimestampType, false)
      )
    )

    val jsonDF = Seq(("{\"name\": \"marek\",\"time\": 1469501675}"), ("{\"name\": \"kasia\",\"time\": 1469501623}")).toDF("value")
    val fromJsonDF = jsonDF.withColumn("jsonData", from_json(col("value"), dataSchema)).select("jsonData.*")
    jsonDF.show(false)
    jsonDF.printSchema()
    jsonDF.withColumn("jsonData", from_json(col("value"), dataSchema)).show(false)
    jsonDF.withColumn("jsonData", from_json(col("value"), dataSchema)).printSchema()
    fromJsonDF.show(false)

    val people: Seq[(String, String, String, Int)] = Seq(("1", "marek", "czuma", 28), ("2", "ania", "kowalska", 30), ("3", "magda", "nowak", 28),
      ("4", "jan", "kowalski", 15), ("5", "jozef", "czuma", 25), ("6", "ignacy", "czuma", 35),
      ("7", "laura", "moscicka", 68), ("8", "zuzanna", "birecka", 12), ("9", "roman", "kowalski", 45),
      ("10", "marek", "kowalski", 68), ("11", "ignacy", "nowak", 43), ("12", "ania", "nowak", 33),
      ("13", "laura", "czuma", 6), ("14", "karol", "birecki", 21), ("15", "karol", "nowak", 43),
      ("16", "jan", "moscicki", 33), ("17", "jan", "birecki", 36), ("18", "andrzej", "kowalski", 82))

    val peopleDF: Dataset[Row] = people.toDF("id", "firstName", "lastName", "age")

    val peopleWithFullNameDF: Dataset[Row] = peopleDF.withColumn("fullName", functions.concat(col("firstName"), lit(" "), col("lastName")))
      .drop("firstName", "lastName")

    val adultDF: Dataset[Row] = peopleWithFullNameDF.filter(col("age").geq(18))

    adultDF.show()
  }
}
