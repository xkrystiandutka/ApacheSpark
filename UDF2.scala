import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{Dataset, Row, SparkSession}


object UDF2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Spark")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val people: Seq[(String, String, String, Int)] = Seq(("1", "marek", "czuma", 28), ("2", "ania", "kowalska", 30), ("3", "magda", "nowak", 28),
      ("4", "jan", "kowalski", 15), ("5", "jozef", "czuma", 25), ("6", "ignacy", "czuma", 35),
      ("7", "laura", "moscicka", 68), ("8", "zuzanna", "birecka", 12), ("9", "roman", "kowalski", 45),
      ("10", "marek", "kowalski", 68), ("11", "ignacy", "nowak", 43), ("12", "ania", "nowak", 33),
      ("13", "laura", "czuma", 6), ("14", "karol", "birecki", 21), ("15", "karol", "nowak", 43),
      ("16", "jan", "moscicki", 33), ("17", "jan", "birecki", 36), ("18", "andrzej", "kowalski", 82))

    val peopleDF: Dataset[Row] = people.toDF("id", "firstName", "lastName", "age")

    val peopleWithAdultsDF: Dataset[Row] = peopleDF.transform(isAdult)

    peopleWithAdultsDF.show()

  }

  def isAdult(df: Dataset[Row]): Dataset[Row] = {
    df.withColumn("isAdult", when(col("age").geq(18), "T").otherwise("F"))

  }
}
