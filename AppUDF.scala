import UDFs.InitialsUDF
import org.apache.spark.sql.functions.{call_udf, col}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object AppUDF {
      def main(args: Array[String]): Unit ={
      val spark: SparkSession = SparkSession.builder()
        .appName("Sparka")
        .master("local")
        .getOrCreate()

      import spark.implicits._

      val initialsUDF: InitialsUDF = new InitialsUDF()
      spark.udf.register("initialsUDF", initialsUDF, DataTypes.StringType)

      val people: Seq[(String, String, String, Int)] = Seq(("1", "marek", "czuma", 28), ("2", "ania", "kowalska", 30), ("3", "magda", "nowak", 28),
        ("4", "jan", "kowalski", 15), ("5", "jozef", "czuma", 25), ("6", "ignacy", "czuma", 35),
        ("7", "laura", "moscicka", 68), ("8", "zuzanna", "birecka", 12), ("9", "roman", "kowalski", 45),
        ("10", "marek", "kowalski", 68), ("11", "ignacy", "nowak", 43), ("12", "ania", "nowak", 33),
        ("13", "laura", "czuma", 6), ("14", "karol", "birecki", 21), ("15", "karol", "nowak", 43),
        ("16", "jan", "moscicki", 33), ("17", "jan", "birecki", 36), ("18", "andrzej", "kowalski", 82))

      val peopleDF: Dataset[Row] = people.toDF("id", "firstName", "lastName", "age")

      val peopleWithInitials: Dataset[Row] = peopleDF.withColumn("initials",
        call_udf("InitialsUDF", col("firstName"), col("lastName")))

      peopleWithInitials.show()
    }
}
