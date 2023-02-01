import org.apache.spark.sql.{Dataset, Row, SparkSession}

object App {

  def main(args: Array[String]): Unit ={
    val spark: SparkSession = SparkSession.builder()
      .appName( "Spark")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val people: Seq[(String, String, Int)] = Seq(("krystian", "dutka", 22), ("artur", "kasparov", 26),
      ("marek", "samos", 30), ("zosia", "nowak", 23), ("kasia", "kowalska", 30))

    val peopleDF: Dataset[Row] = people.toDF( "firstName", "lastName", "age")

    peopleDF.show()
    peopleDF.select("firstName", "age").show()
  }
}