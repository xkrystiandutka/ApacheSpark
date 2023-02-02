import org.apache.spark.sql.{Dataset, Row, SparkSession}

object AppTask {

  def main(args: Array[String]): Unit ={
    val spark: SparkSession = SparkSession.builder()
      .appName( "Spark")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val people: Seq[(String, String, String, Int, String, Int)] = Seq(("krystian", "dutka", "0012342506" ,22, "M", 15000), ("artur", "kasparov", "1223002506" ,26, "M", 10000),
      ("marek", "samos", "0025063212" ,30, "M", 7500), ("zosia", "nowak", "0020303243",23, "F", 5000), ("kasia", "kowalska", "0034432301", 35, "F", 5675))

    val peopleDF: Dataset[Row] = people.toDF( "firstName", "lastName", "pesel", "age", "sex", "income")

    peopleDF.show()
    peopleDF.select("lastName", "sex", "income").show()
  }
}