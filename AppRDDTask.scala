import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.SparkSession

object RDDExample {
  def main(args: Array[String]): Unit = {
    // Tworzymy sesję Spark
    val spark = SparkSession.builder()
      .appName("RDDExample")
      .master("local[*]")
      .getOrCreate()

    // Tworzymy RDD z danymi
    val data = spark.sparkContext.parallelize(Seq(
      ("Jan", "Kowalski", "M", 35),
      ("Anna", "Nowak", "K", 28),
      ("Piotr", "Cieślak", "M", 42),
      ("Marta", "Kaczmarek", "K", 31),
      ("Adam", "Szymański", "M", 29),
      ("Alicja", "Wójcik", "K", 36),
      ("Tomasz", "Kruk", "M", 44),
      ("Natalia", "Kamińska", "K", 25),
      ("Wojciech", "Lewandowski", "M", 38),
      ("Karolina", "Piotrowska", "K", 27)
    ))

    // Obliczamy sumaryczny wiek mężczyzn
    val sumAgeM = data.filter(x => x._3 == "M").map(x => x._4).sum()

    // Obliczamy sumaryczny wiek kobiet
    val sumAgeF = data.filter(x => x._3 == "K").map(x => x._4).sum()

    // Obliczamy minimalny i maksymalny wiek w zestawie danych
    val (minAge, maxAge) = data.map(x => x._4).aggregate[(Int, Int)]((Int.MaxValue, Int.MinValue))(
      { case ((min, max), age) => (Math.min(min, age), Math.max(max, age)) },
      { case ((min1, max1), (min2, max2)) => (Math.min(min1, min2), Math.max(max1, max2)) }
    )

    // Wyświetlamy wyniki
    println(s"Sumaryczny wiek mężczyzn: $sumAgeM")
    println(s"Sumaryczny wiek kobiet: $sumAgeF")
    println(s"Minimalny wiek: $minAge")
    println(s"Maksymalny wiek: $maxAge")
  }
}
