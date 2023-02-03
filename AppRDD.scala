import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object AppRDD {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Spark")
      .master("local")
      .getOrCreate()

    val namesLists : Seq[String] = Seq("krystian", "artur", "kamil", "marek", "julia", "adam")
    val namesRDD : RDD[String] = spark.sparkContext.parallelize(namesLists)

    val namesBig: RDD[String] = namesRDD.map(x=>x.toUpperCase) // take - transformation


    namesBig.take(5) // take - action
      .foreach(println)

    val namesFiltered : RDD[String] = namesRDD.filter(n=>n.length<5)

    namesFiltered.take(5) // take - action
      .foreach(println)

    val numberList: Seq[Int] = Seq(10, 20, 30, 40, 50, 50)
    val  numbersRDD: RDD[Int] = spark.sparkContext.parallelize(numberList)

    val  sum: Int = numbersRDD.reduce((v1, v2)=> v1 * v2)

    println(sum)


  }
}