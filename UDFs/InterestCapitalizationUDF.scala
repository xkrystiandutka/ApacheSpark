package UDFs

import org.apache.spark.sql.api.java.UDF3

import scala.math.pow

class InterestCapitalizationUDF extends UDF3[Long, Int, Int, Double]{
  override def call(startCapital: Long, yearsAmount: Int, interest: Int): Double = {
    "%.2f".format(startCapital*pow((1 + interest.toDouble/100), yearsAmount)).toDouble
  }
}
