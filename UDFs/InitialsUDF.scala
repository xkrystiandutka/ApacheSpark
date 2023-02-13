package UDFs

import org.apache.spark.sql.api.java.UDF2

class InitialsUDF extends UDF2[String, String, String]{
  override def call(firstName: String, lastName: String): String = {
    s"${firstName.charAt(0).toUpper} ${lastName.charAt(0).toUpper}"
  }
}
