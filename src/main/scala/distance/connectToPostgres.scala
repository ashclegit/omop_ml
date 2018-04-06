package distance

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import scala.math.min


object connectToPostgres {

  def main(args: Array[String]): Unit = {

    Class.forName("org.postgresql.Driver")
    val url = "jdbc:postgresql://localhost:5432/omop_postgres?user=pguser&password=test"
    val table1 = "cohort"
    val table2 = "cohort_definition"


    val spark = SparkSession
      .builder()
      .appName("calculate distance")
      .config("spark.master", "local")
      .getOrCreate()


    val sDF1 = spark.read.format("jdbc").options(Map("url" -> url,"dbtable" -> table1)).load()
    val sDF2 = spark.read.format("jdbc").options(Map("url" -> url,"dbtable" -> table2)).load()

    var a1 = new Array[String](sDF1.columns.length)
    var a2 = new Array[String](sDF2.columns.length)

    val cols1 = sDF1.columns.copyToArray(a1)
    val cols2 = sDF2.columns.copyToArray(a2)

    for(i <- 0 until a1.length){
      println("i is: " + i);
      println("i'th column is: " + a1(i));
    }

    for(i <- 0 until a2.length){
      println("i is: " + i);
      println("i'th column is: " + a2(i));
    }

    var i = 0
    var j = 0

    while(i < a1.length && j < a2.length)
      {
        println(distanceBetweenColumns(a1(i).toString,a2(j).toString))
        i = i+1
        j = j+1
        j
      }

  }

  def distanceBetweenColumns(string1: String, string2: String): Int = {
    val distArray = Array.ofDim[Int](string1.length + 1, string2.length + 1)
    for (i <- 0 to string1.length)
      {
        distArray(i)(0) = i
      }
    for (j <- 0 to string1.length)
      {
        distArray(0)(j) = j
      }
    for (j <- 1 to string1.length; i <- 1 to string1.length) {
      if (string1(i - 1) == string1(j - 1)) distArray(i)(j) = distArray(i - 1)(j - 1)
      else distArray(i)(j) = min(min(distArray(i - 1)(j), distArray(i)(j - 1)), distArray(i - 1)(j - 1)) + 1
    }
    distArray(string1.length)(string1.length)
  }

}
