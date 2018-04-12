package distance

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
//import com.rockymadden.stringmetric.similarity.LevenshteinMetric
import scala.math.min

object connectToPostgres {

  def main(args: Array[String]): Unit = {

    //register the postgres driver
    Class.forName("org.postgresql.Driver")
    val url = "jdbc:postgresql://localhost:5432/omop_postgres?user=pguser&password=test"
    val table1 = "cohort"
    val table2 = "cohort_definition"

    //create a spark session
    val spark = SparkSession
      .builder()
      .appName("calculate distance")
      .config("spark.master", "local")
      .getOrCreate()



    //read data from the individual tables and load into separate dataframes
    val sDF1 = spark.read.format("jdbc").options(Map("url" -> url,"dbtable" -> table1)).load()
    val sDF2 = spark.read.format("jdbc").options(Map("url" -> url,"dbtable" -> table2)).load()

    //separate string arrays to store the column names
    var a1 = new Array[String](sDF1.columns.length)
    var a2 = new Array[String](sDF2.columns.length)

    val cols1 = sDF1.columns.copyToArray(a1)
    val cols2 = sDF2.columns.copyToArray(a2)

    /*for(i <- 0 until a1.length){
      println("i is: " + i);
      println("i'th column is: " + a1(i));
    }

    for(i <- 0 until a2.length){
      println("i is: " + i);
      println("i'th column is: " + a2(i));
    }*/

    var i = 0
    var j = 0

    //print the individual distances between two columns
    while(i < a1.length && j < a2.length)
      {
        println(distanceBetweenColumns(a1(i).toString,a2(j).toString))
        i = i+1
        j = j+1
      }

  }

  //Levenshtein distance metric implemented using dynamic programming instead of recursion technique used in the git repo.
  def distanceBetweenColumns(string1: String, string2: String): Int = {
    val dp = Array.ofDim[Int](string1.length + 1, string2.length + 1)
    for (i <- 0 to string1.length)
      {
        dp(i)(0) = i
      }
    for (j <- 0 to string2.length)
      {
        dp(0)(j) = j
      }
    for (j <- 1 to string2.length; i <- 1 to string1.length) {
      if (string1(i - 1) == string2(j - 1))
        {
          dp(i)(j) = dp(i - 1)(j - 1)
        }
      else
        {
          dp(i)(j) = min(min(dp(i - 1)(j), dp(i)(j - 1)), dp(i - 1)(j - 1)) + 1
        }
    }
    dp(string1.length)(string2.length)
  }
}
