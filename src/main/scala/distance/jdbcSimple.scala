package distance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import java.{sql, util}
import java.util.Properties
import java.sql.DriverManager

import org.apache.spark.sql
import java.util.ArrayList
import java.util.List

import collection.JavaConverters._
import collection.mutable._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer;
import scala.math.min

object jdbcSimple {

  def main(args: Array[String]): Unit = {


    Class.forName("org.postgresql.Driver")
    val jdbcUrlOmopDB = "jdbc:postgresql://localhost:5432/omop_postgres"
    val jdbcUrlOmopCDWDB = "jdbc:postgresql://localhost:5432/omop_postgres"


    //create a spark session
    val spark = SparkSession
      .builder()
      .appName("calculate distance")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val connectionProperties = new Properties()

    connectionProperties.put("user", "pguser" )
    connectionProperties.put("password", "test")

    val dfTableDataOmop = spark.read.format("jdbc").options(Map("url" -> jdbcUrlOmopDB,
                          "dbtable" -> "(select table_name from information_schema.tables where table_schema ='public') vocab_alias")
                          ).load()

    val dfTableDataOmopCDW = spark.read.format("jdbc").options(Map("url" -> jdbcUrlOmopCDWDB,
                          "dbtable" -> "(select table_name from information_schema.tables where table_schema ='public') vocab_alias")
                          ).load()

    val tableListOMOP = dfTableDataOmop.select("table_name").map(_.getString(0)).collect.toList
    val tableListOMOPCDW = dfTableDataOmopCDW.select("table_name").map(_.getString(0)).collect.toList


    //var resColumnData: java.util.List[String] = null
    var columnListJava: java.util.List[String] = null
    var resColumnDataOMOP = new ListBuffer[String]()

    for(i <- 0 until tableListOMOP.length)
      {
        var tableName = tableListOMOP(i)
        val dfColumnDataOMOP = spark.read.format("jdbc").options(Map("url" -> jdbcUrlOmopDB,
          "dbtable" -> s"(SELECT column_name  FROM information_schema.columns WHERE table_schema = 'public'  AND table_name  = '$tableName') vocab_alias")
        ).load()
       val columnList = dfColumnDataOMOP.select("column_name").map(_.getString(0)).collect.toList
       addToResult(columnList,resColumnDataOMOP)
      }

    var resColumnDataOMOPCDW = new ListBuffer[String]()

    for(i <- 0 until tableListOMOPCDW.length)
    {
      var tableName = tableListOMOPCDW(i)
      val dfColumnDataOMOPCDW = spark.read.format("jdbc").options(Map("url" -> jdbcUrlOmopCDWDB,
        "dbtable" -> s"(SELECT column_name  FROM information_schema.columns WHERE table_schema = 'public'  AND table_name  = '$tableName') vocab_alias")
      ).load()
      val columnList = dfColumnDataOMOPCDW.select("column_name").map(_.getString(0)).collect.toList
      addToResult(columnList,resColumnDataOMOPCDW)
    }

    var resListOMOP = resColumnDataOMOP.toList.sorted
    var resListOMOPCDW = resColumnDataOMOPCDW.toList.sorted
    var resColumnManipulated = new ListBuffer[String]()

    manipulateColValues(resListOMOPCDW,resColumnManipulated)
    var manColValues = resColumnManipulated.toList

    var j = 0
    var i = 0

    while(i < resListOMOP.length && j < manColValues.length)
    {
      print(resListOMOP(i).toString + "   " + manColValues(j).toString + " ")
      print(distanceBetweenColumns(resListOMOP(i).toString,manColValues(j).toString))
      println("")
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


  def addToResult(strings: scala.List[String], resBuffer: ListBuffer[String]): Unit =
  {
    for(i <- 0 until strings.length)
      {
        resBuffer += strings(i)
      }
  }

  def manipulateColValues(strings: scala.List[String], revBuffer: ListBuffer[String]): Unit =
  {
    for(i <- 0 until strings.length)
      {
        revBuffer += strings(i).reverse
      }
  }

}