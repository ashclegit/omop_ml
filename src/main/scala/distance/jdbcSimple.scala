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
    var resColumnData = new ListBuffer[String]()

    for(i <- 0 until tableListOMOP.length)
      {
        var tableName = tableListOMOP(i)
        val dfColumnDataOMOP = spark.read.format("jdbc").options(Map("url" -> jdbcUrlOmopDB,
          "dbtable" -> s"(SELECT column_name  FROM information_schema.columns WHERE table_schema = 'public'  AND table_name  = '$tableName') vocab_alias")
        ).load()
       val columnList = dfColumnDataOMOP.select("column_name").map(_.getString(0)).collect.toList
       addToResult(columnList,resColumnData)
      }

    var resList = resColumnData.toList
    displayResult(resList)
    /*val dfColumnDataOMOP = spark.read.format("jdbc").options(Map("url" -> jdbcUrlOmopDB,
      "dbtable" -> s"(SELECT column_name  FROM information_schema.columns WHERE table_schema = 'public'  AND table_name  = 'concept') vocab_alias")
    ).load()
    val columnList = dfColumnDataOMOP.select("column_name").map(_.getString(0)).collect.toList
    for(i <- 0 until columnList.length)
    {
      resColumnData += columnList(i)
    }

    var resList = resColumnData.toList
    for(i <- 0 until resList.length)
    {
      println(resList(i))
    }*/


  }


  def addToResult(strings: scala.List[String], resBuffer: ListBuffer[String]): Unit =
  {
    for(i <- 0 until strings.length)
      {
        resBuffer += strings(i)
      }
  }

  def displayResult(strings: scala.List[String]): Unit =
  {
    for(i <- 0 until strings.length)
    {
      println(strings(i))
    }
  }

  def addToFinalList(columnListJava: java.util.List[String], resColumnData: java.util.List[String]): Unit =
  {

    for(i <- 0 until columnListJava.size())
      {
        resColumnData.add(columnListJava.get(i))
      }
  }

  def display(strings: java.util.List[String]): Unit =
  {
    for(i <- 0 until strings.size())
      {
        println(strings.get(i))
      }
  }

}
