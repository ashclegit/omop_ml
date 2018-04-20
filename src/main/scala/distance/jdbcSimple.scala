package distance

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import java.util.Properties

import com.rockymadden.stringmetric.similarity.JaroWinklerMetric

import collection.mutable._
import scala.collection.mutable
import scala.math.min

//import com.rockymadden.stringmetric.similarity.JaroWinklerMetric


object jdbcSimple {

  def collectAndPrintDistance(spark: SparkSession , jdbcUrlOmopDB: String, jdbcUrlOmopCDWDB: String): Unit =
  {
    import spark.implicits._

    val connectionProperties = new Properties()
    connectionProperties.put("user", "pguser" )
    connectionProperties.put("password", "test")

    //store all the tables in a dataframe
    val dfTableDataOmop: DataFrame = spark.read.format("jdbc").options(Map("url" -> jdbcUrlOmopDB,
      "dbtable" -> "(select table_name from information_schema.tables where table_schema ='public') vocab_alias")
    ).load()

    val dfTableDataOmopCDW = spark.read.format("jdbc").options(Map("url" -> jdbcUrlOmopCDWDB,
      "dbtable" -> "(select table_name from information_schema.tables where table_schema ='public') vocab_alias")
    ).load()

    val tableListOMOP: List[String] = dfTableDataOmop.select("table_name").map(_.getString(0)).collect.toList
    val tableListOMOPCDW = dfTableDataOmopCDW.select("table_name").map(_.getString(0)).collect.toList

    //mutable hashmap to maintain the key as the tablename and the value as list of columns
    val tableToColumnMapOMOP: mutable.HashMap[String,List[String]] = mutable.HashMap.empty[String,List[String]]
    val tableToColumnMapOMOPCDW: mutable.HashMap[String,List[String]] = mutable.HashMap.empty[String,List[String]]

    val resColumnDataOMOP: List[String] =
      tableListOMOP.flatMap{tableName: String => collectColumns(tableName,jdbcUrlOmopDB,spark,tableToColumnMapOMOP)}.sorted
    val resColumnDataOMOPCDW: List[String] =
      tableListOMOPCDW.flatMap{tableName: String => collectColumns(tableName,jdbcUrlOmopCDWDB,spark,tableToColumnMapOMOPCDW)}.sorted

    calculateColumnDistance(tableToColumnMapOMOP,tableToColumnMapOMOPCDW)

  }

  //function to extract column lists for each of the tables and calculate distance between the column names
  def calculateColumnDistance(tableToColumnMapOMOP: mutable.HashMap[String, List[String]], tableToColumnMapOMOPCDW: mutable.HashMap[String, List[String]]): Unit =
  {
    tableToColumnMapOMOPCDW.keySet.foreach{
      case (key) =>
        if(tableToColumnMapOMOP.contains(key)) {
          val omopList: List[String] = tableToColumnMapOMOP.get(key).toList.flatten
          val cdwList: List[String] = tableToColumnMapOMOPCDW.get(key).toList.flatten
          val cdwManip = cdwList.map(perturbString)
          println("******************")
          println(key)
          println("******************")
          omopList.zip(cdwManip)
            .foreach{ case (a, b) =>
              println(s"$a   $b -> ${JaroWinklerMetric.compare(a.toCharArray, b.toCharArray)}")
            }
          println("")
        }
    }
  }

  //function to collect all the columns in the tables
  def collectColumns(tableName: String, jdbcUrlOmopDB: String, spark: SparkSession, tableToColumnMap: mutable.HashMap[String, List[String]]): List[String] =
  {
    import spark.implicits._
    val dfColumnDataOMOP = spark.read.format("jdbc").options(Map("url" -> jdbcUrlOmopDB,
      "dbtable" -> s"(SELECT column_name  FROM information_schema.columns WHERE table_schema = 'public'  AND table_name  = '$tableName') vocab_alias")
    ).load()
    val columnList: List[String] = dfColumnDataOMOP.select("column_name").map(_.getString(0)).collect.toList
    tableToColumnMap.put(tableName,columnList)
    columnList
  }

  //function to mamipulate and reverse the column names
  def perturbString(s: String): String = s.reverse

  //Levenshtein distance metric implemented using dynamic programming instead of recursion technique used in the git repo.
  /*def distanceBetweenColumns(col1: String, col2: String): Option[Double] = {
    val distance : Option[Double] = JaroWinklerMetric.compare(col1, col2)


    /*val dp = Array.ofDim[Int](string1.length + 1, string2.length + 1)
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
    dp(string1.length)(string2.length)*/
  }*/


  /*def extractDouble(x: Any): Option[Double] = x match {
    case n: java.lang.Number => Some(n.doubleValue())
    case _ => None
  }*/




  def main(args: Array[String]): Unit = {


    Class.forName("org.postgresql.Driver")
    val jdbcUrlOmopDB  = "jdbc:postgresql://localhost:5432/omop_postgres"
    val jdbcUrlOmopCDWDB = "jdbc:postgresql://localhost:5432/omop_postgres1"


    //create a spark session
    val spark = SparkSession
      .builder()
      .appName("calculate distance")
      .config("spark.master", "local")
      .getOrCreate()

    collectAndPrintDistance(spark,jdbcUrlOmopDB,jdbcUrlOmopCDWDB)
  }

}