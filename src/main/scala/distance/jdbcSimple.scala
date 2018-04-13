package distance

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import java.util.Properties
import collection.mutable._
import scala.math.min


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

    val resColumnDataOMOP: List[String] = tableListOMOP.flatMap{ tableName: String =>
      val dfColumnDataOMOP = spark.read.format("jdbc").options(Map("url" -> jdbcUrlOmopDB,
        "dbtable" -> s"(SELECT column_name  FROM information_schema.columns WHERE table_schema = 'public'  AND table_name  = '$tableName') vocab_alias")
      ).load()
      val columnList: List[String] = dfColumnDataOMOP.select("column_name").map(_.getString(0)).collect.toList
      columnList
    }.sorted

    val resColumnDataOMOPCDW = tableListOMOPCDW.flatMap { tableName =>
      val dfColumnDataOMOPCDW = spark.read.format("jdbc").options(Map("url" -> jdbcUrlOmopCDWDB,
        "dbtable" -> s"(SELECT column_name  FROM information_schema.columns WHERE table_schema = 'public'  AND table_name  = '$tableName') vocab_alias")
      ).load()
      val columnList = dfColumnDataOMOPCDW.select("column_name").map(_.getString(0)).collect.toList
      columnList
    }.sorted

    val resColumnManipulated = resColumnDataOMOPCDW.map(perturbString)

    resColumnDataOMOP.zip(resColumnManipulated)
      .foreach{ case (a, b) =>
        println(s"$a   $b -> ${distanceBetweenColumns(a, b)}")
      }

  }

  def perturbString(s: String): String = s.reverse

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