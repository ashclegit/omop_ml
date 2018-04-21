package distance

import java.util.ArrayList
import java.lang.StringBuilder
import scala.collection.JavaConversions._

class ConsoleDisplay(columnHeadings: ArrayList[String],
                     rowDataList: ArrayList[ArrayList[String]]) {

  private val dataSeparator: Int = 4
  private val sepSymbol: Char = '-'
  private var columns: ArrayList[String] = columnHeadings
  private var data: ArrayList[ArrayList[String]] = rowDataList
  private var rowSpace: ArrayList[Integer] = new ArrayList[Integer]()
  private var noOfRows: Int = _
  private var noOfCols: Int = _

  for (i <- 0 until columns.size) {
    rowSpace.add(columns.get(i).length)
  }
  calcrowSpaceAll()

  def dataDisplay(): Unit = {
    val sb: StringBuilder = new StringBuilder()
    val sbRowSep: StringBuilder = new StringBuilder()
    var padder: String = ""
    val rowLength: Int = 0
    var rowSeperator: String = ""
    for (i <- 0 until dataSeparator) {
      padder += " "

    }
    for (i <- 0 until rowSpace.size) {
      sbRowSep.append("|")
      for (j <- 0 until rowSpace.get(i) + (dataSeparator * 2)) {
        sbRowSep.append(sepSymbol)
      }
    }
    sbRowSep.append("|")
    rowSeperator = sbRowSep.toString
    sb.append(rowSeperator)
    sb.append("\n")
    sb.append("|")
    for (i <- 0 until columns.size) {
      sb.append(padder)
      sb.append(columns.get(i))
      for (k <- 0 until (rowSpace.get(i) - columns.get(i).length)) {
        sb.append(" ")
      }
      sb.append(padder)
      sb.append("|")
    }
    sb.append("\n")
    sb.append(rowSeperator)
    sb.append("\n")
    for (i <- 0 until data.size) {
      val tempRow: ArrayList[String] = data.get(i)
      sb.append("|")
      for (j <- 0 until tempRow.size) {
        sb.append(padder)
        sb.append(tempRow.get(j))
        for (k <- 0 until (rowSpace.get(j) - tempRow.get(j).length)) {
          sb.append(" ")
        }
        sb.append(padder)
        sb.append("|")
      }
      sb.append("\n")
      sb.append(rowSeperator)
      sb.append("\n")
    }

    println(sb.toString)
  }


  private def calcrowSpaceAll(): Unit = {
    for (i <- 0 until data.size) {
      val temp: ArrayList[String] = data.get(i)
      for (j <- 0 until temp.size if temp.get(j).length > rowSpace.get(j)) {
        rowSpace.set(j, temp.get(j).length)
      }
    }
  }

}