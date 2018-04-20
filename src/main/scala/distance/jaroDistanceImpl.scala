package distance

object jaroDistanceImpl {

  def main(args: Array[String]): Unit = {
    val string1: String = "cat"
    val string2: String = "catia"

    println(calculateJaroWinker(string1,string2))
  }

  def calculateJaroWinker(columnA: String, columnB: String): Double = {
    val columnA_len = columnA.length
    val columnB_len = columnB.length
    if (columnA_len == 0 && columnB_len == 0) return 1.0
    val match_distance = Math.max(columnA_len, columnB_len) / 2 - 1
    val columnAMatchCount = Array.ofDim[Boolean](columnA_len)
    val columnBMatchCount = Array.ofDim[Boolean](columnB_len)
    var matches = 0
    for (i <- 0 until columnA_len) {
      val start = Math.max(0, i - match_distance)
      val end = Math.min(i + match_distance + 1, columnB_len)
      start until end find { j => !columnBMatchCount(j) && columnA(i) == columnB(j) } match {
        case Some(j) =>
          columnAMatchCount(i) = true
          columnBMatchCount(j) = true
          matches += 1
        case None =>
      }
    }
    if (matches == 0) return 0.0
    var t = 0.0
    var k = 0
    0 until columnA_len filter columnAMatchCount foreach { i =>
      while (!columnBMatchCount(k)) k += 1
      if (columnA(i) != columnB(k)) t += 0.5
      k += 1
    }

    val m = matches.toDouble
    (m / columnA_len + m / columnB_len + (m - t) / m) / 3.0
  }

}
