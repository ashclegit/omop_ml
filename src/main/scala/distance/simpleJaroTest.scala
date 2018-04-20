package distance

import com.rockymadden.stringmetric.similarity.{JaroMetric, JaroWinklerMetric}

object simpleJaroTest {

  def main(args: Array[String]): Unit = {

    val string1: String = "CAT"
    val string2: String = "CATIA"

    println(JaroWinklerMetric.compare(string1,string2))
  }
}
