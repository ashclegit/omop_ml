package distance

import com.rockymadden.stringmetric.similarity.{JaroMetric, JaroWinklerMetric}

object simpleJaroTest {

  def main(args: Array[String]): Unit = {

    val string1: String = "trace"
    val string2: String = "create"

    println(JaroWinklerMetric.compare(string1,string2))
  }
}
