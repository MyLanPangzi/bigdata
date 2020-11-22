import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

object SplitTest {
  def main(args: Array[String]): Unit = {
    val s = "aaa{a}aa{b}{nn}"
    var r: ListBuffer[String] = ListBuffer()
    var start = 0;
    Breaks.breakable {
      while (start < s.length) {
        start = s.indexOf("{", start)
        val end = s.indexOf("}", start)
        println(s"s:${start},e:$end")
        if (start <= 0) {
          Breaks.break
        }
        if (end<=0) {
          throw new IllegalArgumentException("no match }")
        }
        r += s.substring(start, end + 1)
        start = end + 1
      }
    }
    r.foreach(println)
  }
}
