/**
 * Created by jiaqige on 12/31/14.
 */
class Test {
}

object Test{
  def main(args:Array[String])= {
    val s = "1,2,3"
    val set = s.split(",").toSet
    val subsets = set.subsets

    System.out.println(subsets.mkString)
  }
}

