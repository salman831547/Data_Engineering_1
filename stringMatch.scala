package interview.prep.com
package sparkAtoZ

object stringMatch {
  def main(args: Array[String]){
    println(matchTest(3))
  }
  def matchTest(x:Int):String =x match {
    case 1 =>"salman"
    case 2 =>"ssk"
    case _ => "srk"


  }


}
