package testing

import org.apache.log4j.{Level, Logger}

object function_argument {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //val a=scala.io.StdIn.readInt()
    print("Enter number: ")
    val a=scala.io.StdIn.readInt()
    find_n(a)

  }
  def find_n(n:Int):Unit={
    for(n<-1 to n) {
      if(n%2==0)
        println("zero remainder")
      else if(n%2==1)
        println("one remainder")
      else
        println(n)
    }
  }

}
