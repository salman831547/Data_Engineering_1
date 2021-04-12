package scala_besics

import org.apache.log4j.{Level, Logger}

object functions {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Calling the function
    println("addition of numbers = "+add_numbers(5,6))
    println(add(2,3))
    //Currying in Scala is simply a technique or a process of transforming a function.
    //This function takes multiple arguments into a function that takes single argument.
    println(add2(29)(20))
    // Partially Applied function.
    val sum=add3(29)_
    println(sum(5))


  }
  def add_numbers(x:Int,y:Int):Int={
    val sum=x+y
    //if return keyword is not given, last variable is return
    return sum
  }
  def add(x: Int, y: Int):Int = x + y
  def curry_add(a:Int,b:Int):Int=a+b
  //Currying
  def add2(a: Int) (b: Int): Int = a + b
  // Partially Applied function.
  def add3(a: Int) (b: Int): Int = a + b


}
