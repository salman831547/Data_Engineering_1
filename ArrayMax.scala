package interview.prep.com
package sparkAtoZ

object ArrayMax {
  def main(args: Array[String]):Unit = {
    val arr1=Array(1,2,3,4,5)
    val arr2=Array(6,7,8,9,0)
    //total of array
    var total=0
    for(x<-0 to arr1.length - 1){
      total+=arr1(x)

    }
    println("Total of array is "+total)
    //Find max of an Array
    var max=arr1(0)
    for(x<-1 to arr1.length-1){
      if(arr1(x)>max){
        max=arr1(x)
      }

    }
    println("Max in Array is "+max)
    //Concat 2 arrays
    import Array._
    val arr3=concat(arr1,arr2)
    for(x<-0 to arr3.length - 1){
      println(x)
    }
    //Create array using range
    val arr4=range(1,10,3)
    println("Range Aray with gap of 3")
    for(x<-arr4){
      println(x)
    }


  }

}
