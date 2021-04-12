package Learning.journal.com
package Scala_Journal
import Learning.journal.com.Scala_Journal.read_date.getClass
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Transformations_and_Actions {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)
  def main(args: Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().appName("Trans and actions").
      master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val rdd=sc.textFile("C:\\Users\\Sayyad Manzil\\Desktop\\Python_all_scripts\\youtube_links\\Spark_links.txt")
    //df.printSchema()
    //df.show(false)
    val reparRdd = rdd.repartition(4)
    println(reparRdd)
    //flatmap.collect().foreach(println)

    // rdd flatMap transformation
    val rdd2 = rdd.flatMap(f=>f.split(" "))
    rdd2.foreach(f=>println(f))

    //rdd Map transformation
    val rdd3= rdd2.map(m=>(m,1))
    rdd3.foreach(println)

    //Filter transformation
    val rdd4 = rdd3.filter(a=> a._1.startsWith("a"))
    rdd4.foreach(println)

    //ReduceBy transformation
    val rdd5 = rdd3.reduceByKey(_ + _)
    rdd5.foreach(println)

    //Swap word,count and sortByKey transformation
    val rdd6 = rdd5.map(a=>(a._2,a._1)).sortByKey()
    println(rdd6)


  }

}
