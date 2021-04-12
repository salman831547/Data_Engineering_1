package testing
import org.apache.spark.sql.SparkSession
object word_count {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Word Count")
      .getOrCreate()

    val lines = spark.sparkContext.parallelize(
      Seq("Spark Intellij Idea Scala test one",
        "Spark Intellij Idea Scala test two",
        "Spark Intellij Idea Scala test three"))

    val counts = lines
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreach(println)
  }

}