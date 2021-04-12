package interview.prep.com
package inter_1

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object besic_1 {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)
  def main(args: Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().appName("scala").master("local[4]").getOrCreate()
    //val df1=spark.read.option("header",true).option("inferSchema",true).csv("E:\\Salman\\Hive-Data\\actor.csv")
    val df1=spark.read.option("header",true).option("inferSchema",true).csv("C:\\Users\\Sayyad Manzil\\Desktop\\superstore.csv")
    //import org.apache.spark.sql.functions.to_date
    import spark.implicits._
    import org.apache.spark.sql.functions.regexp_replace
    val df2=df1.withColumn("Order Date",to_date(col("Order Date"),"dd-MMM-yy"))
    //val df3= df2.withColumn("Discount",regexp_replace(col("Discount"),"%",""))
    val df3 = df2.withColumn("Discount",expr("replace(Discount, '%', '')"))
    val df4 = df3.withColumn("Sales",expr("replace(Sales, '$', '')"))
    val df5 = df4.withColumn("Profit",expr("replace(Profit, '$', '')"))
    val df6=df5.withColumn("Profit",col("Profit").cast(FloatType)).
      withColumn("Sales",col("Sales").cast(FloatType)).
      withColumn("Discount",col("Discount").cast(FloatType))

    //DataFrame operations that trigger shufflings are join(), union() and all aggregate functions.
    //spark.sql.shuffle.partitions which is by default set to 200.
    //val df7 = df6.groupBy("Category").count()
    //println(df7.rdd.getNumPartitions)

    //spark.conf.set("spark.sql.shuffle.partitions",100)
    //println(df6.groupBy("Category").count().rdd.partitions.length)

    //Spark submit - https://sparkbyexamples.com/spark/spark-submit-command/

    /* Shuffle partition size
Based on your dataset size, number of cores, and memory, Spark shuffling can benefit or harm your jobs.
When you dealing with less amount of data, you should typically reduce the shuffle partitions otherwise you will
end up with many partitioned files with a fewer number of records in each partition. which results in running many
tasks with lesser data to process.

On other hand, when you have too much of data and having less number of partitions results in fewer longer running
tasks and some times you may also get out of memory error.

Getting a right size of the shuffle partition is always tricky and takes many runs with different value to achieve the
optimized number. This is one of the key property to look for when you have performance issues on Spark jobs. */

    val df8=df6.withColumnRenamed("Customer Name","Customer_Name").
      withColumnRenamed("Number of Records","Number_of_Records").
      withColumnRenamed("Order Date","Order_Date").
      withColumnRenamed("Product Name","Product_Name")

    import spark.implicits._
    val df9 = df8.na.fill(115.0,Seq("Sales","Discount","Profit"))
    val df10=df9.repartition(5)
    df10.write.mode("overwrite").option("header",true).parquet("C:\\Users\\Sayyad Manzil\\Desktop\\Spark_Data\\superstore.parquet")

    /*val old_columns = Seq("Customer Name","Number of Records","Order Date","Product Name")
    val new_columns = Seq("Customer_Name","Number_of_Records","Order_Date","Product_Name")
    val columnsList = old_columns.zip(new_columns).map(f=>{col(f._1).as(f._2)})
    val df8 = df6.select(columnsList:_*) */


    //df6.printSchema()
    df9.show(false)





    /*println(df1.rdd.partitions.length)
    val df2 = df1.repartition(6)
    val df3 = df2.coalesce(4)
    println(df2.rdd.partitions.length)
    println(df3.rdd.partitions.length)*/



  }

}
