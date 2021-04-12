import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions._

object test {

  def main(args:Array[String]): Unit ={
    val spark=SparkSession.builder().master("local[*]").appName("demo1").getOrCreate()
    val df1=spark.read.option("header",value = true).option("inferSchema",value = true).
      csv("/home/sayyad/Desktop/supermarket.csv")
    import  spark.implicits._
    val df2=df1.withColumn("Date",to_date(col("Date"),"dd/MM/yyyy"))
    df2.createOrReplaceTempView("order")
    //spark.sql("select distinct * from (select City,sum(Total) over(partition by City order by Total rows between unbounded preceding and unbounded following) as total_sum from order order by City)").show()
    spark.sql("select City,Total,rank() over(partition by City order by City) as row_number from order").show()
  }

}
