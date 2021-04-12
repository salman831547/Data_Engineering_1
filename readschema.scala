package interview.prep.com
package InferSchema
import interview.prep.com.sparkAtoZ.sparkByexamples.getClass
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.expressions.Window

object readschema {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)
  def main(args: Array[String]):Unit={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder.master("local[*]").appName("schema").getOrCreate()
    //val schema="actor_id int,first_name string,last_name string,last_update timestamp"
    val df1=spark.read.option("header",true).option("inferSchema",true).csv("C:\\Users\\Sayyad Manzil\\Desktop\\duplicate.csv")
    val df2=spark.read.option("header",true).option("inferSchema",true).csv("E:\\Salman\\Hive-Data\\city.csv")

    /*df1.printSchema()
    df1.show(5)
    df2.printSchema()
    df2.show(5)*/
    //df1.groupBy("Name","Age","Salary","City").count().where("count>1").show()
    //val win=Window.partitionBy("Name").orderBy("Age","Salary")
    //df1.withColumn("row_num",row_number().over(win)).filter("row_num>1").show()
    //df1.withColumn("row_num",row_number().over(win)).filter("row_num>1").show()
    //df1.withColumn("rank",rank().over(win)).show()
    //df1.withColumn("rank",dense_rank().over(win)).show()
    //println( spark.conf.get("spark.sql.autoBroadcastJoinThreshold").toInt/1024/1024)
    //BroadCast Join and SortMerge Join
    //df1.join(df2,df1("actor_id")<=>df2("city_id")).explain() //default is broadcast join,table size less than = 10 mb
    import org.apache.spark.sql.functions.broadcast
    //df1.join(broadcast(df2),df1("actor_id")<=>df2("city_id")).explain()//Explicit broadcast join
    //spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)//disable autobroadcast join
    //spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 20485760)
    //spark.conf.set("spark.sql.shuffle.partitions",3)
    //df1.join(df2,df1("actor_id")<=>df2("city_id")).explain()
    //df1.createOrReplaceTempView("table1")
    //spark.sql("select * from table1 limit 5").show()
    /*spark.sql("select Name,Salary,row_number() over(partition by Name order by Salary) as row_number," +
      "rank() over(partition by Name order by Salary) as rank," +
      "dense_rank() over(partition by Name order by Salary) as dense_rank from table1").show()*/
    import spark.implicits._
  //df1.select("City").withColumn("isNull_City", col("City").isNull).where("isNull_City = true").show
    df1.withColumn("isNull_City", col("City").isNull).where("isNull_City = true").show










  }

}
