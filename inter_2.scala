package interview.prep.com
package inter_1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import scala.io.Source

object inter_2 {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)
  def main(args: Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().config(getConfig).getOrCreate()


    if(args.length==0){
      logger.error("No file has been found")
      System.exit(1)

    }
    val df1=load_df(spark,args(0))
    //df1.printSchema()
    //df1.show(5)
    df1.write.mode("overwrite").csv("C:\\Users\\Sayyad Manzil\\Desktop\\Spark_Data\\sales.csv")

  }

  def getConfig:SparkConf = {
    val config=new SparkConf
    val prop=new Properties
    prop.load(Source.fromFile("spark.conf").bufferedReader())
    prop.forEach((k,v)=>config.set(k.toString,v.toString))
    config

  }
  def load_df(x:SparkSession,y:String):DataFrame={
    val schema="actor_id int,first_name STRING,last_name string,last_update DATE"
    x.read.option("header",true).schema(schema).csv(y)
  }


}
