package Learning.journal.com
package Scala_Journal
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import scala.io.Source

object Scala_pro extends Serializable {

  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)

  def main (args: Array[String]):Unit = {

    if(args.length==0){
      logger.error("No file has been found")
      System.exit(1)

    }

    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession.builder().config(getConfig).getOrCreate()

    val df=load_df(spark,args(0))
    val partitioned_df=df.repartition(2)//update in spark.config because we don't
    //know, how many partitions driver is gonna create after groupBy shuffle sort.
    val group_result=group_by_cut(partitioned_df)

    logger.info(group_result.collect.mkString("->"))

    logger.info("Starting Learning journal")
    logger.info("Spark config"+spark.conf.getAll.toString())
    logger.info("Stopping Learning journal")
    //scala.io.StdIn.readLine()//hold UI for local debugging only, remove once done with spark UI
    spark.stop()

  }

  def getConfig:SparkConf = {
    val config=new SparkConf
    val prop=new Properties
    prop.load(Source.fromFile("spark.config").bufferedReader())
    prop.forEach((k,v)=>config.set(k.toString,v.toString))
    config

  }
  def load_df(x:SparkSession,y:String):DataFrame={
    x.read
      .option("header",true)
      .option("inferSchema",true)
      .csv(y)

  }
def group_by_cut(x:DataFrame):DataFrame={
  x.where("carat=0.23")
    .select("cut","color","clarity","carat")
    .groupBy("cut").count()

}

}
