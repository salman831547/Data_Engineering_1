package Spark_examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object connect_mysql {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)

   def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder().master("local[3]").appName("sql_connect")
      .getOrCreate()

    val sql_df= spark.read.format("jdbc")
      .option("driver","org.postgresql.Driver")
      .option("url","jdbc:postgresql://localhost:5432/demo")
      .option("dbtable","employee")
      .option("user","postgres")
      .option("password","postgres")
      .load()

    sql_df.show()


  }

}
