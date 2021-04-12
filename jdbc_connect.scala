package Spark_examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object jdbc_connect {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().appName("jdbc connect")
      .master("local[4]").getOrCreate()

    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("dbtable", "employee")
      .option("user", "root")
      .option("password", "Passw0rd")
      .load()
    df.printSchema()
    df.show()
  }


}
