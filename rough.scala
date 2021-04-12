package testing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object rough {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().appName("test")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir","hdfs://localhost:9000/user/hive/warehouse/")
      .getOrCreate()
    val df=spark.read.format("csv")
      .option("InferSchema","true")
      .option("header","true")
      .load("/home/sayyad/Downloads/Datasets/supermarket_sales.csv")
    //df.printSchema()
    //df.show(5)
    val df1=df.withColumnRenamed("Invoice ID","id")
      .withColumnRenamed("Customer type","customer_type")
      .withColumnRenamed("Product line","name")
      .withColumnRenamed("Unit price","price")
      .withColumnRenamed("Tax 5%","tax")
      .withColumnRenamed("gross margin percentage","gmp")
      .withColumnRenamed("gross income","gross_income")


    val customer=df1.select("id","Branch","City","customer_type","Gender")

    val product=df1.select("id","name","price","Quantity","tax","total")

    val rating=df1.select("id","Date","Payment","cogs","gmp","gross_income","Rating")

    spark.sql("use demo")
    spark.sql("create table customer(id string,branch string,city string,c_type string,gender string)" +
      "row format delimited" +
      "fields terminated by ','")
    customer.write.mode("append").format("orc").saveAsTable("customer")
    customer.show()
    spark.sql("select * from customer").show


    /*
    df1.write.csv("/home/sayyad/Downloads/Datasets/sales_table")
    customer.write.csv("/home/sayyad/Downloads/Datasets/customer")
    product.write.csv("/home/sayyad/Downloads/Datasets/product")
    rating.write.csv("/home/sayyad/Downloads/Datasets/rating")

     */



  }

}
