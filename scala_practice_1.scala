package testing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import java.util.Calendar

object scala_practice_1 {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder().appName("read_bank_data").master("local[*]").getOrCreate()
    //val schema="CustomerId Int,Surname String"
    val df1=spark.read.format("csv")
      .option("inferSchema","true")
      .option("header","true")
      .option("spark.sql.shuffle.partitions","3")
      .load("/home/sayyad/Downloads/Datasets and pdfs/bank.csv")

    //println(df1.rdd.getNumPartitions)

    val df2=df1.select(col("CustomerId"),col("Surname"),col("CreditScore"))
    val df3=df1.select(col("CustomerId"),col("Geography"),col("Gender"),col("Age"),
      col("Tenure"),col("Balance"),col("HasCrCard"),col("EstimatedSalary"))

    //df2.cache() is an alias for persist(StorageLevel.MEMORY_ONLY)
    val now = Calendar.getInstance()
    val t1 = now.get(Calendar.MILLISECOND)

    //val df4=df2.persist(StorageLevel.MEMORY_ONLY)
    //val df5=df3.repartition(5)

    /*val join_df=df2.join(df3,df2.col("CustomerId")===
      df3.col("CustomerId"),"inner").drop(df2.col("CustomerId"))

    val t2 = now.get(Calendar.MILLISECOND)

    join_df.select("CustomerId","Surname","CreditScore","Geography","Gender").show(5)

     */

    //println(t1,t2)
    //https://docs.databricks.com/spark/latest/spark-sql/udf-scala.html

    //val udf1=udf((x:Int)=>if (x>750) "pass" else "fail")
    //df2.withColumn("credit*2",udf1(df2.col("CreditScore"))).show(5)
    //df3.select("Geography").distinct().show()

    //broadcast variables
    /*val lookup=Map("Germany"->"Berlin","France"->"Paris","Spain"->"Madrid")
    //println(broad("Germany"))
    val sc: SparkContext = spark.sparkContext
    //sc.setLogLevel("ERROR")
    val broad=sc.broadcast(lookup)
    val UDF=udf((x:String)=>broad.value(x))
    df3.withColumn("Capital",UDF(df3.col("Geography"))).show*/

    //Accumulators
    //Long Accumulator
    //Double Accumulator
    //Collection Accumulator
    /*val sc: SparkContext = spark.sparkContext
    val accm=sc.longAccumulator("acc1")
    println(accm.value)

    val name=df2.select("Surname")
    for(x<-name) {
      if(x.mkString.startsWith("S")) {
        println(x)
        accm.add(1)
      }
    }
    println(accm.value)*/
    //when.otherwise
    //df2.withColumn("CreditResult",when(col("CreditScore")>750,"pass").otherwise("fail")).show(10)
    //df2.withColumn("CreditResult",when(col("CreditScore").startsWith("6"),"pass").otherwise("fail")).show(10)
    //df2.withColumn("CreditResult",when(col("CreditScore").contains(0),5).otherwise(col("CreditScore"))).show(10)
    //df2.withColumn("Surname_",concat(col("Surname"),lit("_abc"))).show(5)
    //df2.withColumn("replace_H",expr("replace(Surname,'i','e')")).show(5)
    //lit - https://harshitjain.home.blog/2019/09/26/spark-sql-functions-lit-and-typedlit/
    //df2.withColumn("CreditTest",when(col("CreditScore")>750,lit(1).cast(IntegerType)).otherwise(lit(0).cast(IntegerType))).show(5)
    val products=spark.read.format("csv")
      .option("inferSchema","true")
      .option("header","true")
      .load("/home/sayyad/products.csv")
    //products.printSchema()
    //products.show
    //find duplicate
    //products.groupBy("p_name","price","discount","price").count.where("count>1").show()
    //find distinct
    //products.select("p_name","price","discount","price").distinct().show
    /*products.withColumn("row_number",
      row_number().over(Window.partitionBy("p_name").orderBy("p_name","price"))).
      withColumn("rank",
        rank().over(orderBy("p_name","price")))
      .withColumn("dense_rank",
        dense_rank().over(orderBy("p_name","price"))).*/
    //partition by and group by columns must be same to find duplicates
    /*products.withColumn("row_number",
      row_number().over(Window.partitionBy("p_name","price")
        .orderBy("p_name","price")))
      //.filter("row_number>1") .show//duplicates
      .filter("row_number=1") .show//distinct*/
    /*products.withColumn("row_number",
      row_number().over(Window.partitionBy("p_name")
        .orderBy("p_name")))
    .filter("row_number=1") .show*/






  }

}
