package testing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object file_format {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder().appName("orc_format")
      .master("local[4]").getOrCreate()
    val df1=spark.read.format("parquet")
      .option("inferSchema","true")
      .load("/home/sayyad/study_material/PARQUET_files")
    df1.printSchema()
    df1.show()

    //df1.write.parquet("/home/sayyad/study_material/PARQUET_files")

  }

}
