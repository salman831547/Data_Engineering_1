package testing
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object read_text {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder().appName("read_txt").master("local[4]").getOrCreate()
    val df1=spark.read.textFile("/home/sayyad/study_material/demo.txt")
    df1.show()

  }

}
