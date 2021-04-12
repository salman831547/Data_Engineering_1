package testing

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
//https://gist.github.com/fancellu/f78e11b1808db2727d76
import java.util.Properties

object producer extends Serializable {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val  props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val TOPIC="test"

    for(i<- 1 to 50){
      val record = new ProducerRecord(TOPIC,"", s"hello $i")
      producer.send(record)
    }

    val record = new ProducerRecord(TOPIC, "key", "the end "+new java.util.Date)
    producer.send(record)

    producer.close()


  }


}
