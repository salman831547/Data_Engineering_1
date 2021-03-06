package kafka_demo
import io.netty.util.Timeout

import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.time.Duration
import scala.collection.JavaConverters._

object consumer extends  App {
  import java.util.Properties

  val TOPIC="topic_1"

  val  props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(util.Collections.singletonList(TOPIC))

  while(true){
    val records=consumer.poll(Duration.ofMillis(1000))
    for (record<-records.asScala){
      println(record)
    }
  }

}
