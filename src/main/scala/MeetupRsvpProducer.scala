import java.net.URL
import java.util.Properties
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object MeetupRsvpProducer {
  def main(args: Array[String]): Unit = {
    val url = new URL("http://stream.meetup.com/2/rsvps")
    val conn = url.openConnection()
    val jsonFactory = new JsonFactory(new ObjectMapper)
    val parser = jsonFactory.createParser(conn.getInputStream)

    val props = new Properties()
    props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")

    val kafkaProducer = new KafkaProducer[String, String](props)

    while (parser.nextToken() != null) {
      val record = parser.readValueAsTree().toString()
      val producerRecord = new ProducerRecord[String, String]("meetup-rsvp", record)
      kafkaProducer.send(producerRecord)
    }
    kafkaProducer.close()
  }
}

// To Run this file:
// spark-submit --class MeetupRsvpProducer --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --master local[*] ./meetuprsvp_2.12-0.1.jar
