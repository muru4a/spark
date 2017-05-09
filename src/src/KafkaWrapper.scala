import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.Logging

/**
  * A smart wrapper of a
  * [[org.apache.kafka.clients.producer.KafkaProducer KafkaProducer]]. The
  * wrapped KafkaProducer is initialized just before its first use. It also
  * takes care of closing the KafkaProducer object at JVM shutdown.
  *
  * @param config KafkaProducer configuration.
  *
  * @author Murugesan Alagusundaram
  */
case class KafkaWrapper(config: Properties) extends Logging {

  private lazy val producer = {
    val kp = new KafkaProducer[String, String](config)

    sys.addShutdownHook {
      kp.close()
    }

    kp
  }

  /**
    * Abstracts the creation of a kafka producer record, given the topic and
    * value of the record.
    *
    * @param topic The topic of the record.
    * @param value The value of the record.
    */
  def send(topic: String, value: String): Unit = {
    logDebug(s"topic:::value = [$topic]:::[$value]")
    producer.send(new ProducerRecord(topic, value))
  }

}
