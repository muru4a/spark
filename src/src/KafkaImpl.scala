import java.util.Properties

import common.ConfigurationManager
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * This is the Kafka implementation
  *
  * @author Murugesan Alagusundaram
  */
object KafkaImpl extends Logging {

  private val KafkaBroker = ConfigurationManager.getConfigString("constants.kafka.broker")
  private val KafkaTopic = ConfigurationManager.getConfigString("constants.kafka.topics")

  def writeKafkaData(dataDF: DataFrame) (implicit sparkContext: SparkContext): Unit = {

  val props = new Properties()
  props.put("bootstrap.servers", KafkaBroker)
  props.put("key.serializer", classOf[StringSerializer].getCanonicalName)
  props.put("value.serializer", classOf[StringSerializer].getCanonicalName)

  val kafkaWrapper = sparkContext.broadcast(KafkaWrapper(props))

  dataDF.toJSON.foreachPartition(partition => {
  partition.foreach(message => {
  logDebug(s"message = [$message]")
  kafkaWrapper.value.send(KafkaTopic, message)
})
})
}

}
