import common.ConfigurationManager
import tools._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * This is the application driver that controls the flow of execution writing
  * Hive Datasets and computations to Kafka.
  *
  * @author Murugesan Alagusundaram
  */
object HivetoESJSONDriver extends Logging {

  private val Store = ConfigurationManager.getConfigString("constants.es.values.store")
  private val paritionlable = ConfigurationManager.getConfigString("tables.hive.partitionlable")

  def main(args: Array[String]) {

    // Throw an exception if the expected arguments are missing
    if (args.length < 1) throw new IllegalArgumentException("Usage: com.paypal.daputility.driver.HivetoESJSONDriver [date]")

    // Input Hive Table Name, partition date
    val InputHiveTblName = args(0)
    val partitionDate = args(1)

    logInfo("Creating Spark Context...")
    val conf = new SparkConf().setAppName("HivetoESJSON")
    implicit val sparkContext = new SparkContext(conf)

    implicit val hc = new HiveContext(sparkContext)

    val TableDF = hc.table(InputHiveTblName)

    // GET Hive Table Data
    val InputTableDF = TableDF.filter(TableDF(paritionlable) === partitionDate)

    // Convert Hive Data to Elasticsearch compliant schema
    val EsSchemaDF = ElasticSearchSchema.getESCompliantDF(InputTableDF,Store)

    // Write Elasticsearch Dataframe to kafka
    KafkaImpl.writeKafkaData(EsSchemaDF)
  }

}
