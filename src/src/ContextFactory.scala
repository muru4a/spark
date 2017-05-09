

import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext


object ContextFactory extends Logging {

  private val APPNAME = "spark_projects"
  private val HiveConfPartitionMode = "hive.exec.dynamic.partition.mode"
  private val HiveConfPartitionModeNonStrict = "nonstrict"

  private val sparkContext = {
    logInfo("Creating Spark Context...")
    val conf = new SparkConf().setAppName(APPNAME)
    new SparkContext(conf)
  }

  private val sqlContext = {
    logInfo("Creating SQL Context...")

    val sqlContext = new SQLContext(sparkContext)
    sparkContext.broadcast[SQLContext](sqlContext)
  }

  private val hc = {
    logInfo("Creating Hive Context...")

    val hc = new HiveContext(sparkContext)

    // Turn dynamic partition strict mode off for dynamic partition column load
    hc.setConf(HiveConfPartitionMode, HiveConfPartitionModeNonStrict)

    sparkContext.broadcast[HiveContext](hc)
  }

  def getSQLContext: SQLContext = {
    sqlContext.value
  }

  def getSparkContext: SparkContext = {
    sparkContext
  }

  def getHiveContext: HiveContext = {
    hc.value
  }
  
  






}
