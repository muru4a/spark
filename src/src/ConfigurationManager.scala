import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Manages application wide configuration loading and availing by associating
  * with a pre-loaded properties file in HDFS that holds all application
  * configuration property keys and values.
  *
  * @author Murugesan Alagusundaram
  */
object ConfigurationManager {

  private val config: Properties =
    synchronized {
      val propFilepath = "hdfs://mars-hdp-6399.ccg21.dev.paypalcorp.com:8020/apps/dt/merchant/Utility/Utility.properties"

      val path = new Path(propFilepath)

      val fs = FileSystem.get(new Configuration)

      val inputStream = fs.open(path)

      val props = new Properties

      props.load(inputStream)

      props
    }

  def getConfigString(key: String): String = {
    config.getProperty(key)
  }

}
