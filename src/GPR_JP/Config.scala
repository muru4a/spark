package com.groupon.edw.gpr.common

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

object Config {

  private val config: Properties =
    synchronized {

      val propFilepath = "hdfs://gdoop-namenode/user/grp_gdoop_edw_etl_prod/GPR/GPR.properties.prod"
      //Dev: val propFilepath = "hdfs://gdoop-staging-namenode/user/grp_gdoop_edw_etl_dev/GPR/GPR.properties.dev"
      //Prod: val propFilepath = "hdfs://gdoop-namenode/user/grp_gdoop_edw_etl_prod/GPR/GPR.properties.prod"

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
