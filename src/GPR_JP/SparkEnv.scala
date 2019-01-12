package com.groupon.edw.gpr.common

  import org.apache.hadoop.fs.FileSystem
  import org.apache.spark.sql.{SQLContext, SparkSession}
  import org.apache.spark.{SparkConf, SparkContext}

  object SparkEnv {

    object SparkConfig {
      val sparkConf: SparkConf = new SparkConf()
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer.max","1024m")
        .set("spark.kryoserializer.buffer", "512m")
        .set("spark.akka.frameSize", "1000")
        .set("spark.driver.maxResultSize", "4g")
        .set("spark.hadoop.mapred.output.compress", "true")
        .set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
        .set("spark.hadoop.mapred.output.compression.type", "BLOCK")
        .set("es.http.timeout", "30s")
        .set("es.scroll.size", "50")
        .set("es.index.auto.create", "true")
        .setAppName("GprTrafficAggreagtes")

      val sc = SparkContext.getOrCreate(sparkConf)

      val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

      hiveContext.setConf("hive.exec.dynamic.partition","true")
      hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

      val hadoopFileSystem = FileSystem.get(sc.hadoopConfiguration)

      val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    }


}
