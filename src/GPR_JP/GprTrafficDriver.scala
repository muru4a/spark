package com.groupon.edw.gpr.traffic.driver

import java.text.SimpleDateFormat
import java.util.Date
import com.groupon.edw.gpr.common.SparkEnv
import com.groupon.edw.gpr.traffic.{GprTraffic, GprTrafficDeal, GprTrafficL1, GprTrafficL2}
import org.slf4j.{Logger, LoggerFactory}

/**
  * GPR traffic Driver build all the traffic aggregate tables
  *
  * @author Murugesan Alagusundaram
  *
  */

object GprTrafficDriver {
  def main(args: Array[String]): Unit = {

    val LOGGER = LoggerFactory.getLogger(getClass)

    // Throw an exception if the expected arguments are missing
    if (args.length < 3) throw new IllegalArgumentException("Usage: com.groupon.edw.gpr.traffic [startDate] [endDate] [tablename]")

    //Input parameters
    val startDate = args(0)
    val endDate = args(1)
    val tablename = args(2)


    //Get all the source tables
    val dimGblDealLobDf = SparkEnv.SparkConfig.spark.sql("select distinct grt_l1_cat_name,grt_l2_cat_name,deal_id from edwprod.dim_gbl_deal_lob")
    val gblTrafficSuperFunnelDeal = SparkEnv.SparkConfig.spark.sql(s"""select * from prod_groupondw.gbl_traffic_superfunnel_deal where event_date   BETWEEN '$startDate' and '$endDate'""")
    val gblBotCookies = SparkEnv.SparkConfig.spark.sql(s"""select * from prod_groupondw.gbl_bot_cookies where event_date  BETWEEN '$startDate' and '$endDate'""")
    val dimDay = SparkEnv.SparkConfig.spark.sql("select * from prod_groupondw.dim_day")
    val dimWeek = SparkEnv.SparkConfig.spark.sql("select * from prod_groupondw.dim_week")
    val gblDimPlatform = SparkEnv.SparkConfig.spark.sql("select * from prod_groupondw.gbl_dim_platform")
    val gblDimCountry = SparkEnv.SparkConfig.spark.sql("select * from prod_groupondw.gbl_dim_country_orc")
    val gblDimEconomicArea = SparkEnv.SparkConfig.spark.sql("select * from prod_groupondw.gbl_dim_economic_area")
    val gblDimRegion = SparkEnv.SparkConfig.spark.sql("select * from prod_groupondw.gbl_dim_region")
    val gblTrafficSuperFunnel = SparkEnv.SparkConfig.spark.sql(s"""select * from prod_groupondw.gbl_traffic_superfunnel where event_date BETWEEN '$startDate' and '$endDate'""")

    //Create Temp View on Source tables
    dimGblDealLobDf.createOrReplaceTempView("dim_gbl_deal_lob")
    gblTrafficSuperFunnel.createOrReplaceTempView("gbl_traffic_superfunnel")
    gblTrafficSuperFunnelDeal.createOrReplaceTempView("gbl_traffic_superfunnel_deal")
    gblBotCookies.createOrReplaceTempView("gbl_bot_cookies")
    dimDay.createOrReplaceTempView("dim_day")
    dimWeek.createOrReplaceTempView("dim_week")
    gblDimPlatform.createOrReplaceTempView("gbl_dim_platform")
    gblDimCountry.createOrReplaceTempView("gbl_dim_country")
    gblDimEconomicArea.createOrReplaceTempView("gbl_dim_economic_area")
    gblDimRegion.createOrReplaceTempView("gbl_dim_region")

    LOGGER.info(s"Build the Traffic Aggregate")

    if (tablename == "agg_gbl_traffic") {
      //Build Traffic Aggregate
      println("Building Traffic Aggregate ")
      GprTraffic.buildAggGblTraffic(startDate, endDate)
      println("Completed Traffic Aggregate")
    }
    else if (tablename == "agg_gbl_traffic_l1") {
      //Build Trafficl1 Aggregate
      println("Building Trafficl1 Aggregate ")
      GprTrafficL1.buildAggGblTrafficL1(startDate, endDate)
      println("Completed Trafficl1 Aggregate")
    }
    else if (tablename == "agg_gbl_traffic_l2") {
      //Build Trafficl2 Aggregate
      println("Building Trafficl2 Aggregate ")
      GprTrafficL2.buildAggGblTrafficL2(startDate, endDate)
      println("Completed Trafficl2 Aggregate")
    }
    else if (tablename == "agg_gbl_traffic_deal") {

      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val date1 = sdf.parse(args(0))
      val date2 = sdf.parse(args(1))
      val diff: Int = ((date2.getTime - date1.getTime) / 86400000).asInstanceOf[Int]
      for {
        i <- (0 to diff)
        date = new Date(date1.getTime + 86400000 * i)
      } {
        println("Building TrafficlDeal Aggregate ")
        GprTrafficDeal.buildAggGblTrafficDeal(sdf.format(date), sdf.format(date))
        println("Completed TrafficlDeal Aggregate")
      }

    }

    LOGGER.info(s"Completed all the Aggreagte")
    SparkEnv.SparkConfig.sc.stop()

  }

}
