package com.groupon.edw.gpr.traffic

import com.groupon.edw.gpr.common.{Config, SparkEnv}
import com.groupon.edw.gpr.common.SparkEnv.SparkConfig.hiveContext._

/**
  * GPR trafficDeal calculates the trafficDeal aggregate metrics
  *
  * @author Murugesan Alagusundaram
  *
  */

object GprTrafficDeal {

  //output Hdfs directory
  val gdoopEdwDir=Config.getConfigString("gdoopEdwDir")
  val tgtSchema=Config.getConfigString("tgtSchema")

  def buildAggGblTrafficDeal(startDate : String, endDate : String)= {


    val sql = SparkEnv.SparkConfig.spark.sql _

    val stgGblTrafficDealMetricsQry  =
      s"""
         |  SELECT
         |     a.event_date as report_date
         |    ,cookie_first_country_code   country_code
         |    ,cookie_first_country_key   country_id
         |    ,CASE
         |           WHEN cookie_first_country_code in ('JP') then 2
         |           WHEN cookie_first_country_code in ('US') then 1
         |           WHEN cookie_first_country_code in ('CA') then 2
         |           ELSE 2
         |     END   platform_key
         |    ,CASE WHEN cookie_first_platform  IS NULL AND LENGTH(cookie_first_platform) = 0 THEN 'unknown' ELSE cookie_first_platform END platform
         |    ,CASE WHEN cookie_first_sub_platform IS NULL AND LENGTH( cookie_first_sub_platform ) =0 THEN 'unknown'  ELSE cookie_first_sub_platform END  sub_platform
         |    ,CASE WHEN cookie_first_traf_source   IS NULL  AND LENGTH(cookie_first_traf_source) = 0 THEN 'unknown' ELSE cookie_first_traf_source END  traffic_source
         |    ,CASE WHEN cookie_first_traf_sub_source   IS NULL AND LENGTH(cookie_first_traf_sub_source) =0  THEN 'unknown' ELSE cookie_first_traf_sub_source END   traffic_sub_source
         |    ,CASE WHEN cookie_ref_attr_class_key   IS NULL   THEN 28 ELSE cookie_ref_attr_class_key END  ref_attr_class_key
         |    ,deal_uuid   deal_id
         |    ,cookie_first_brand  brand
         |    ,count( distinct a.unique_dv_visitors )  AS uniq_deal_view_visitors
         |    ,count( distinct a.unique_usr_buy_button_clks ) AS uniq_buy_btn_clickers
         |    ,count( distinct COALESCE(a.unique_cart_chkout_viewers, a.unique_conf_page_viewers) ) AS uniq_conf_page_visitors_gl
         |    ,count( distinct a.unique_cart_chkout_viewers) AS uniq_cart_chkout_visitors
         |    ,count( distinct a.unique_conf_page_viewers) AS uniq_conf_page_visitors
         |    ,count( distinct a.unique_cart_summary_viewers ) AS uniq_cart_summarypage_visitors
         |    ,count( distinct a.unique_cart_receipt_viewers) AS uniq_cart_conf_page_visitors
         |    ,count( distinct COALESCE(a.unique_usr_cart_comp_clks, a.uniq_usr_buy_but_final_clks )) AS uniq_comp_ordr_btn_clckrs_gl
         |    ,count( distinct a.unique_usr_cart_comp_clks ) AS uniq_user_cart_comp_clickers
         |    ,count( distinct a.uniq_usr_buy_but_final_clks )  AS uniq_usr_buy_butt_finl_clckrs
         |    ,count( distinct a.unique_purchasers ) AS uniq_receipt_page_visitors
         |    ,SUM(a.unique_deal_views) AS uniq_deal_views
         |    ,SUM(a.unique_buy_button_clks) AS uniq_buy_btn_clicks
         |    ,SUM(a.unique_cart_chkout_views) AS uniq_cart_chkout_views
         |    ,SUM(a.unique_conf_page_views) AS uniq_conf_page_views
         |    ,SUM(a.unique_cart_summary_views) AS uniq_cart_summary_page_views
         |    ,SUM(a.unique_cart_receipt_views) AS uniq_cart_conf_page_views
         |    ,SUM(a.unique_cart_comp_clks) AS uniq_cart_comp_clicks
         |    ,SUM(a.unique_buy_but_final_clks) AS uniq_buy_butt_final_clicks
         |    ,SUM(a.unique_receipt_page_views) AS uniq_receipt_page_views
         |    ,SUM(a.deal_views) AS deal_views
         |    ,SUM(a.buy_button_clicks) AS buy_btn_clicks
         |    ,SUM(a.confirm_page_views) AS conf_page_views
         |    ,SUM(0) AS cart_summary_page_views
         |    ,SUM(a.cart_receipt_views) AS cart_conf_page_views
         |    ,SUM(a.comp_order_button_clicks) AS comp_order_btn_clicks
         |    ,SUM(a.receipt_page_views) AS receipt_page_views
         |    ,MAX(CASE WHEN a.ind_new_visitor IS NOT NULL THEN 1 ELSE 0 end) AS new_visitor
         |    ,sum(a.unique_available_dvs) AS uniq_available_deal_views
         |    ,sum(a.available_deal_views) AS available_deal_views
         |    ,SUM(a.page_views) AS page_views
         |    ,SUM(a.actual_page_views) AS actual_page_views
         |    ,SUM(0) AS page_views_bounce
         |    FROM
         |    gbl_traffic_superfunnel_deal as a
         |    left  join gbl_bot_cookies b
         |    on a.cookie_b = b.cookie_b
         |    AND (b.flag_ip=1 OR b.flag_self_id =1)
         |    and a.event_date = b.event_date
         |    WHERE a.event_date BETWEEN '$startDate' and '$endDate'
         |    AND cookie_first_country_code in ('AU','BE','CA','FR','DE','IE','IT','JP','NL','NZ','PL','ES','AE','QC','UK','US')
         |    AND b.cookie_b IS NULL
         |    GROUP BY
         |    a.event_date
         |    ,cookie_first_country_code
         |    ,cookie_first_country_key
         |    ,case
         |           WHEN cookie_first_country_code in ('JP') then 2
         |           WHEN cookie_first_country_code in ('US') then 1
         |           WHEN cookie_first_country_code in ('CA') then 2
         |           ELSE 2
         |    END
         |    ,CASE WHEN cookie_first_platform  IS NULL AND LENGTH(cookie_first_platform) = 0 THEN 'unknown' ELSE cookie_first_platform END
         |    ,CASE WHEN cookie_first_sub_platform IS NULL AND LENGTH( cookie_first_sub_platform ) =0 THEN 'unknown'  ELSE cookie_first_sub_platform END
         |    ,CASE WHEN cookie_first_traf_source   IS NULL  AND LENGTH(cookie_first_traf_source) = 0 THEN 'unknown' ELSE cookie_first_traf_source END
         |    ,CASE WHEN cookie_first_traf_sub_source   IS NULL AND LENGTH(cookie_first_traf_sub_source) =0  THEN 'unknown' ELSE cookie_first_traf_sub_source END
         |    ,CASE WHEN cookie_ref_attr_class_key   IS NULL   THEN 28 ELSE cookie_ref_attr_class_key END
         |    ,deal_uuid
         |    ,cookie_first_brand
         |
      """.stripMargin

    val stgGblTrafficDealMetricsQryDf = sql(stgGblTrafficDealMetricsQry)

    stgGblTrafficDealMetricsQryDf.createOrReplaceTempView("stg_gbl_traffic_deal_metrics")

    println("******** Loading agg_gbl_traffic_deal for date:","$startDate")

    val aggGblTrafficDealQry =
      s"""
         |SELECT
         | c.week_of_year_num AS report_week
         |, MONTH (a.report_date) AS report_month
         |, substr(c.WEEK_END,1,10) AS wk_end
         |, b.year_key AS report_year
         |, a.country_code
         |, a.country_id
         |, reg.region AS region
         |, reg.region_long_desc AS region_long_desc
         |, eca.economic_area AS economic_area
         |,  CASE
         | WHEN LOWER(a.platform) ='web' THEN a.platform_key
         | WHEN LOWER(a.platform) ='touch' THEN a.platform_key
         | WHEN LOWER(a.platform) ='snap' THEN 98
         | WHEN LOWER(a.platform) ='other' THEN 99
         | WHEN LOWER(a.platform) ='orderup-web' THEN 14
         | WHEN LOWER(a.platform) ='orderup-touch' THEN 14
         | WHEN LOWER(a.platform) ='orderup-ios' THEN 14
         | WHEN LOWER(a.platform) ='orderup-android' THEN 14
         | WHEN LOWER(a.platform) ='mobile-getaways' THEN a.platform_key
         | WHEN LOWER(a.platform) ='app' THEN a.platform_key
         | WHEN LOWER(a.platform) ='desktop' THEN a.platform_key
         | ELSE -1
         |  END
         |  AS platform_key_traffic
         |,CASE
         | WHEN a.platform ='Web' THEN platform_desc
         | WHEN a.platform ='Touch' THEN platform_desc
         | WHEN a.platform ='snap' THEN 'Other'
         | WHEN a.platform ='Other' THEN 'Other'
         | WHEN a.platform ='orderup-web' THEN 'OU'
         | WHEN a.platform ='orderup-touch' THEN 'OU'
         | WHEN a.platform ='orderup-ios' THEN 'OU'
         | WHEN a.platform ='orderup-android' THEN 'OU'
         | WHEN a.platform ='mobile-getaways' THEN platform_desc
         | WHEN a.platform ='App' THEN platform_desc
         | ELSE 'Other'
         | END
         | AS platform_key_traffic_desc
         |, a.platform_key AS platform_key_raw
         |, p.platform_desc AS platform_key_raw_desc
         |, CASE WHEN a.platform = 'mobile-getaways' THEN 'app'
         |  WHEN a.platform IN ('Web','App','Touch') THEN LOWER(a.platform)
         |  WHEN a.platform IN ('Desktop') Then 'Web'
         |  ELSE 'other'
         |  END AS platform
         |, CASE WHEN lower(a.platform) IN ('desktop','web') AND lower(a.sub_platform) IN ('desktop') THEN 'web'
         |  ELSE LOWER(a.sub_platform)
         |  END AS sub_platform
         |, a.traffic_source
         |, a.traffic_sub_source
         |, a.ref_attr_class_key
         |, a.deal_id
         |, CASE WHEN vert.grt_l1_cat_name IS NULL THEN 'unknown'
         |  ELSE vert.grt_l1_cat_name
         |  END AS grt_l1_cat_name
         |, CASE WHEN vert.grt_l2_cat_name IS NULL THEN 'unknown'
         |  ELSE vert.grt_l2_cat_name
         |  END AS grt_l2_cat_name
         |, a.brand
         |, a.uniq_deal_view_visitors
         |, a.uniq_buy_btn_clickers
         |, a.uniq_conf_page_visitors_gl
         |, a.uniq_cart_chkout_visitors
         |, a.uniq_conf_page_visitors
         |, a.uniq_cart_summarypage_visitors
         |, a.uniq_cart_conf_page_visitors
         |, a.uniq_comp_ordr_btn_clckrs_gl
         |, a.uniq_user_cart_comp_clickers
         |, a.uniq_usr_buy_butt_finl_clckrs
         |, a.uniq_receipt_page_visitors
         |, a.uniq_deal_views
         |, a.uniq_buy_btn_clicks
         |, a.uniq_cart_chkout_views
         |, a.uniq_conf_page_views
         |, a.uniq_cart_summary_page_views
         |, a.uniq_cart_conf_page_views
         |, a.uniq_cart_comp_clicks
         |, a.uniq_buy_butt_final_clicks
         |, a.uniq_receipt_page_views
         |, a.deal_views
         |, a.buy_btn_clicks
         |, a.conf_page_views
         |, a.cart_summary_page_views
         |, a.cart_conf_page_views
         |, a.comp_order_btn_clicks
         |, a.receipt_page_views
         |, a.new_visitor AS new_visitor_90
         |, a.uniq_available_deal_views
         |, a.available_deal_views
         |, a.page_views
         |, a.actual_page_views
         |, a.page_views_bounce
         |, cast(substr(current_timestamp,1,19)  as timestamp) dwh_created_at
         |, cast(substr(current_timestamp,1,19)  as timestamp) dwh_updated_at
         |, a.report_date
         |FROM
         |stg_gbl_traffic_deal_metrics a
         |LEFT JOIN dim_gbl_deal_lob vert
         |   ON vert.deal_id   = a.deal_id
         |JOIN dim_day b
         |   ON a.report_date = b.day_rw
         |JOIN dim_week c
         |   ON b.week_key=c.week_key
         |LEFT JOIN gbl_dim_platform p
         |   ON a.platform_key = p.platform_key
         |LEFT JOIN gbl_dim_country ctry ON a.country_id=ctry.country_id -- added this to join to economic_area and region tables
         |LEFT JOIN gbl_dim_economic_area eca ON ctry.economic_area_id=eca.economic_area_id -- Economic_are description
         |LEFT JOIN gbl_dim_region reg ON ctry.region_id=reg.region_id -- Region id Description
         |WHERE a.report_date BETWEEN '$startDate' and '$endDate'
      """.stripMargin


    val aggGblTrafficDealDf = sql(aggGblTrafficDealQry).distinct


    println ("Writing into Hdfs file")

    //Insert data into agg_gbl_traffic_deal partition table
    aggGblTrafficDealDf.coalesce(2).write.mode("overwrite").insertInto("$tgtSchema.agg_gbl_traffic_deal".replace("$tgtSchema",tgtSchema))


  }
}
