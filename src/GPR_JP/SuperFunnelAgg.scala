package com.groupon.edw.aggs

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object SparkConfig {
  val sparkConf: SparkConf = new SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryoserializer.buffer.max", "2047mb")
    .set("spark.sql.parquet.compression.codec","uncompressed")
    .set("spark.sql.inMemoryColumnarStorage.compressed","true")
    .set("spark.shuffle.compress","true")
    .set("spark.driver.maxResultSize","0")
    .set("spark.shuffle.memoryFraction","0.4")
    .set("spark.dynamicAllocation.maxExecutors","400")
    .set("spark.sql.files.ignoreCorruptFiles","true")
    .set("spark.rdd.compress","true")
    .set("spark.io.compression.codec","snappy")
    .set("spark.shuffle.service.enabled","true")

}

object ReadSuperfunnelConfig {

  val config = ConfigFactory.load()

  // Read event and eventDestinations

  val ed = config.getStringList("event.destination").asScala
  val juno_order_destinations  = config.getStringList("juno.order.destinations").asScala
  val juno_traffic_events = config.getStringList("juno.traffic.events").asScala
  val mob_cart_chkout_btn_clk = config.getStringList("mob.cart.chkout.btn.clk").asScala
  val other_buy_buttons = config.getStringList("other.buy.buttons").asScala
  val mob_buy_button_final_click = config.getStringList("mob.buy.button.final.click").asScala
  val mob_other_pvs = config.getStringList("mob.other.pvs").asScala
  val juno_order_events = config.getStringList("juno.order.events").asScala
  val goods_common_dkey_US = config.getString("goods.common.dkey.US")
  val goods_common_duuid_US = config.getString("goods.common.duuid.US")
  val goods_common_duuid_BE = config.getString("goods.common.duuid.BE")
  val goods_common_duuid_DE = config.getString("goods.common.duuid.DE")
  val goods_common_duuid_ES = config.getString("goods.common.duuid.ES")
  val goods_common_duuid_FR = config.getString("goods.common.duuid.FR")
  val goods_common_duuid_IE = config.getString("goods.common.duuid.IE")
  val goods_common_duuid_IT = config.getString("goods.common.duuid.IT")
  val goods_common_duuid_NL = config.getString("goods.common.duuid.NL")
  val goods_common_duuid_UK = config.getString("goods.common.duuid.UK")
  val gdoop_edw_dir = config.getString("gdoop.edw.dir")
  val goods_common_duuid_all =  Map("US"->goods_common_duuid_US,
    "UK"->goods_common_duuid_UK,
    "BE"->goods_common_duuid_BE,
    "DE"->goods_common_duuid_DE,
    "ES"->goods_common_duuid_ES,
    "FR"->goods_common_duuid_FR,
    "IE"->goods_common_duuid_IE,
    "IT"->goods_common_duuid_IT,
    "NL"->goods_common_duuid_NL)


  def lookupCommonUUID =  udf((country:String) => goods_common_duuid_all.get(country))
  // Read juno and new visitor base path

  val juno_base_path = config.getString("juno.base.path")
  val new_visitor_base_path = config.getString("new.visitor.base.path")
  val janus_sf_countries = config.getStringList("janus.sf.countries").asScala

  // Columns required for this agg

  val mobile_all = config.getStringList("mobile.all").asScala.toList
  val mobile_atts_cols    = config.getStringList("mobile.atts.cols").asScala.toList
  val mobile_tmp_cols     = config.getStringList("mobile.tmp.cols").asScala.toList
  val mobile_order_cols   = config.getStringList("mobile.order.cols").asScala.toList
  val juno_traffic_cols   = config.getStringList("juno.traffic.cols").asScala.toList
  val mobile_traffic_cols = config.getStringList("mobile.traffic.cols").asScala.toList


}

object SparkEnvironment {
  val sc = new SparkContext(SparkConfig.sparkConf)
  val hadoopFileSystem = FileSystem.get(sc.hadoopConfiguration)
  val spark = SparkSession
    .builder()
    .config(SparkConfig.sparkConf).enableHiveSupport().getOrCreate()
  print("************** Application ID: ")
  print(sc.applicationId)
  println(" **************")
  val args = sc.getConf.get("spark.driver.args").split("\\s+")
  val num_partitions = args(0).toInt
}

import com.groupon.edw.aggs.SparkEnvironment._
import com.groupon.edw.aggs.SparkEnvironment.spark.implicits._


object AppConfig {
  var dates: Array[String] = _
}

import com.groupon.edw.aggs.ReadSuperfunnelConfig._
import com.groupon.edw.aggs.SparkEnvironment.num_partitions
object SuperFunnelAgg {


  def BuildSuperFunnelAgg = {


    val juno_traf_hdfs_dir = new ListBuffer[String]()
    val juno_ord_hdfs_dir = new ListBuffer[String]()

    val dim_deal_df = spark.sql ("select uuid,deal_key,permalink from prod_groupondw.dim_deal")
    val dmp_df = spark.sql ("select merch_product_id,deal_uuid from edwprod.deal_merch_product")
    val dum_df = spark.sql ("SELECT unity.deal_uuid,cnt.country_iso_code_2 AS country_id ,TRIM(MIN(dd.deal_permalink))AS deal_permalink,TRIM(CAST(MIN(dd.ref_deal_id) AS VARCHAR(64))) deal_key,TRIM(CAST(MIN(dd.deal_id) AS VARCHAR(64))) deal_id FROM edwprod.deal_unity_map unity JOIN prod_groupondw.gbl_dim_deal_option dopt ON CAST(unity.deal_id AS VARCHAR(64)) = dopt.ref_deal_option_id AND unity.country_id = dopt.country_key JOIN prod_groupondw .gbl_dim_deal dd ON dopt.gbl_deal_key = dd.gbl_deal_key LEFT JOIN prod_groupondw.gbl_dim_country cnt on dd.country_key = cnt.country_id GROUP BY unity.deal_uuid  ,cnt.country_iso_code_2")
    val gbl_dim_country_df = spark.sql ("select country_iso_code_2,country_id from prod_groupondw.gbl_dim_country").cache()
    val ref_attr_class_df = spark.sql ("select ref_attr_class_key,traffic_source, traffic_sub_source from prod_groupondw.ref_attr_class").cache()


    // Windows for later use
    val window_date_bcookie_OeventTimeA = Window.partitionBy("eventDate","bcookie").orderBy("eventTime")
    val window_date_bcookie_OeventTimeD = Window.partitionBy("eventDate","bcookie").orderBy(desc("eventTime"))
    val first_order_window = Window.partitionBy("eventDate","bcookie","orderid").orderBy("eventTime")


    var juno_read_path:List[String] = Nil
    var juno_read_path_bld:List[String] = Nil
    val config_length = AppConfig.dates.length - 1
    for(i <- 0 to config_length) {
      juno_read_path_bld = List(juno_base_path + "/eventDate=" + AppConfig.dates(i)  + "/platform=mobile")
      println("juno_read_path_bld")
      juno_read_path_bld.foreach(println)
      juno_read_path = juno_read_path_bld ::: juno_read_path
      println("juno_read_path")
      juno_read_path.foreach(println)
    }

    var juno_ord_read_path:List[String] = Nil
    var juno_ord_read_path_bld:List[String] = Nil
    for(i <- 0 to config_length) {
      juno_ord_read_path_bld = List(juno_base_path + "/eventDate=" + AppConfig.dates(i)  + "/platform=oltp" + "/eventDestination=purchaseFunnel")

      juno_ord_read_path_bld.foreach(println)
      juno_ord_read_path = juno_ord_read_path_bld ::: juno_ord_read_path
      println("juno_ord_read_path")
      juno_ord_read_path.foreach(println)
    }

    val df_all = spark
      .read
      .option("basePath",juno_base_path)
      .parquet(juno_read_path:_*)

    val df_nv_all = spark.read.parquet(new_visitor_base_path)

    val df_all_ed = df_all.filter($"eventDate".isin(AppConfig.dates:_*) && $"eventDestination".isin(ed: _*))
    val df_ord_all = spark.read.option("basePath",juno_base_path).parquet(juno_ord_read_path:_*)

    val juno_traffic_init = df_all_ed.select(juno_traffic_cols.head, juno_traffic_cols: _*).distinct()
    println("juno_traffic_init")
    juno_traffic_init.printSchema()

    val df_ord_ed = df_ord_all.filter($"eventDate".isin(AppConfig.dates:_*))
    val oltp_ords = df_ord_ed.select(mobile_order_cols.head, mobile_order_cols.tail: _*)

    val new_visitors_0 = df_nv_all.filter($"ds".isin(AppConfig.dates:_*))
    val new_visitors = new_visitors_0.withColumn("eventDate", $"ds").withColumn("cookie_b",$"bcookie").withColumn("nv_ind",lit(1))

    // Filter out juno bots, unnecessary countries and events while modifying dealUUID & country
    //'mobile_common' will be used by the day-cookie-attributes steps and the traffic transformations
    val juno_filtered = juno_traffic_init.
      filter($"event".isin(juno_traffic_events:_*)
        && (length(trim($"bcookie")) > 0 && length(trim($"bcookie")) < 60)
        && ((upper(trim($"country"))).isin(janus_sf_countries:_*))
        && (lower(trim($"userAgent")).like("%bot%") === false)
        && (lower(trim($"userAgent")).like("%crawler%") === false)
        && (lower(trim($"userAgent")).like("%search%") === false)
        && (lower(trim($"userAgent")).like("%search%") === false)
        && (lower(trim($"userAgent")).like("%spider%") === false)
        && (lower(trim($"userAgent")).like("%spyder%") === false)
      )

    println("juno_filtered")

    val mobile_common = juno_filtered.
      withColumn(
        "dealUUID", when($"dealUUID".isNull || (length(trim($"dealUUID")) === 0), lit(-1)).
          when(
            ($"event".isin("checkoutView", "dealPurchase", "completeOrderButtonClick")
              && ($"cartStatus".isNotNull)
              && (length(trim($"cartStatus")) > 0)
              ) ||
              ($"event".isin("genericClick")
                && $"clickType".isin(mob_cart_chkout_btn_clk:_*))
            ,lit(-1)
          ).
          when( $"event".isin("buyButtonClick"
            ,"buyOptionClick"
            ,"checkoutView"
            ,"completeOrderButtonClick"
            ,"dealPurchase"
            ,"dealView"
          ) ||   ($"event".isin("genericClick")
            && $"clickType".isin(other_buy_buttons:_*))
            ,$"dealUUID")
          .otherwise(lit(-1))
      ).
      withColumn("country_code", upper(trim($"country")))


    //Select only relevant traffic columns for metric column population

    val mobile_traf_1 = mobile_common.select(mobile_traffic_cols.head,mobile_traffic_cols.tail:_*)
    println("mobile_traf_1")
    mobile_traf_1.printSchema()

    val mobile_traf_2 = mobile_traf_1.
      withColumn("deal_key",
        when(($"event".isin("checkoutView","dealPurchase","completeOrderButtonClick")
          && ($"cartStatus".isNotNull)
          && (length(trim($"cartStatus")) > 0) && ($"country_code").isin("US","CA")
          ) ||
          ($"event".isin("genericClick")
            && $"clickType".isin(mob_cart_chkout_btn_clk:_*) && ($"country_code").isin("US","CA"))
          ,goods_common_dkey_US).
          when(($"event".isin("checkoutView","dealPurchase","completeOrderButtonClick")
            && ($"cartStatus".isNotNull)
            && (length(trim($"cartStatus")) > 0) &&  ($"country_code").isin("UK","BE","DE","ES","FR","IE","IT","NL")
            ) ||
            ($"event".isin("genericClick")
              && $"clickType".isin(mob_cart_chkout_btn_clk:_*) && ($"country_code").isin("UK","BE","DE","ES","FR","IE","IT","NL"))
            ,lookupCommonUUID($"country_code")).
          when($"country_code".isin("US") === false, $"dealUUID").
          otherwise(lit(-1))).
      withColumn("deal_id",                   lit(-1)).
      withColumn("orderId",                   when($"event" =!= "dealPurchase", lit(null)).
        otherwise($"orderId")).
      withColumn("parentOrderUUID",           when($"event" =!= "dealPurchase", lit(null)).
        otherwise($"parentOrderUUID")).
      withColumn("f_dealview",               when($"event".isin("dealView")
        && ($"dealUUID".isin("-1") === false), 1).
        otherwise(0)
      ).
      withColumn("f_confirmation_pv",        when($"event".isin("checkoutView") && ($"cartStatus".isNull || (length(trim($"cartStatus")) === 0)),1).
        otherwise(0)
      ).
      withColumn("f_cart_chkout_pv",         when($"event".isin("checkoutView") && ($"cartStatus".isNotNull && (length(trim($"cartStatus")) > 0) ), 1).
        otherwise(0)
      ).
      withColumn("f_complete_ord_click",     when($"event".isin("completeOrderButtonClick")
        && ($"dealUUID".isin("-1") === false)
        && ($"cartStatus".isNull || (length(trim($"cartStatus")) === 0)), 1).
        otherwise(0)
      ).
      withColumn("f_cart_comp_ord_click",    when($"event".isin("completeOrderButtonClick")
        && ($"cartStatus".isNotNull
        && (length(trim($"cartStatus")) > 0)), 1).
        otherwise(0)
      ).
      withColumn("f_buy_button_click",       when(
        ($"event".isin("genericClick")
          && (
          (
            lower($"clientPlatform").like("and%")
              && ($"clickType" === "deal_option_click")
              && ($"eventDate" > "2015-09-14")
            ) || (
            (lower($"clientPlatform").like("iphone%"
            ) || lower($"clientPlatform").like("ipad%"))
              && ($"clickType" === "deal_option_click")
              && ($"eventDate" >= "2015-10-20")
            )
          )
          ) ||   $"event".isin("buyButtonClick"
        ) ||   ($"event".isin("buyOptionClick")
          && ($"rawevent".isin("GRP26") === false)),1).
        otherwise(0)
      ).
      withColumn("f_final_buy_button_click",  when($"event".isin("genericClick") && $"clickType".isin(mob_buy_button_final_click:_*),1).
        otherwise(0)
      ).
      withColumn("f_cart_chkout_click",      when($"event".isin("genericClick") && $"clickType".isin(mob_cart_chkout_btn_clk:_*),1).
        otherwise(0)
      ).
      withColumn("f_goods_gv",               when($"event".isin("genericPageView")
        && $"pageViewType".isin("goodsGallery"),1).otherwise(0)).
      withColumn("f_getaways_gv",            when($"event".isin("genericPageView")
        && $"pageViewType".isin("getawaysGallery"),1).otherwise(0)).
      withColumn("f_occasions_gv",           when($"event".isin("genericPageView")
        && $"pageViewType".isin("occasionsGallery"),1).otherwise(0)).
      withColumn("f_local_gv",               when($"event".isin("genericPageView")
        && $"pageViewType".isin("localGallery"),1).otherwise(0)).
      withColumn("f_search",                 when($"event".isin("search"),1).otherwise(0)).
      withColumn("f_nav",                    when($"event".isin("search")
        && lower($"channel").isin("nearby")
        && lower($"searchCategory").isNotNull
        && (lower($"searchCategory").isin("all") === false),1).otherwise(0)).
      withColumn("f_browse",                 when($"event".isin("browse"),1).otherwise(0)).
      withColumn("f_pull",                   when($"event".isin("search")
        && lower($"channel").isin("global_search"
      ) ||   (
        lower($"channel").isin("nearby")
          && lower($"searchCategory").isNotNull
          && lower($"searchCategory").isin("all") === false
        ),1).otherwise(0)).
      withColumn("f_receipt_pv",             lit(0)).
      withColumn("f_cart_receipt_pv",        lit(0)).
      withColumn("f_pageview",               when(
        (lower(trim($"pageId")).isin(mob_other_pvs:_*)) === false
          && ($"event".isin("genericPageView")), 1).otherwise(0)).
      withColumn("f_other_pageview",         when(
        (lower(trim($"pageId")).isin(mob_other_pvs:_*))
          && ($"event".isin("genericPageView")), 1).otherwise(0)).
      withColumn("f_download",               when($"event".isin("appDownload")
        && (trim(lower($"clientPlatform")).isin("touch") === false ), 1).
        otherwise(0)
      ).
      repartition(num_partitions, $"eventDate",$"bcookie").
      persist(StorageLevel.MEMORY_AND_DISK_SER)

    mobile_traf_1.unpersist()

    //  Dummy instant-buy confirmation pageviews are inserted into the funnel to ensure our checkout metrics are not
    //  affected by the instant-buy flow.

    val instant_buy_clk = mobile_traf_2.
      filter(($"event" === "dealPurchase")
        && ($"dealUUID".isNotNull
        && (length(trim($"dealUUID")) > 0)
        )
        && ($"orderStatusCode".isin("0","200"))
        && (lower($"extraInfo").like("%instant_buy%"
      ) || lower($"extraInfo").like("%apple_instant_pay%"
      ) || lower($"extraInfo").like("%apple_guest_pay%"
      )
        )).
      withColumn("orderId",                  lit(-1)).
      withColumn("parentOrderUUID",          lit(-1)).
      withColumn("f_confirmation_pv",        when($"cartStatus".isNull || (length(trim($"cartStatus")) === 0),1).otherwise(0)).
      withColumn("f_cart_chkout_pv",         when($"cartStatus".isNotNull && (length(trim($"cartStatus")) > 0),1).otherwise(0))

    /** ---------------------------------------------------------------------------------------
      * Order translations for receipt pageviews begin.
      * Order mappings are retrieved from junohourly's OLTP platform / orderCreation events.
      * ---------------------------------------------------------------------------------------
      * Identifies cart orders using the structure of the orderid (UUID format)
      * ord_num is created here because the cart parent order uuid is populated
      * for each child order within the cart order.  Only one is required
      * and will be used against a parent order to child order mapping to retrieve
      * the cart order's child orders.
      */

    val cart_ords = mobile_traf_2.
      filter(($"event" === "dealPurchase")
        && ($"orderStatusCode".isin("0","200"))
        && ($"extraInfo".like("%marketrate") === false)
        && ($"cartStatus".isNotNull)
        && (length(trim($"cartStatus")) > 0)
        && ($"orderId".rlike("^[a-zA-F0-9]{8}-([a-zA-F0-9]{4}-){3}[a-zA-F0-9]{12}$"))
      ).
      withColumn("parentOrderUUID",          $"orderId").
      withColumn("f_cart_receipt_pv",        lit(1)).
      withColumn("dealUUID",                 lit(-1)).
      withColumn("ord_num",                  row_number().over(first_order_window))

    // Non-cart orders not picked up in the "cart_ords" dataframe
    val noncart_ords = mobile_traf_2.
      filter(($"event" === "dealPurchase")
        && ($"orderStatusCode".isin("0","200"))
        && (
        (trim($"orderId").rlike("^[a-zA-F0-9]{8}-([a-zA-F0-9]{4}-){3}[a-zA-F0-9]{12}$") === false
          ) ||  ((trim($"orderId").rlike("^[a-zA-F0-9]{8}-([a-zA-F0-9]{4}-){3}[a-zA-F0-9]{12}$"))
          && (lower($"extraInfo").like("%marketrate%")))
          ||  ((trim($"orderId").rlike("^[a-zA-F0-9]{8}-([a-zA-F0-9]{4}-){3}[a-zA-F0-9]{12}$"))
          && ($"cartStatus".isNull || (length(trim($"cartStatus")) === 0)))
        )).
      withColumn("parentOrderUUID",          lit(-1)).
      withColumn("f_receipt_pv",             lit(1))

    cart_ords.printSchema()
    noncart_ords.printSchema()
    println(mobile_all)
    val all_ords = cart_ords.
      filter($"ord_num" === 1).select(mobile_all.head,mobile_all.tail:_*).
      union(noncart_ords.select(mobile_all.head,mobile_all.tail:_*))

    println("all_ords")
    all_ords.printSchema()

    // Redefining oltp_ords from OLTP here.  The merchandising product id was added to the orderCreation event as dealOptionId.
    val oltp_ords_fin = oltp_ords.
      filter($"event".isin(juno_order_events:_*)).
      withColumn("country_code", upper(trim($"country"))).
      withColumn("merchandising_product_id", $"dealOptionId")

    // Mapping of orders to parent orders.  Will be used for non-US countries.
    // Need to dedupe these - duplicates exist in the OLTP order creation events.
    val ord_to_pord_0 = oltp_ords_fin.filter($"country_code".isin(janus_sf_countries:_*)).
      select($"eventDate".alias("event_date_x"),
        $"bcookie".alias("bcookie_x"),
        $"country_code".alias("country_code_x"),
        $"orderId".alias("orderId_x"),
        $"parentOrderUUID".alias("parentOrderUUID_x"),
        $"merchandising_product_id".alias("merchandising_product_id_x"),
        lit(null).cast(StringType).alias("cart_deal_uuid_x")).
      distinct()

    // Retrieves the dealUUID for each order using deal_merchant_product.
    val ord_to_pord_1 = ord_to_pord_0.
      join(dmp_df,$"merchandising_product_id_x" === $"merch_product_id", "left_outer").
      withColumn("cart_deal_uuid_x", $"deal_uuid")

    // Selecting the columns from ord_to_pord_0 to remove the residual cols from the join to deal merch product
    val ord_to_pord = ord_to_pord_1.select((ord_to_pord_0.columns).head,(ord_to_pord_0.columns).tail:_*)

    // This is a copy of "ord_to_pord" with different column names
    val cart_map = ord_to_pord.
      select(  $"event_date_x".alias("event_date_y")
        ,$"bcookie_x".alias("bcookie_y")
        ,$"country_code_x".alias("country_code_y")
        ,$"orderId_x".alias("orderId_y")
        ,$"parentOrderUUID_x".alias("parentOrderUUID_y")
        ,$"cart_deal_uuid_x".alias("cart_deal_uuid_y")
      )

    // Retrieves the correct orderid and dealuuid for each order
    // deal_key should be "-1" for all cart orders
    val receipt_coalesce = all_ords.
      join(cart_map,    $"orderId"      === $"parentOrderUUID_y"
        &&  $"bcookie"      === $"bcookie_y"
        &&  $"eventDate"   === $"event_date_y"
        &&  $"orderId".rlike("^[a-zA-F0-9]{8}-([a-zA-F0-9]{4}-){3}[a-zA-F0-9]{12}$"),"left_outer").
      withColumn("deal_key_c", trim(coalesce($"dealUUID", lit(-1)))).
      withColumn("dealUUID_c", trim(coalesce($"cart_deal_uuid_y", $"dealUUID", lit(-1)))).
      withColumn("orderId_c", trim(coalesce( $"orderId_y", $"orderId")))

    // Drop unnecessary columns from the join ending in either x or y.
    //   These came from the mapping dataframes "ord_to_pord" and "cart_map"

    val receipt_dropcol = receipt_coalesce.select(receipt_coalesce.columns.filter(!_.endsWith("_x")).filter(!_.endsWith("_y")).head,
      receipt_coalesce.columns.filter(!_.endsWith("_x")).filter(!_.endsWith("_y")).tail:_*)

    val receipt_renamed = receipt_dropcol.
      withColumn("deal_key", $"deal_key_c").
      withColumn("dealUUID", $"dealUUID_c").
      withColumn("orderId",  $"orderId_c")

    // reorder the columns to union back with the other events
    val receipt_final = receipt_renamed.select(mobile_all.head,mobile_all.tail:_*)

    /**
      *  Event union - ready for US deal key mapping
      */

    val event_union = mobile_traf_2.
      select(mobile_all.head,mobile_all.tail:_*).
      filter($"event".isin("dealPurchase") === false).
      union(instant_buy_clk.select(mobile_all.head,mobile_all.tail:_*)).
      union(receipt_final.select(mobile_all.head,mobile_all.tail:_*)).
      repartition(num_partitions, $"eventDate",$"bcookie").
      persist(StorageLevel.MEMORY_AND_DISK_SER)

    cart_map.unpersist()
    ord_to_pord.unpersist()

    //-----------------------------------------------------
    // deal_uuid to unified_deal_key mapping begins for US
    //-----------------------------------------------------

    // Only records requiring an update
    val update_US_0 = event_union.filter(
      $"country_code".isin("US")
        && ($"deal_key".isin(goods_common_dkey_US) === false)
        && ($"dealUUID".isin("-1") === false)
    )

    // Left join to dim_deal
    val update_US_1 = update_US_0.
      join(dim_deal_df, update_US_0.col("dealUUID") === dim_deal_df.col("uuid"), "left_outer").
      withColumn("dealUUID_c", coalesce(dim_deal_df.col("uuid"),update_US_0.col("dealUUID"))).
      withColumn("deal_key_c", coalesce(dim_deal_df.col("deal_key"), update_US_0.col("deal_key"))).
      withColumn("deal_id_c", coalesce(dim_deal_df.col("deal_key"), update_US_0.col("deal_key")))

    // Drop & rename extra columns from the left join
    val update_US_2 = update_US_1.drop("deal_key","dealUUID","deal_id")

    val update_US_f = update_US_2.
      withColumn("deal_key", update_US_2.col("deal_key_c")).
      withColumn("dealUUID", update_US_2.col("dealUUID_c")).
      withColumn("deal_id",  update_US_2.col("deal_id_c"))

    update_US_f.printSchema()

    // Final union for aggregation

    val event_union_final = event_union.
      filter(
        (($"country_code".isin("US") === false)
          || $"deal_key".isin(goods_common_dkey_US)
          || $"dealUUID".isin("-1"))
      ).
      union(update_US_f.select(mobile_all.head,mobile_all.tail:_*)).
      repartition(num_partitions, $"eventDate",$"bcookie").
      sortWithinPartitions("eventDate","bcookie","eventTime").
      persist(StorageLevel.MEMORY_AND_DISK_SER)

    println("event_union_final")

    update_US_f.unpersist()

    // Day cookie attributes

    val mobile_atts_1 = mobile_common.
      join(broadcast(gbl_dim_country_df), $"country_code" === $"country_iso_code_2", "left_outer").
      withColumn("country_key",              coalesce($"country_id",lit(-1))).
      withColumn("fullUrl",                  when(($"fullUrl".isNull || $"fullUrl".isin("")), lit(null)).
        otherwise($"fullUrl")).
      withColumn("referralUrl",              when(($"referralUrl".isNull || $"referralUrl".isin("")), lit(null)).
        otherwise($"referralUrl")).
      withColumn("refAttrClassKey",          when(($"refAttrClassKey".isNull || $"refAttrClassKey".isin("")), lit(null)).
        otherwise($"refAttrClassKey")).
      withColumn("utm_medium",               when(($"medium".isNull || $"medium".isin("")), lit(null)).
        otherwise($"medium")).
      withColumn("utm_source",               when(($"source".isNull || $"source".isin("")), lit(null)).
        otherwise($"source")).
      withColumn("utm_campaign",             when(($"campaign".isNull || $"campaign".isin("")), lit(null)).
        otherwise($"campaign"))

    println("mobile_atts_1")
    mobile_atts_1.printSchema()


    val mobile_atts_2 = mobile_atts_1.
      withColumn("utm_campaign_brand",       when(size((split($"utm_campaign","_"))) >= "10"
        ,(split($"utm_campaign","_")(9))).
        otherwise(lit(null))).
      withColumn("utm_campaign_channel",     when(size((split($"utm_campaign","_"))) >= "3"
        ,(split($"utm_campaign","_")(2))).
        otherwise(lit(null))).
      withColumn("utm_campaign_inventory",   when(size((split($"utm_campaign","_"))) >= "7"
        ,(split($"utm_campaign","_")(6))).
        otherwise(lit(null))).
      withColumn("utm_campaign_strategy",    when(size((split($"utm_campaign","_"))) >= "6"
        ,(split($"utm_campaign","_")(5))).
        otherwise(lit(null)))

    println("mobile_atts_2")


    val mobile_atts_3 = mobile_atts_2.select(mobile_atts_cols.head,mobile_atts_cols.tail:_*)

    println("mobile_atts_3")


    val referrer_events_0 = mobile_atts_3.
      filter($"event".isin("externalReferrer")).
      select($"*",row_number().over(window_date_bcookie_OeventTimeA).alias("event_num"))

    println("referrer_events_0")


    // It is important to note here that using this join condition syntax allows both refAttrClassKey fields to be referenced in the
    //  selected data.  Supplying "refAttrClassKey" or ["refAttrClassKey"] for the join condition will not work.
    // See https://docs.databricks.com/spark/latest/faq/join-two-dataframes-duplicated-column.html

    val referrer_f = referrer_events_0.
      filter($"event_num" === "1").
      join(broadcast(ref_attr_class_df), referrer_events_0.col("refAttrClassKey") === ref_attr_class_df.col("ref_attr_class_key"),"left_outer").
      select(    referrer_events_0.col("eventDate")
        ,referrer_events_0.col("bcookie")
        ,referrer_events_0.col("fullUrl")
        ,referrer_events_0.col("referralUrl")
        ,referrer_events_0.col("utm_medium")
        ,referrer_events_0.col("utm_source").alias(ref_utm_source)
        ,referrer_events_0.col("utm_campaign")
        ,referrer_events_0.col("utm_campaign_channel")
        ,referrer_events_0.col("utm_campaign_brand")
        ,referrer_events_0.col("utm_campaign_inventory")
        ,referrer_events_0.col("utm_campaign_strategy")
        ,referrer_events_0.col("referrerDomain")
        ,coalesce(ref_attr_class_df.col("ref_attr_class_key"),referrer_events_0.col("refAttrClassKey")).alias("refAttrClassKey")
        ,ref_attr_class_df.col("traffic_source")
        ,ref_attr_class_df.col("traffic_sub_source")
      )

    println("referrer_f")


    // Create a row number per order so that the cookie"s first order may be found
    val cookie_order_0 = event_union_final.
      filter(event_union_final.col("event").isin("dealPurchase")
        && event_union_final.col("orderId").isNotNull
        && (length(event_union_final.col("orderId")) > 0)).
      select($"eventDate",$"bcookie",$"orderId",$"eventTime",
        row_number().over(window_date_bcookie_OeventTimeA).alias("ord_num"))

    // Collection of orders is found here along with the cookie"s first order
    val cookie_orders = cookie_order_0.
      groupBy("eventDate","bcookie").
      agg(collect_set(cookie_order_0.col("orderId")).alias("order_list")
        ,min(when(cookie_order_0.col("ord_num") === 1,cookie_order_0.col("orderId"))).alias("first_orderId"))

    val session_id_0 = mobile_atts_3.
      filter(mobile_atts_3("sessionId").isNotNull && length(trim(mobile_atts_3("sessionId"))) > 0).
      select($"eventDate",$"bcookie",$"sessionId",$"eventTime",
        row_number().over(window_date_bcookie_OeventTimeA).alias("session_id_row"))

    val session_id_info = session_id_0.
      groupBy("eventDate","bcookie").
      agg(
        countDistinct(session_id_0("sessionId")).cast(IntegerType).alias("session_count")
        ,max(when(session_id_0.col("session_id_row") === 1,session_id_0.col("sessionId"))).alias("first_session_id")
      )

    /* we have used below logic to calculate the session_count, but now since sessionId gets started getting populated in source, we just use
     sessionId to calculate the session_count and cookie_first_session_id
    val session_count = mobile_atts_3.
      filter(mobile_atts_3.col("event").isin("genericPageView")).
      withColumn("prev_eventTime",           lag(mobile_atts_3.col("eventTime"),1, 0).over(window_date_bcookie_OeventTimeA))
    val session_count_fin = session_count.
      groupBy("eventDate","bcookie").
      agg(
        (sum(
          (when(
            (session_count.col("eventTime").cast(LongType)
              - session_count.col("prev_eventTime").cast(LongType)).between(1800000,1000000000000l), 1)
            .otherwise(0))
        )
          )
          .cast(IntegerType)
          .alias("session_count")
      )
      */
    val userUUID_0 = mobile_atts_3.
      filter(mobile_atts_3.col("event").isin("genericPageView")
        && mobile_atts_3.col("consumerId").isNotNull
        && (length(mobile_atts_3.col("consumerId")) > 0)).
      select($"eventDate",$"bcookie",$"consumerId",
        row_number().over(window_date_bcookie_OeventTimeD).alias("desc_userUUID_row"))

    val userUUID = userUUID_0.filter(userUUID_0.col("desc_userUUID_row") === 1)

    // First and last page types
    val ptypes_0 = mobile_atts_3.
      filter(mobile_atts_3.col("event").isin("genericPageView")
        && mobile_atts_3.col("pageId").isNotNull
        && (length(mobile_atts_3.col("pageId")) > 0)).
      select($"eventDate",$"bcookie",$"pageId",$"eventTime",
        row_number().over(window_date_bcookie_OeventTimeA).alias("asc_pageId_row"),
        row_number().over(window_date_bcookie_OeventTimeD).alias("desc_pageId_row"))

    val first_last_ptypes = ptypes_0.
      filter((ptypes_0.col("asc_pageId_row") === 1) || (ptypes_0.col("desc_pageId_row") === 1)).
      groupBy("eventDate","bcookie").
      agg(max(when(ptypes_0.col("asc_pageId_row") === 1, ptypes_0.col("pageId"))).alias("first_pageId"),
        max(when(ptypes_0.col("desc_pageId_row") === 1, ptypes_0.col("pageId"))).alias("last_pageId"))

    // First and last divisions must be populated with the first and last POPULATED divisions.
    // It does not matter what event these are populated on.
    val divisions_0 = mobile_atts_3.
      filter(mobile_atts_3.col("division").isNotNull && (length(trim(mobile_atts_3.col("division"))) > 0)).
      select($"eventDate",$"bcookie",$"division",$"eventTime",
        row_number().over(window_date_bcookie_OeventTimeA).alias("asc_div_row"),
        row_number().over(window_date_bcookie_OeventTimeD).alias("desc_div_row"))

    val first_last_divisions = divisions_0.
      filter((divisions_0.col("asc_div_row") === 1) || (divisions_0.col("desc_div_row") === 1)).
      groupBy("eventDate","bcookie").
      agg(max(when(divisions_0.col("asc_div_row") === 1, divisions_0.col("division"))).alias("first_division"),
        max(when(divisions_0.col("desc_div_row") === 1, divisions_0.col("division"))).alias("last_division"))

    val misc_atts_0 = mobile_atts_3.select($"*",row_number().over(window_date_bcookie_OeventTimeA).alias("asc_row"))
    val misc_atts = misc_atts_0.filter(misc_atts_0.col("asc_row") === 1)

    val brand_0 = mobile_atts_3.
      filter(mobile_atts_3.col("event").isin("genericPageView")).
      select($"eventDate",$"bcookie",$"brand",row_number().over(window_date_bcookie_OeventTimeA).alias("asc_row"))

    val brand = brand_0.filter(brand_0.col("asc_row") === 1)

    val bcookie_atts = misc_atts.
      join(referrer_f,                                       Seq("eventDate","bcookie"),"left_outer").
      join(cookie_orders,                                    Seq("eventDate","bcookie"),"left_outer").
      join(session_id_info,                                  Seq("eventDate","bcookie"),"left_outer").
      join(first_last_ptypes,                                Seq("eventDate","bcookie"),"left_outer").
      join(userUUID,                                         Seq("eventDate","bcookie"),"left_outer").
      join(brand,                                            Seq("eventDate","bcookie"),"left_outer").
      join(first_last_divisions,                             Seq("eventDate","bcookie"),"left_outer").
      join(new_visitors,                                     Seq("eventDate","bcookie"),"left_outer").
      select(
        misc_atts.col("eventDate")
        ,misc_atts.col("bcookie")
        ,referrer_f.col("referralUrl").alias("cookie_first_referrer_url")
        ,referrer_f.col("fullUrl").alias("cookie_first_full_url")
        ,referrer_f.col("utm_medium").alias("cookie_first_utm_medium")
        ,referrer_f.col("ref_utm_source").alias("cookie_first_utm_source")
        ,referrer_f.col("utm_campaign").alias("cookie_first_utm_campaign")
        ,referrer_f.col("utm_campaign_channel").alias("cookie_first_utm_camp_channel")
        ,referrer_f.col("utm_campaign_brand").alias("cookie_first_utm_camp_brand")
        ,referrer_f.col("utm_campaign_inventory").alias("cookie_first_utm_camp_inv")
        ,referrer_f.col("utm_campaign_strategy").alias("cookie_first_utm_camp_strategy")
        ,referrer_f.col("referrerDomain").alias("cookie_first_referrer_domain")
        ,coalesce(referrer_f.col("refAttrClassKey").cast(IntegerType), lit(27)).alias("cookie_ref_attr_class_key")
        ,coalesce(referrer_f.col("traffic_source"), lit("Direct")).alias("cookie_first_traf_source")
        ,coalesce(referrer_f.col("traffic_sub_source"), lit("Direct")).alias("cookie_first_traf_sub_source")
        ,when(lower(trim(misc_atts.col("clientPlatform"))).isin("touch"),"Touch").otherwise("App").alias("cookie_first_platform")
        ,misc_atts.col("clientPlatform").alias("cookie_first_sub_platform")
        ,misc_atts.col("mobileDevice").alias("cookie_first_device")
        ,misc_atts.col("appVersion").alias("cookie_first_app_version")
        ,lit("NA").alias("cookie_first_os")
        ,misc_atts.col("userOS").alias("cookie_first_os_version")
        ,misc_atts.col("mobileProvider").alias("cookie_first_carrier")
        ,userUUID.col("consumerId").alias("cookie_last_user_uuid")
        ,misc_atts.col("country_code").alias("cookie_first_country_code")
        ,coalesce(misc_atts.col("country_key").cast(IntegerType), lit(-1)).alias("cookie_first_country_key")
        ,misc_atts.col("browser").alias("cookie_first_browser")
        ,first_last_ptypes.col("first_pageId").alias("cookie_first_page_type")
        ,first_last_ptypes.col("last_pageId").alias("cookie_last_page_type")
        ,coalesce(session_id_info("first_session_id"),lit(null)).alias("cookie_first_session_id")
        ,coalesce(session_id_info("session_count"),lit(1)).alias("session_count")
        ,first_last_divisions.col("first_division").alias("cookie_first_division")
        ,first_last_divisions.col("last_division").alias("cookie_last_division")
        ,cookie_orders.col("first_orderId").alias("cookie_first_order_id")
        ,cookie_orders.col("order_list").cast(ArrayType(StringType)).alias("cookie_orderId_list")
        ,when(brand.col("brand").isNotNull && (length(trim(brand.col("brand"))) > 0), brand.col("brand")).otherwise(lit("groupon").cast(StringType)).alias("cookie_first_brand")
        ,when(new_visitors.col("nv_ind").isNotNull, misc_atts.col("bcookie")).otherwise(lit(null).cast(StringType)).alias("ind_new_visitor")
      ).
      repartition(num_partitions, $"eventDate",$"bcookie").
      persist(StorageLevel.MEMORY_AND_DISK_SER)

    println("bcookie_atts")


    mobile_atts_3.unpersist()
    misc_atts.unpersist()
    cookie_orders.unpersist()

    //######################################################################################################################################################
    //# Aggregate from the event_union_final dataframe
    //######################################################################################################################################################

    val day_cookie_deal_platform_0 = event_union_final.
      groupBy(
        event_union_final("eventDate")
        ,event_union_final("bcookie")
        ,when(lower(trim(event_union_final("clientPlatform"))).isin("touch"),"Touch").otherwise("App").alias("deal_platform")
        ,event_union_final("clientPlatform").alias("deal_sub_platform")
        ,event_union_final("deal_key")
        ,event_union_final("dealUUID")
        ,event_union_final("deal_id")
      ).
      agg(
        sum(event_union_final("f_pageview")
          + event_union_final("f_dealview")
          + event_union_final("f_receipt_pv")
          + event_union_final("f_confirmation_pv")
          + event_union_final("f_nav")
          + event_union_final("f_search")
          + event_union_final("f_browse")
          + lit(0).cast(LongType) // cart summary page views
          + event_union_final("f_cart_chkout_pv")
          + event_union_final("f_cart_receipt_pv")
          + when(
          event_union_final("event").isin("search")
            && (
            (event_union_final("f_search")         === 0)
              && (event_union_final("f_nav")          === 0)
              && (event_union_final("f_browse")       === 0)
              && (event_union_final("f_pull")         === 0)
              && (event_union_final("f_goods_gv")     === 0)
              && (event_union_final("f_getaways_gv")  === 0)
              && (event_union_final("f_occasions_gv") === 0)
              && (event_union_final("f_local_gv")     === 0)
            ), 1).otherwise(0).cast(LongType) // other search pageviews
        ).cast(LongType).alias("page_views")
        ,sum(event_union_final("f_dealview")).cast(LongType).alias("available_deal_views")
        ,sum(lit(0)).cast(LongType).alias("unavailable_deal_views")
        ,sum(lit(0)).cast(LongType).alias("untagged_deal_views")
        ,sum(event_union_final("f_buy_button_click")).cast(LongType).alias("buy_button_clicks")
        ,sum(event_union_final("f_confirmation_pv")).cast(LongType).alias("confirm_page_views")
        ,sum(event_union_final("f_complete_ord_click")).cast(LongType).alias("comp_order_button_clicks")
        ,sum(event_union_final("f_receipt_pv")).cast(LongType).alias("receipt_page_views")
        ,sum(event_union_final("f_goods_gv")).cast(LongType).alias("goods_gallery_page_views")
        ,sum(event_union_final("f_getaways_gv")).cast(LongType).alias("getaways_gallery_page_views")
        ,sum(event_union_final("f_occasions_gv")).cast(LongType).alias("occasions_gallery_page_views")
        ,sum(event_union_final("f_local_gv")).cast(LongType).alias("local_gallery_page_views")
        ,sum(event_union_final("f_nav")).cast(LongType).alias("navigation_page_views")
        ,sum(event_union_final("f_search")).cast(LongType).alias("search_page_views")
        ,sum(event_union_final("f_browse")).cast(LongType).alias("browse_page_views")
        ,sum(lit(0)).cast(LongType).alias("other_clicks")
        ,sum(event_union_final("f_other_pageview")).cast(LongType).alias("other_page_views")
        ,sum(when(
          event_union_final("event").isin("search")
            && (
            (event_union_final("f_search") === 0)
              && (event_union_final("f_nav") === 0)
              && (event_union_final("f_browse") === 0)
              && (event_union_final("f_pull") === 0)
              && (event_union_final("f_goods_gv") === 0)
              && (event_union_final("f_getaways_gv") === 0)
              && (event_union_final("f_occasions_gv") === 0)
              && (event_union_final("f_local_gv") === 0)
            ), 1).otherwise(0)
        ).cast(LongType).alias("other_search_page_views")
        ,sum(lit(0)).cast(LongType).alias("cart_summary_page_views")
        // Cart checkout views are duplicated per item within the cart.  The distinct eventTime is
        //   counted to bypass this.  The deal information for these cart events is "-1".
        ,coalesce(countDistinct(when((event_union_final("f_cart_chkout_pv") === 1),event_union_final("eventTime"))),lit(0)).cast(LongType).alias("cart_checkout_views")
        ,sum("f_cart_receipt_pv").cast(LongType).alias("cart_receipt_views")
        ,sum("f_cart_chkout_click").cast(LongType).alias("cart_chkout_btn_clicks")
        ,sum("f_cart_comp_ord_click").cast(LongType).alias("cart_comp_order_btn_clicks")
        ,sum(lit(0)).cast(LongType).alias("sigin_redirect_page_views")
        ,sum("f_final_buy_button_click").cast(LongType).alias("final_buy_button_clicks")
        ,collect_set("orderId").cast(ArrayType(StringType)).alias("cookie_deal_orderIds")
        ,max("f_download").alias("ind_downloader")
      ).
      repartition(num_partitions, $"eventDate",$"bcookie").
      persist(StorageLevel.MEMORY_AND_DISK_SER)

    println("day_cookie_deal_platform_0")


    val day_cookie_deal_platform_1 = day_cookie_deal_platform_0.
      select(
        day_cookie_deal_platform_0("eventDate"),
        day_cookie_deal_platform_0("bcookie"),
        day_cookie_deal_platform_0("deal_platform"),
        day_cookie_deal_platform_0("deal_sub_platform"),
        day_cookie_deal_platform_0("deal_key"),
        day_cookie_deal_platform_0("dealUUID"),
        day_cookie_deal_platform_0("deal_id"),
        day_cookie_deal_platform_0("page_views"),
        day_cookie_deal_platform_0("available_deal_views"),
        day_cookie_deal_platform_0("unavailable_deal_views"),
        day_cookie_deal_platform_0("untagged_deal_views"),
        day_cookie_deal_platform_0("buy_button_clicks"),
        day_cookie_deal_platform_0("confirm_page_views"),
        day_cookie_deal_platform_0("comp_order_button_clicks"),
        day_cookie_deal_platform_0("receipt_page_views"),
        day_cookie_deal_platform_0("goods_gallery_page_views"),
        day_cookie_deal_platform_0("getaways_gallery_page_views"),
        day_cookie_deal_platform_0("occasions_gallery_page_views"),
        day_cookie_deal_platform_0("local_gallery_page_views"),
        day_cookie_deal_platform_0("navigation_page_views"),
        day_cookie_deal_platform_0("search_page_views"),
        day_cookie_deal_platform_0("browse_page_views"),
        day_cookie_deal_platform_0("other_clicks"),
        day_cookie_deal_platform_0("other_page_views"),
        day_cookie_deal_platform_0("other_search_page_views"),
        day_cookie_deal_platform_0("cart_summary_page_views"),
        day_cookie_deal_platform_0("cart_checkout_views"),
        day_cookie_deal_platform_0("cart_receipt_views"),
        day_cookie_deal_platform_0("cart_chkout_btn_clicks"),
        day_cookie_deal_platform_0("cart_comp_order_btn_clicks"),
        day_cookie_deal_platform_0("sigin_redirect_page_views"),
        day_cookie_deal_platform_0("final_buy_button_clicks"),
        day_cookie_deal_platform_0("cookie_deal_orderIds"),
        day_cookie_deal_platform_0("ind_downloader")
        ,when(day_cookie_deal_platform_0("page_views") > 0, day_cookie_deal_platform_0("bcookie")).alias("unique_visitors")
        ,when(day_cookie_deal_platform_0("available_deal_views") > 0, day_cookie_deal_platform_0("bcookie")).alias("unique_dv_visitors")
        ,day_cookie_deal_platform_0("available_deal_views").alias("deal_views")
        ,when(day_cookie_deal_platform_0("available_deal_views") > 0, 1).otherwise(0).cast(LongType).alias("unique_deal_views")
        ,when(day_cookie_deal_platform_0("available_deal_views") > 0, 1).otherwise(0).cast(LongType).alias("unique_available_dvs")
        ,when(day_cookie_deal_platform_0("buy_button_clicks") > 0, 1).otherwise(0).cast(LongType).alias("unique_buy_button_clks")
        ,when(day_cookie_deal_platform_0("buy_button_clicks") > 0, day_cookie_deal_platform_0("bcookie")).alias("unique_usr_buy_button_clks")
        ,when(day_cookie_deal_platform_0("receipt_page_views") > 0, 1).otherwise(0).cast(LongType).alias("unique_receipt_page_views")
        ,when(day_cookie_deal_platform_0("receipt_page_views") > 0, day_cookie_deal_platform_0("bcookie")).alias("unique_receipt_page_viewers")
        ,when(day_cookie_deal_platform_0("confirm_page_views") > 0, 1).otherwise(0).cast(LongType).alias("unique_conf_page_views")
        ,when(day_cookie_deal_platform_0("confirm_page_views") > 0, day_cookie_deal_platform_0("bcookie")).alias("unique_conf_page_viewers")
        ,when(day_cookie_deal_platform_0("cart_summary_page_views") > 0, 1).otherwise(0).cast(LongType).alias("unique_cart_summary_views")
        ,when(day_cookie_deal_platform_0("cart_summary_page_views") > 0, day_cookie_deal_platform_0("bcookie")).alias("unique_cart_summary_viewers")
        ,when(day_cookie_deal_platform_0("cart_checkout_views") > 0, 1).otherwise(0).cast(LongType).alias("unique_cart_chkout_views")
        ,when(day_cookie_deal_platform_0("cart_checkout_views") > 0, day_cookie_deal_platform_0("bcookie")).alias("unique_cart_chkout_viewers")
        ,when(day_cookie_deal_platform_0("cart_receipt_views") > 0, 1).otherwise(0).cast(LongType).alias("unique_cart_receipt_views")
        ,when(day_cookie_deal_platform_0("cart_receipt_views") > 0, day_cookie_deal_platform_0("bcookie")).alias("unique_cart_receipt_viewers")
        ,when(day_cookie_deal_platform_0("cart_chkout_btn_clicks") > 0, 1).otherwise(0).cast(LongType).alias("unique_cart_chkout_clks")
        ,when(day_cookie_deal_platform_0("cart_chkout_btn_clicks") > 0, day_cookie_deal_platform_0("bcookie")).alias("unique_usr_cart_chkout_clks")
        ,when(day_cookie_deal_platform_0("cart_comp_order_btn_clicks") > 0, 1).otherwise(0).cast(LongType).alias("unique_cart_comp_clks")
        ,when(day_cookie_deal_platform_0("cart_comp_order_btn_clicks") > 0, day_cookie_deal_platform_0("bcookie")).alias("unique_usr_cart_comp_clks")
        ,when(day_cookie_deal_platform_0("sigin_redirect_page_views") > 0, 1).otherwise(0).cast(LongType).alias("unique_signin_redir_pviews")
        ,when(day_cookie_deal_platform_0("sigin_redirect_page_views") > 0, day_cookie_deal_platform_0("bcookie")).alias("unique_signin_redir_pviewers")
        ,when(day_cookie_deal_platform_0("final_buy_button_clicks") > 0, 1).otherwise(0).cast(LongType).alias("unique_buy_but_final_clks")
        ,when(day_cookie_deal_platform_0("final_buy_button_clicks") > 0, day_cookie_deal_platform_0("bcookie")).alias("uniq_usr_buy_but_final_clks")
        ,when(day_cookie_deal_platform_0("goods_gallery_page_views") > 0, day_cookie_deal_platform_0("bcookie")).alias("unique_goods_gallery_viewers")
        ,when(day_cookie_deal_platform_0("getaways_gallery_page_views") > 0, day_cookie_deal_platform_0("bcookie")).alias("unique_ga_gallery_viewers")
        ,when(day_cookie_deal_platform_0("occasions_gallery_page_views") > 0, day_cookie_deal_platform_0("bcookie")).alias("unique_occ_gallery_viewers")
        ,when(day_cookie_deal_platform_0("local_gallery_page_views") > 0, day_cookie_deal_platform_0("bcookie")).alias("unique_loc_gallery_viewers")
        ,when(day_cookie_deal_platform_0("navigation_page_views") > 0, day_cookie_deal_platform_0("bcookie")).alias("unique_navi_page_viewers")
        ,when(day_cookie_deal_platform_0("search_page_views") > 0, day_cookie_deal_platform_0("bcookie")).alias("unique_search_page_viewers")
        ,when(day_cookie_deal_platform_0("browse_page_views") > 0, day_cookie_deal_platform_0("bcookie")).alias("unique_browse_page_viewers")
        ,when((day_cookie_deal_platform_0("navigation_page_views")
          + day_cookie_deal_platform_0("search_page_views")
          + day_cookie_deal_platform_0("browse_page_views")) > 0,day_cookie_deal_platform_0("bcookie")).alias("unique_pull_viewers")
        ,when(day_cookie_deal_platform_0("page_views") === 1,day_cookie_deal_platform_0("bcookie")).alias("bounce_visitors"))

    println("day_cookie_deal_platform_1")

    val day_cookie_0 = day_cookie_deal_platform_1.
      groupBy(
        day_cookie_deal_platform_0("eventDate")
        ,day_cookie_deal_platform_0("bcookie")
      ).
      agg(
        sum("page_views").alias("page_views")
        ,sum("available_deal_views").alias("available_deal_views")
        ,sum("unavailable_deal_views").alias("unavailable_deal_views")
        ,sum("untagged_deal_views").alias("untagged_deal_views")
        ,sum("buy_button_clicks").alias("buy_button_clicks")
        ,sum("confirm_page_views").alias("confirm_page_views")
        ,sum("comp_order_button_clicks").alias("comp_order_button_clicks")
        ,sum("receipt_page_views").alias("receipt_page_views")
        ,sum("goods_gallery_page_views").alias("goods_gallery_page_views")
        ,sum("getaways_gallery_page_views").alias("getaways_gallery_page_views")
        ,sum("occasions_gallery_page_views").alias("occasions_gallery_page_views")
        ,sum("local_gallery_page_views").alias("local_gallery_page_views")
        ,sum("navigation_page_views").alias("navigation_page_views")
        ,sum("search_page_views").alias("search_page_views")
        ,sum("browse_page_views").alias("browse_page_views")
        ,sum("other_clicks").alias("other_clicks")
        ,sum("other_page_views").alias("other_page_views")
        ,sum("other_search_page_views").alias("other_search_page_views")
        ,sum("cart_summary_page_views").alias("cart_summary_page_views")
        ,sum("cart_checkout_views").alias("cart_checkout_views")
        ,sum("cart_receipt_views").alias("cart_receipt_views")
        ,sum("cart_chkout_btn_clicks").alias("cart_chkout_btn_clicks")
        ,sum("cart_comp_order_btn_clicks").alias("cart_comp_order_btn_clicks")
        ,sum("sigin_redirect_page_views").alias("sigin_redirect_page_views")
        ,sum("final_buy_button_clicks").alias("final_buy_button_clicks")
        ,countDistinct("deal_key").alias("deal_count")
        ,max("ind_downloader").alias("ind_downloader")
        ,max("unique_visitors").alias("unique_visitors")
        ,max("unique_dv_visitors").alias("unique_dv_visitors")
        ,sum("deal_views").alias("deal_views")
        ,sum("unique_deal_views").alias("unique_deal_views")
        ,sum("unique_available_dvs").alias("unique_available_dvs")
        ,sum("unique_buy_button_clks").alias("unique_buy_button_clks")
        ,max("unique_usr_buy_button_clks").alias("unique_usr_buy_button_clks")
        ,sum("unique_receipt_page_views").alias("unique_receipt_page_views")
        ,max("unique_receipt_page_viewers").alias("unique_receipt_page_viewers")
        ,sum("unique_conf_page_views").alias("unique_conf_page_views")
        ,max("unique_conf_page_viewers").alias("unique_conf_page_viewers")
        ,sum("unique_cart_summary_views").alias("unique_cart_summary_views")
        ,max("unique_cart_summary_viewers").alias("unique_cart_summary_viewers")
        ,sum("unique_cart_chkout_views").alias("unique_cart_chkout_views")
        ,max("unique_cart_chkout_viewers").alias("unique_cart_chkout_viewers")
        ,sum("unique_cart_receipt_views").alias("unique_cart_receipt_views")
        ,max("unique_cart_receipt_viewers").alias("unique_cart_receipt_viewers")
        ,sum("unique_cart_chkout_clks").alias("unique_cart_chkout_clks")
        ,max("unique_usr_cart_chkout_clks").alias("unique_usr_cart_chkout_clks")
        ,sum("unique_cart_comp_clks").alias("unique_cart_comp_clks")
        ,max("unique_usr_cart_comp_clks").alias("unique_usr_cart_comp_clks")
        ,sum("unique_signin_redir_pviews").alias("unique_signin_redir_pviews")
        ,max("unique_signin_redir_pviewers").alias("unique_signin_redir_pviewers")
        ,sum("unique_buy_but_final_clks").alias("unique_buy_but_final_clks")
        ,max("uniq_usr_buy_but_final_clks").alias("uniq_usr_buy_but_final_clks")
        ,max("unique_goods_gallery_viewers").alias("unique_goods_gallery_viewers")
        ,max("unique_ga_gallery_viewers").alias("unique_ga_gallery_viewers")
        ,max("unique_occ_gallery_viewers").alias("unique_occ_gallery_viewers")
        ,max("unique_loc_gallery_viewers").alias("unique_loc_gallery_viewers")
        ,max("unique_navi_page_viewers").alias("unique_navi_page_viewers")
        ,max("unique_search_page_viewers").alias("unique_search_page_viewers")
        ,max("unique_browse_page_viewers").alias("unique_browse_page_viewers")
        ,max("unique_pull_viewers").alias("unique_pull_viewers")
        ,max("bounce_visitors").alias("bounce_visitors")
      ).
      repartition(num_partitions, $"eventDate",$"bcookie")

    val day_cookie_deal_plat = day_cookie_deal_platform_1.
      join(bcookie_atts, Seq("eventDate","bcookie"),"left_outer").
      select(
        current_timestamp().alias("edw_modified_ts")
        ,regexp_replace(current_date().cast(StringType), "-", "").alias("load_key")
        ,day_cookie_deal_platform_1("bcookie").alias("cookie_b")
        ,coalesce(day_cookie_deal_platform_1("deal_key"),lit(-1)).alias("unified_deal_key")
        ,coalesce(when(day_cookie_deal_platform_1("deal_key").isin(goods_common_dkey_US) ,goods_common_duuid_US).
          when(day_cookie_deal_platform_1("deal_key").isin(goods_common_duuid_UK),goods_common_duuid_UK).
          when(day_cookie_deal_platform_1("deal_key").isin(goods_common_duuid_BE),goods_common_duuid_BE).
          when(day_cookie_deal_platform_1("deal_key").isin(goods_common_duuid_DE),goods_common_duuid_DE).
          when(day_cookie_deal_platform_1("deal_key").isin(goods_common_duuid_ES),goods_common_duuid_ES).
          when(day_cookie_deal_platform_1("deal_key").isin(goods_common_duuid_FR),goods_common_duuid_FR).
          when(day_cookie_deal_platform_1("deal_key").isin(goods_common_duuid_IE),goods_common_duuid_IE).
          when(day_cookie_deal_platform_1("deal_key").isin(goods_common_duuid_IT),goods_common_duuid_IT).
          when(day_cookie_deal_platform_1("deal_key").isin(goods_common_duuid_NL),goods_common_duuid_NL)
          .otherwise(day_cookie_deal_platform_1("dealUUID")),lit(-1)).alias("deal_uuid")
        ,coalesce(day_cookie_deal_platform_1("deal_id"),lit(-1)).alias("dealId")
        ,day_cookie_deal_platform_1("deal_platform")
        ,day_cookie_deal_platform_1("deal_sub_platform")
        ,bcookie_atts("cookie_first_country_code")
        ,bcookie_atts("cookie_first_country_key")
        ,bcookie_atts("cookie_first_platform")
        ,bcookie_atts("cookie_first_sub_platform")
        ,bcookie_atts("cookie_first_brand")
        ,bcookie_atts("cookie_first_division")
        ,bcookie_atts("cookie_last_division")
        ,bcookie_atts("cookie_first_page_type")
        ,bcookie_atts("cookie_last_page_type")
        ,bcookie_atts("cookie_ref_attr_class_key")
        ,bcookie_atts("cookie_first_traf_source")
        ,bcookie_atts("cookie_first_traf_sub_source")
        ,bcookie_atts("cookie_first_utm_medium")
        ,bcookie_atts("cookie_first_utm_camp_channel")
        ,bcookie_atts("cookie_first_utm_camp_brand")
        ,bcookie_atts("cookie_first_utm_camp_inv")
        ,bcookie_atts("cookie_first_utm_camp_strategy")
        ,bcookie_atts("cookie_first_referrer_domain")
        ,bcookie_atts("cookie_first_utm_source")
        ,bcookie_atts("cookie_first_utm_campaign")
        ,bcookie_atts("cookie_first_referrer_url")
        ,bcookie_atts("cookie_first_full_url")
        ,bcookie_atts("cookie_first_browser")
        ,bcookie_atts("cookie_first_os")
        ,bcookie_atts("cookie_first_os_version")
        ,bcookie_atts("cookie_first_carrier")
        ,bcookie_atts("cookie_first_device")
        ,bcookie_atts("cookie_first_app_version")
        ,bcookie_atts("cookie_last_user_uuid")
        ,bcookie_atts("cookie_first_order_id")
        ,bcookie_atts("cookie_orderId_list")
        ,day_cookie_deal_platform_1("page_views")
        ,day_cookie_deal_platform_1("available_deal_views")
        ,day_cookie_deal_platform_1("unavailable_deal_views")
        ,day_cookie_deal_platform_1("buy_button_clicks")
        ,day_cookie_deal_platform_1("confirm_page_views")
        ,day_cookie_deal_platform_1("comp_order_button_clicks")
        ,day_cookie_deal_platform_1("receipt_page_views")
        ,day_cookie_deal_platform_1("goods_gallery_page_views")
        ,day_cookie_deal_platform_1("getaways_gallery_page_views")
        ,day_cookie_deal_platform_1("occasions_gallery_page_views")
        ,day_cookie_deal_platform_1("local_gallery_page_views")
        ,day_cookie_deal_platform_1("navigation_page_views")
        ,day_cookie_deal_platform_1("search_page_views")
        ,day_cookie_deal_platform_1("browse_page_views")
        ,day_cookie_deal_platform_1("other_clicks")
        ,day_cookie_deal_platform_1("other_page_views")
        ,day_cookie_deal_platform_1("other_search_page_views")
        ,day_cookie_deal_platform_1("cookie_deal_orderIds")
        ,lit(1).cast(IntegerType).alias("count_constant")
        ,day_cookie_deal_platform_1("cart_summary_page_views")
        ,day_cookie_deal_platform_1("cart_checkout_views")
        ,day_cookie_deal_platform_1("cart_receipt_views")
        ,day_cookie_deal_platform_1("cart_chkout_btn_clicks")
        ,day_cookie_deal_platform_1("cart_comp_order_btn_clicks")
        ,day_cookie_deal_platform_1("sigin_redirect_page_views")
        ,day_cookie_deal_platform_1("final_buy_button_clicks")
        ,bcookie_atts("ind_new_visitor")
        ,when(day_cookie_deal_platform_0("ind_downloader") > 0, day_cookie_deal_platform_0("bcookie")).alias("ind_downloader")
        ,(day_cookie_deal_platform_1("page_views")
          - ( day_cookie_deal_platform_1("navigation_page_views")
          +day_cookie_deal_platform_1("search_page_views")
          +day_cookie_deal_platform_1("browse_page_views")
          +day_cookie_deal_platform_1("other_search_page_views"))).cast(LongType).alias("actual_page_views")
        ,when( (day_cookie_deal_platform_1("page_views") === 1), 1).otherwise(0).cast(LongType).alias("page_views_bounce")
        ,day_cookie_deal_platform_1("unique_visitors")
        ,day_cookie_deal_platform_1("unique_dv_visitors")
        ,when(bcookie_atts("cookie_first_order_id").isNotNull, day_cookie_deal_platform_1("bcookie")).alias("unique_purchasers")
        ,day_cookie_deal_platform_1("deal_views")
        ,day_cookie_deal_platform_1("unique_deal_views")
        ,day_cookie_deal_platform_1("unique_available_dvs")
        ,day_cookie_deal_platform_1("unique_buy_button_clks")
        ,day_cookie_deal_platform_1("unique_usr_buy_button_clks")
        ,day_cookie_deal_platform_1("unique_receipt_page_views")
        ,day_cookie_deal_platform_1("unique_receipt_page_viewers")
        ,day_cookie_deal_platform_1("unique_conf_page_views")
        ,day_cookie_deal_platform_1("unique_conf_page_viewers")
        ,day_cookie_deal_platform_1("unique_cart_summary_views")
        ,day_cookie_deal_platform_1("unique_cart_summary_viewers")
        ,day_cookie_deal_platform_1("unique_cart_chkout_views")
        ,day_cookie_deal_platform_1("unique_cart_chkout_viewers")
        ,day_cookie_deal_platform_1("unique_cart_receipt_views")
        ,day_cookie_deal_platform_1("unique_cart_receipt_viewers")
        ,day_cookie_deal_platform_1("unique_cart_chkout_clks")
        ,day_cookie_deal_platform_1("unique_usr_cart_chkout_clks")
        ,day_cookie_deal_platform_1("unique_cart_comp_clks")
        ,day_cookie_deal_platform_1("unique_usr_cart_comp_clks")
        ,day_cookie_deal_platform_1("unique_signin_redir_pviews")
        ,day_cookie_deal_platform_1("unique_signin_redir_pviewers")
        ,day_cookie_deal_platform_1("unique_buy_but_final_clks")
        ,day_cookie_deal_platform_1("uniq_usr_buy_but_final_clks")
        ,day_cookie_deal_platform_1("unique_goods_gallery_viewers")
        ,day_cookie_deal_platform_1("unique_ga_gallery_viewers")
        ,day_cookie_deal_platform_1("unique_occ_gallery_viewers")
        ,day_cookie_deal_platform_1("unique_loc_gallery_viewers")
        ,day_cookie_deal_platform_1("unique_navi_page_viewers")
        ,day_cookie_deal_platform_1("unique_search_page_viewers")
        ,day_cookie_deal_platform_1("unique_browse_page_viewers")
        ,day_cookie_deal_platform_1("unique_pull_viewers")
        ,day_cookie_deal_platform_1("bounce_visitors")
        ,lit("NST").alias("source_type")
        ,day_cookie_deal_platform_0("eventDate").alias("event_date")
      )

    println("day_cookie_deal_plat")

    val day_cookie = day_cookie_0.
      join(bcookie_atts, Seq("eventDate","bcookie"),"left_outer").
      select(
        current_timestamp().alias("edw_modified_ts")
        ,regexp_replace(current_date().cast(StringType), "-", "").alias("load_key")
        ,day_cookie_0("bcookie").alias("cookie_b")
        ,bcookie_atts("cookie_first_country_code")
        ,bcookie_atts("cookie_first_country_key")
        ,bcookie_atts("cookie_first_platform")
        ,bcookie_atts("cookie_first_sub_platform")
        ,bcookie_atts("cookie_first_brand")
        ,bcookie_atts("cookie_first_division")
        ,bcookie_atts("cookie_last_division")
        ,bcookie_atts("cookie_first_page_type")
        ,bcookie_atts("cookie_last_page_type")
        ,bcookie_atts("cookie_ref_attr_class_key")
        ,bcookie_atts("cookie_first_session_id")
        ,bcookie_atts("cookie_first_traf_source")
        ,bcookie_atts("cookie_first_traf_sub_source")
        ,bcookie_atts("cookie_first_utm_medium")
        ,bcookie_atts("cookie_first_utm_camp_channel")
        ,bcookie_atts("cookie_first_utm_camp_brand")
        ,bcookie_atts("cookie_first_utm_camp_inv")
        ,bcookie_atts("cookie_first_utm_camp_strategy")
        ,bcookie_atts("cookie_first_referrer_domain")
        ,bcookie_atts("cookie_first_utm_source")
        ,bcookie_atts("cookie_first_utm_campaign")
        ,bcookie_atts("cookie_first_referrer_url")
        ,bcookie_atts("cookie_first_full_url")
        ,bcookie_atts("cookie_first_browser")
        ,bcookie_atts("cookie_first_os")
        ,bcookie_atts("cookie_first_os_version")
        ,bcookie_atts("cookie_first_carrier")
        ,bcookie_atts("cookie_first_device")
        ,bcookie_atts("cookie_first_app_version")
        ,bcookie_atts("cookie_last_user_uuid")
        ,bcookie_atts("cookie_first_order_id")
        ,bcookie_atts("cookie_orderId_list")
        ,when( (day_cookie_0("goods_gallery_page_views") > 0), 1).otherwise(0).cast(IntegerType).alias("goods_gallery_pv_flag")
        ,when( (day_cookie_0("getaways_gallery_page_views") > 0), 1).otherwise(0).cast(IntegerType).alias("ga_gallery_pv_flag")
        ,when( (day_cookie_0("occasions_gallery_page_views") > 0), 1).otherwise(0).cast(IntegerType).alias("occ_gallery_pv_flag")
        ,when( (day_cookie_0("local_gallery_page_views") > 0), 1).otherwise(0).cast(IntegerType).alias("local_gallery_pv_flag")
        ,when( (day_cookie_0("browse_page_views") > 0), 1).otherwise(0).cast(IntegerType).alias("browse_local_flag")
        ,when( (day_cookie_0("search_page_views") > 0), 1).otherwise(0).cast(IntegerType).alias("search_flag")
        ,when( (day_cookie_0("navigation_page_views") > 0), 1).otherwise(0).cast(IntegerType).alias("navigation_flag")
        ,when( (day_cookie_0("browse_page_views")
          + day_cookie_0("navigation_page_views")
          + day_cookie_0("search_page_views") > 0), 1).otherwise(0).alias("pull_flag")
        ,when((day_cookie_0("available_deal_views")
          + day_cookie_0("unavailable_deal_views")
          + day_cookie_0("untagged_deal_views")) === 0, lit("No Deal View")).
          when(
            ((day_cookie_0("available_deal_views")
              + day_cookie_0("unavailable_deal_views")
              + day_cookie_0("untagged_deal_views")) > 0)
              && (day_cookie_0("deal_count") <= 1), lit("Single Deal View")).
          when(
            ((day_cookie_0("available_deal_views")
              + day_cookie_0("unavailable_deal_views")
              + day_cookie_0("untagged_deal_views")) > 0)
              && (day_cookie_0("deal_count") > 1), lit("Multi Deal View")).otherwise(lit(null).cast(StringType))
          .alias("deal_view_type")
        ,when( (day_cookie_0("page_views") === 1), 1).otherwise(0).cast(IntegerType).alias("bounce_flag")
        ,day_cookie_0("deal_count").cast(LongType)
        ,day_cookie_0("page_views")
        ,day_cookie_0("available_deal_views")
        ,day_cookie_0("unavailable_deal_views")
        ,day_cookie_0("buy_button_clicks")
        ,day_cookie_0("confirm_page_views")
        ,day_cookie_0("comp_order_button_clicks")
        ,day_cookie_0("receipt_page_views")
        ,day_cookie_0("goods_gallery_page_views")
        ,day_cookie_0("getaways_gallery_page_views")
        ,day_cookie_0("occasions_gallery_page_views")
        ,day_cookie_0("local_gallery_page_views")
        ,day_cookie_0("navigation_page_views")
        ,day_cookie_0("search_page_views")
        ,day_cookie_0("browse_page_views")
        ,day_cookie_0("other_clicks")
        ,day_cookie_0("other_page_views")
        ,day_cookie_0("other_search_page_views")
        ,lit(1).cast(LongType).alias("count_constant")
        ,day_cookie_0("cart_summary_page_views")
        ,day_cookie_0("cart_checkout_views")
        ,day_cookie_0("cart_receipt_views")
        ,day_cookie_0("cart_chkout_btn_clicks")
        ,day_cookie_0("cart_comp_order_btn_clicks")
        ,day_cookie_0("sigin_redirect_page_views")
        ,day_cookie_0("final_buy_button_clicks")
        ,bcookie_atts("session_count")
        ,bcookie_atts("ind_new_visitor")
        ,when(day_cookie_0("ind_downloader") > 0, day_cookie_0("bcookie")).alias("ind_downloader")
        ,(day_cookie_0("page_views")
          - ( day_cookie_0("navigation_page_views")
          +day_cookie_0("search_page_views")
          +day_cookie_0("browse_page_views")
          +day_cookie_0("other_search_page_views")) ).cast(LongType).alias("actual_page_views")
        ,when((day_cookie_0("page_views") === 1), 1).otherwise(0).cast(LongType).alias("page_views_bounce")
        ,day_cookie_0("unique_visitors")
        ,day_cookie_0("unique_dv_visitors")
        ,when(bcookie_atts("cookie_first_order_id").isNotNull, day_cookie_0("bcookie")).alias("unique_purchasers")
        ,day_cookie_0("deal_views")
        ,day_cookie_0("unique_deal_views")
        ,day_cookie_0("unique_available_dvs")
        ,day_cookie_0("unique_buy_button_clks")
        ,day_cookie_0("unique_usr_buy_button_clks")
        ,day_cookie_0("unique_receipt_page_views")
        ,day_cookie_0("unique_receipt_page_viewers")
        ,day_cookie_0("unique_conf_page_views")
        ,day_cookie_0("unique_conf_page_viewers")
        ,day_cookie_0("unique_cart_summary_views")
        ,day_cookie_0("unique_cart_summary_viewers")
        ,day_cookie_0("unique_cart_chkout_views")
        ,day_cookie_0("unique_cart_chkout_viewers")
        ,day_cookie_0("unique_cart_receipt_views")
        ,day_cookie_0("unique_cart_receipt_viewers")
        ,day_cookie_0("unique_cart_chkout_clks")
        ,day_cookie_0("unique_usr_cart_chkout_clks")
        ,day_cookie_0("unique_cart_comp_clks")
        ,day_cookie_0("unique_usr_cart_comp_clks")
        ,day_cookie_0("unique_signin_redir_pviews")
        ,day_cookie_0("unique_signin_redir_pviewers")
        ,day_cookie_0("unique_buy_but_final_clks")
        ,day_cookie_0("uniq_usr_buy_but_final_clks")
        ,day_cookie_0("unique_goods_gallery_viewers")
        ,day_cookie_0("unique_ga_gallery_viewers")
        ,day_cookie_0("unique_occ_gallery_viewers")
        ,day_cookie_0("unique_loc_gallery_viewers")
        ,day_cookie_0("unique_navi_page_viewers")
        ,day_cookie_0("unique_search_page_viewers")
        ,day_cookie_0("unique_browse_page_viewers")
        ,day_cookie_0("unique_pull_viewers")
        ,day_cookie_0("bounce_visitors")
        ,lit("NST").alias("source_type")
        ,day_cookie_0("eventDate").alias("event_date")
      )

    day_cookie_deal_plat.
      repartition($"source_type",$"event_date").
      write.
      partitionBy("source_type","event_date").
      mode(saveMode="Append").
      parquet("%s/prod_groupondw.gbl_traffic_superfunnel_deal".format(gdoop_edw_dir))
    // parquet("/user/aduraipandian/aduraipandian_hiveDB.db/gbl_traffic_superfunnel_deal")
    //parquet("/user/grp_gdoop_pit/grp_gdoop_pit_hiveDB.db/gbl_traffic_superfunnel_deal")


    day_cookie.
      repartition($"source_type",$"event_date").
      write.
      partitionBy("source_type","event_date").
      mode(saveMode="Append").
      parquet("%s/prod_groupondw.gbl_traffic_superfunnel".format(gdoop_edw_dir))
    //  parquet("/user/aduraipandian/aduraipandian_hiveDB.db/gbl_traffic_superfunnel")
    //parquet("/user/grp_gdoop_pit/grp_gdoop_pit_hiveDB.db/gbl_traffic_superfunnel")



    mobile_traf_2.unpersist()
    event_union.unpersist()
    event_union_final.unpersist()
    bcookie_atts.unpersist()
    day_cookie_deal_platform_0.unpersist()

    spark.stop()
  }
}

object gblSuperfunnelMobile {
  def main(args: Array[String]) {
    if (args.length == 0)
    {
      println("Dates to be processed, Missing")
      sys.exit(0)
    }
    ReadSuperfunnelConfig
    SparkConfig
    SparkEnvironment
    AppConfig.dates = args
    SuperFunnelAgg.BuildSuperFunnelAgg

  }
}