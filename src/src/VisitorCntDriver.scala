
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

/**
  * Created by malagusundaram on 5/3/17.
  */
object VisitorCntDriver {

  def main(args: Array[String]): Unit = {

    // Throw an exception if the expected arguments are missing
    if (args.length < 6) throw new IllegalArgumentException("Usage: com.paypal.audience_insights.driver.MerchantReportDriver [batchDate] [batchDate - 7] [batchDate - 30] [batchDate - 60] [batchDate - 90] [rpsDate]")

    // input start partition date also used as output partition date
    val batchDate = args(0)

    // lookback dates for 7day, 14day and 30day window
    val date7 = args(1)
    val date30 = args(2)
    val date60 = args(3)
    val date90 = args(4)

    // rps date used to query partition in merchant_rps and industry_rps table
    val rpsDate = args(5)

    // Get FPTI dataframe for given range
    val fptiCatalog = CatalogFactory.getFptiCatalog

    val fpti7DayDF = fptiCatalog.getFptiDataForDateRange(date7, batchDate)

    val fpti30DayDF = fptiCatalog.getFptiDataForDateRange(date30, batchDate)

    val fpti60DayDF = fptiCatalog.getFptiDataForDateRange(date60, batchDate)

    val fpti90DayDF = fptiCatalog.getFptiDataForDateRange(date90, batchDate)

    // Get Receiver Data
    val receiverCatalog = CatalogFactory.getReceiverCatalog

    val receiverDF = receiverCatalog.getReceiver

    // Get Merchant Rps for partdate
    val merchantRpsCatalog = CatalogFactory.getMerchantRpsCatalog

    val merchantRpsDF = merchantRpsCatalog.getMerchantRpsCatalogNoDemoData(rpsDate)

    // Get Industry Rps for partdate
    val industryRpsCatalog = CatalogFactory.getIndustryRpsCatalog

    val industryRpsDF = industryRpsCatalog.getIndustryRpsCatalogNoDemoData(rpsDate)


    //val paypalRpsCatalog = CatalogFactory.getPaypalRpsCatalog
    //val paypalRpsDF = paypalRpsCatalog.getPaypalRpsCatalogNoDemoData(rpsDate)

    // Get Credit Paypal Rps for partdate
    val creditpaypalRpsCatalog = CatalogFactory.getCreditCatalog

    val creditpaypalRpsDF = creditpaypalRpsCatalog.getCreditPaypalRpsCatalog(rpsDate)

    //Get dim_cust_no_ebay_daily for partdate
    val creditCustDF=creditpaypalRpsCatalog.getCreditDataforDate(rpsDate)

    //Get dim_cr_acct for partdate
    val creditLineDF=creditpaypalRpsCatalog.getDimCrAcct(rpsDate)


    //creditLineDF.show(100,false)

    val visitorCount7day = getVisitorCount(fpti7DayDF, creditpaypalRpsDF, merchantRpsDF, receiverDF,creditCustDF,creditLineDF, "7days")

    val visitorCount30day = getVisitorCount(fpti30DayDF, creditpaypalRpsDF, merchantRpsDF, receiverDF,creditCustDF,creditLineDF, "30days")

    val visitorCount60day = getVisitorCount(fpti60DayDF, creditpaypalRpsDF, merchantRpsDF, receiverDF,creditCustDF,creditLineDF, "60days")

    val visitorCount90day = getVisitorCount(fpti90DayDF, creditpaypalRpsDF, merchantRpsDF, receiverDF,creditCustDF,creditLineDF, "90days")

    val visitorCounts = visitorCount7day.unionAll(visitorCount30day).unionAll(visitorCount60day).unionAll(visitorCount90day)

    visitorCounts.select(
      visitorCounts(Constants.MerchantId),
      visitorCounts(Constants.BusnName),
      visitorCounts(Constants.State),
      visitorCounts("count"),
      visitorCounts("type"),
      visitorCounts("time_period"),
      lit(Constants.DruidDate).as("druid_date"),
      visitorCounts(Constants.CreditQualCnt),
      visitorCounts(Constants.CreditCustCnt),
      visitorCounts(Constants.CredAvgCrLAmt)
    ).
      withColumn("dt", lit(rpsDate)).
      write.format(Constants.ORC).
      partitionBy(Constants.PartitionLabel).
      mode(SaveMode.Append).
      saveAsTable(Constants.SecureAudienceInsightsVisitorCountTable)
  }

  def getVisitorCount(fptiDf : DataFrame, creditpaypalRpsDF : DataFrame,  merchantRpsDf : DataFrame, receiverDf: DataFrame,creditCustDF:DataFrame, creditLineDF: DataFrame,timePeriod : String) : DataFrame = {

    val fptiReceiverJoin = fptiDf.join(receiverDf, Constants.MerchantId).dropDuplicates()

    val visitorsDF = fptiReceiverJoin.filter( (fptiReceiverJoin(Constants.CustId) isNull ) || (fptiReceiverJoin(Constants.CustId) === "" ) )

    // visitorsDF.show()

    val customersDF = fptiReceiverJoin.filter( !((fptiReceiverJoin(Constants.CustId) isNull) || (fptiReceiverJoin(Constants.CustId) === "")) )
      .drop(fptiReceiverJoin(Constants.GuId))
      .dropDuplicates(Seq(Constants.MerchantId, Constants.CustId, Constants.BusnName, Constants.State))

    // customersDF.show()

    val customersSubDF=customersDF.join(creditCustDF,customersDF(Constants.CustId)===creditCustDF(Constants.DimCustId),"left_outer")

    val customersCrDF=customersSubDF.withColumn(Constants.SignedCreditCust,
      when(customersSubDF(Constants.DimCustId).isNull,Constants.SignedCreditCustN).otherwise(Constants.SignedCreditCustY))

    // Join on dim_cr_acct dataset

    val customersCrLDF=customersCrDF.join(creditLineDF,customersCrDF(Constants.CustId)=== creditLineDF(Constants.PpOdmCustID),"left_outer")

    //customersCrLDF.show(100,false)

   // val customersCrLSelDF=customersCrLDF.withColumn(Constants.ShadCrLineAmtD,when(customersCrLDF(Constants.PpOdmCustID).isNotNull,
   //   Constants.ShadCrLineAmt).otherwise(0)
   // )

    //customersCrLSelDF.show()

    // Join with paypal RPS to get active shoppers count
    // active shoppers are the paypal customers who transacted at least once in last one year

    val customerMerchantRpsJoinDf = customersCrLDF.join(merchantRpsDf,
      (customersCrLDF(Constants.CustId) === merchantRpsDf(Constants.CustId)) && (customersCrLDF(Constants.MerchantId) === merchantRpsDf(Constants.MerchantId)) ,
      "left_outer").
      drop(merchantRpsDf(Constants.CustId)).
      drop(merchantRpsDf(Constants.MerchantId)).
      drop(merchantRpsDf(Constants.BusnName)).
      drop(customersCrLDF(Constants.DimCustId))


    val merchantCustomerDf  = customerMerchantRpsJoinDf.filter(
      !(
        (customerMerchantRpsJoinDf(Constants.MerchantFrequencyBucket) isNull) ||
          (customerMerchantRpsJoinDf(Constants.MerchantFrequencyBucket) === ""))
    )

    val merchantCustomerCDf =merchantCustomerDf.join(creditpaypalRpsDF, merchantCustomerDf(Constants.CustId) === creditpaypalRpsDF(Constants.CustId)
      , "left_outer")
      .drop(creditpaypalRpsDF(Constants.CustId))

    // merchantCustomerDf.show(100)

    //merchantCustomerDf.show(200)
    //pr/intf("\n Checkpoint : completed merchantCustomerDf show \n")

    val nonMerchantCustomerDf = customerMerchantRpsJoinDf.filter(
      (customerMerchantRpsJoinDf(Constants.MerchantFrequencyBucket) isNull) ||
        (customerMerchantRpsJoinDf(Constants.MerchantFrequencyBucket) === "")
    )

    //nonMerchantCustomerDf.show(200)
    //printf("\n Checkpoint : completed nonMerchantCustomerDf show \n")

    val customerRpsJoinDF = nonMerchantCustomerDf.join(creditpaypalRpsDF, nonMerchantCustomerDf(Constants.CustId) === creditpaypalRpsDF(Constants.CustId)
      , "left_outer")
      .drop(creditpaypalRpsDF(Constants.CustId))

    //customerRpsJoinDF.show(200)
    //printf("\n Checkpoint : completed customerRpsJoinDF show \n")

    val activeShoppersDf = customerRpsJoinDF.filter( customerRpsJoinDF(Constants.PaypalFrequencyBucket) isNotNull)

     activeShoppersDf.show()
    printf("\n Checkpoint : completed activeShoppersDf show \n")

    val inactiveShoppersDf = customerRpsJoinDF.filter(customerRpsJoinDF(Constants.PaypalFrequencyBucket) isNull)

    val visitorCountDf = visitorsDF.groupBy(visitorsDF(Constants.MerchantId), visitorsDF(Constants.BusnName)
      , visitorsDF(Constants.State)
    ).
      agg(countDistinct(visitorsDF(Constants.GuId)).as("count")).
      withColumn("type", lit("visitor"))

    val visitorCount = visitorCountDf.
      select(
        visitorCountDf(Constants.MerchantId),
        visitorCountDf(Constants.BusnName),
        visitorCountDf(Constants.State),
        visitorCountDf("count"),
        visitorCountDf("type"),
        lit(timePeriod).as("time_period"),
        lit(0).as(Constants.CreditQualCnt),
        lit(0).as(Constants.CreditCustCnt),
        lit(0).as(Constants.CredAvgCrLAmt)

      )


    val temp=null

    val activeShopperCountDf = activeShoppersDf.groupBy(activeShoppersDf(Constants.MerchantId), activeShoppersDf(Constants.BusnName)
      , activeShoppersDf(Constants.State)
    ).
      agg( countDistinct(when(activeShoppersDf(Constants.TransCreditFlag) === "Y",activeShoppersDf(Constants.CustId))
        .otherwise(temp)).alias(Constants.CreditQualCnt),
        countDistinct(when(activeShoppersDf(Constants.SignedCreditCust) === "Y",activeShoppersDf(Constants.CustId))
          .otherwise(temp)).alias(Constants.CreditCustCnt),
        sum(activeShoppersDf(Constants.ShadCrLineAmt)).alias("sum_credit_line_amt"),
        //avg(activeShoppersDf(Constants.ShadCrLineAmt)).alias(Constants.CredAvgCrLAmt),
        countDistinct(activeShoppersDf(Constants.CustId)).as("count")).
      withColumn("type", lit("active_shopper"))

    val activeShopperCount = activeShopperCountDf.
      select(
        activeShopperCountDf(Constants.MerchantId),
        activeShopperCountDf(Constants.BusnName),
        activeShopperCountDf(Constants.State),
        activeShopperCountDf("count"),
        activeShopperCountDf("type"),
        lit(timePeriod).as("time_period"),
        activeShopperCountDf(Constants.CreditQualCnt),
        activeShopperCountDf(Constants.CreditCustCnt),
        getAvgCrdline(activeShopperCountDf("sum_credit_line_amt"),activeShopperCountDf("count")).alias(Constants.CredAvgCrLAmt)
      )

   // activeShopperCount.show(200)
    printf("\n Checkpoint : completed activeShopperCount show \n")

    val inactiveShopperCountDf = inactiveShoppersDf.groupBy(inactiveShoppersDf(Constants.MerchantId), inactiveShoppersDf(Constants.BusnName)
      , inactiveShoppersDf(Constants.State)
    ).
      agg(  countDistinct(when(inactiveShoppersDf(Constants.TransCreditFlag) === "Y",inactiveShoppersDf(Constants.CustId))
        .otherwise(temp)).alias(Constants.CreditQualCnt),
        countDistinct(when(inactiveShoppersDf(Constants.SignedCreditCust) === "Y",inactiveShoppersDf(Constants.CustId))
          .otherwise(temp)).alias(Constants.CreditCustCnt),
        sum(inactiveShoppersDf(Constants.ShadCrLineAmt)).alias("sum_credit_line_amt"),
        //avg(inactiveShoppersDf(Constants.ShadCrLineAmt)).alias(Constants.CredAvgCrLAmt),
        countDistinct(inactiveShoppersDf(Constants.CustId)).as("count")).
      withColumn("type", lit("inactive_shopper"))

    val inactiveShopperCount = inactiveShopperCountDf.
      select(
        inactiveShopperCountDf(Constants.MerchantId),
        inactiveShopperCountDf(Constants.BusnName),
        inactiveShopperCountDf(Constants.State),
        inactiveShopperCountDf("count"),
        inactiveShopperCountDf("type"),
        lit(timePeriod).as("time_period"),
        inactiveShopperCountDf(Constants.CreditQualCnt),
        inactiveShopperCountDf(Constants.CreditCustCnt),
        getAvgCrdline(inactiveShopperCountDf("sum_credit_line_amt"),inactiveShopperCountDf("count")).alias(Constants.CredAvgCrLAmt)

      )

    val merchantShopperCountDf = merchantCustomerCDf.groupBy(merchantCustomerCDf(Constants.MerchantId), merchantCustomerCDf(Constants.BusnName)
      , merchantCustomerCDf(Constants.State)
    ).
      agg(countDistinct(when(merchantCustomerCDf(Constants.TransCreditFlag) === "Y",merchantCustomerCDf(Constants.CustId))
        .otherwise(temp)).alias(Constants.CreditQualCnt),
        countDistinct(when(merchantCustomerCDf(Constants.SignedCreditCust) === "Y",merchantCustomerCDf(Constants.CustId))
          .otherwise(temp)).alias(Constants.CreditCustCnt),
        sum(inactiveShoppersDf(Constants.ShadCrLineAmt)).alias("sum_credit_line_amt"),
        //avg(merchantCustomerCDf(Constants.ShadCrLineAmt)).alias(Constants.CredAvgCrLAmt),
        count(merchantCustomerCDf(Constants.CustId)).as("count")).
      withColumn("type", lit("merchant_shopper"))

    val merchantShopperCount = merchantShopperCountDf.
      select(
        merchantShopperCountDf(Constants.MerchantId),
        merchantShopperCountDf(Constants.BusnName),
        merchantShopperCountDf(Constants.State),
        merchantShopperCountDf("count"),
        merchantShopperCountDf("type"),
        lit(timePeriod).as("time_period"),
        merchantShopperCountDf(Constants.CreditQualCnt),
        merchantShopperCountDf(Constants.CreditCustCnt),
        getAvgCrdline(merchantShopperCountDf("sum_credit_line_amt"),merchantShopperCountDf("count")).alias(Constants.CredAvgCrLAmt)

      )

    val  res = visitorCount.unionAll(activeShopperCount).unionAll(inactiveShopperCount).unionAll(merchantShopperCount)

    res

  }

  val getAvgCrdline = udf ((sum_credit_line_amt: Double, count: Int) => {
    val avgCrdline = sum_credit_line_amt / count
    avgCrdline
  })


}
