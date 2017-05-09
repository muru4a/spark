import org.apache.spark.sql.DataFrame

/**
  * Created by malagusundaram on 3/2/17.
  */
trait CreditCatalog {

  def getTransDataForDateRange(startDate: String, endDate: String) : DataFrame

  def getCreditDataforDate(partDate: String) : DataFrame

  def getCreditPaypalRpsCatalog(partDate: String) : DataFrame

  def getDimCrAcct(partDate: String) : DataFrame

  def getDimChosFmxForDate : DataFrame

  def getMarsTxnFactForDate(partDate: String) : DataFrame

}
