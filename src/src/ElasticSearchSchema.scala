import common.ConfigurationManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext

/**
  * This class features methods for manipulating regular data frames into
  * Elasticsearch compliant dataframes.
  *
  * @author Murugesan Alagusundaram
  *
  */
object ElasticSearchSchema {

  private val FieldNameTenant = "tenant"
  private val FieldValTenant = ConfigurationManager.getConfigString("constants.es.values.tenant")
  private val FieldNameStore = "store"
  private val FieldNameEntity = "entity"
  private val FieldValEntity = ConfigurationManager.getConfigString("constants.es.values.entity")
  private val FieldNameData = "data"


  def getESCompliantDF(inputDF: DataFrame, store: String) (implicit hc: HiveContext): DataFrame = {

    val esSchema = getESSchema(inputDF)

    val outputRDD = inputDF.map(row => Row(FieldValTenant, store, FieldValEntity, row))

    hc.createDataFrame(outputRDD, esSchema)

  }

  private def getESSchema(inputDF: DataFrame): StructType = {

    StructType(
      StructField(FieldNameTenant, StringType) ::
        StructField(FieldNameStore, StringType) ::
        StructField(FieldNameEntity, StringType) ::
        StructField(FieldNameData, StructType(
          inputDF.schema.fields.map[StructField, Array[StructField]](field => {
            StructField(field.dataType match {
              case LongType => field.name + "_lcount"
              case IntegerType => field.name + "_icount"
              case ShortType => field.name + "_scount"
              case ByteType => field.name + "_bcount"
              case DoubleType => field.name + "_dcount"
              case FloatType => field.name + "_fcount"
              case StringType => "sn_" + field.name
              case BooleanType => "b_" + field.name
              case TimestampType => "ts_" + field.name
              case default => field.name
            }, field.dataType)
          }).toSeq
        )) :: Nil)

  }

}
