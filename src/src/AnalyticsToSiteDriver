import java.io.IOException
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import com.paypal.datastreams.dao.MetadataFactory
import com.paypal.datastreams.model.ProducerJobConfig
import com.paypal.datastreams.reader.ReaderFactory
import com.paypal.datastreams.reader.ReaderType
import com.paypal.datastreams.utils.Constants
import com.paypal.datastreams.utils.Utils
import com.paypal.datastreams.writer.WriterFactory
import com.paypal.datastreams.writer.WriterType
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.spark.Logging
import org.apache.avro.Schema
import com.paypal.datastreams.common.ContextFactory
import com.paypal.datastreams.schema.SchemaFactory
import com.paypal.datastreams.schema.EncodingType
import com.paypal.datastreams.utils.CustomUtils
import org.apache.spark.SparkException
import java.io.FileWriter
import java.io.BufferedWriter
import java.io.File
import java.util.Properties
import java.io.FileInputStream

/**
  * This is the application driver that controls the flow of execution for 
  * reading from an analytics infrastructure and publish to a site infra
  *
  * @author Murugesan Alagusundaram
  */
object AnalyticsToSiteDriver extends Logging {
 
    def main(args: Array[String]){
      
      logInfo("Running Job Name:"+Constants.APPNAME)
      
      //Read configuration from a yaml file 
      val configFilePathOnHDFS = args(0)
      logInfo("Loading yaml file::::::"+configFilePathOnHDFS)
      
      val config = Utils.getJobConfigurations(configFilePathOnHDFS)    
      logInfo("In Driver, Job configuration:"+config)
       
      val donefile = args(1)
      logInfo("Done file will be written to:"+donefile)
      
      val debugStats = args(2).toBoolean
           
      var kafkaConfig = new Properties()
      if (args.length == 4) {
        kafkaConfig.load(new FileInputStream(args(3)))
      }
      else { //Loading properties in resource
        kafkaConfig.load(getClass.getClassLoader.getResourceAsStream("kafkaconfig.properties"))
      }
        
      logInfo("Loading config from properties:"+kafkaConfig)

      
      //Read the metadata for replication status for these partitions - Commented as we process all partitions and do not check for only COMPLETED status
      val metadataDao = MetadataFactory.getMetadataDao
      /*var replnInfoMap = null : Map[String,String]
      try {
       replnInfoMap = metadataDao.getReplicationStatus(config.srcTable,config.partitionColumn,
          config.partitionValues)
          logInfo("HBase metadata read successfully")
      }
      catch {
       case e: Throwable => {
         logError("Exception occurred when connecting to HBase metadata")
         throw e
       }
      }*/
     
      //Write details to the done file before starting the processing
      val file = new File(donefile)
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write("TABLE="+config.srcTable)
      bw.newLine()
      bw.write("NOTIFY="+config.notifyTo)
      bw.newLine()
      bw.write(if (config.failureNotifyTo.isEmpty()) "FAILURENOTIFY="+config.notifyTo else "FAILURENOTIFY="+config.failureNotifyTo)
      bw.newLine()
      bw.close()
      
      // Get Reader 
      val reader = ReaderFactory.getReader(ReaderType.Hive)
      
      config.partitionValues.split(",").foreach(partitionValue => {
            
          //Get status of replication
          //val status = replnInfoMap.get(partitionValue)
          
          logInfo("Processing for partition:"+partitionValue)
          
          //Do not do status check. Process if the partition is requested.
          /*status match {
            case Some("COMPLETED") => logInfo("Status for partition "+ partitionValue +" is COMPLETED")
            case _ => 
              { logInfo("Status for partition "+ partitionValue +" is not COMPLETED. Proceed to process.") */
          
                  // Get incremental data to publish
                  var incrementalDF = reader.getIncrementalDF(config.srcTable, config.partitionColumn, partitionValue)                  
                  var dataCount = incrementalDF.count()
                  logInfo("Data count:"+dataCount)
                  
                  if (dataCount <= 0) {
                    logInfo("No data for partition.Exiting with code 2")
                    System.exit(2)
                  }
                  /*if (dataCount <= 0)  { 
                    
                    logInfo("Data is not available. Polling every "+ config.retryinterval + " seconds for "+config.maxretrycount+" times")
                    
                    /*
                     * Retry while count <= retrycount. 
                     * Initialize count to 1 as already data is polled for once
                     */
                    var retryCount = 1 
                    while(retryCount < config.maxretrycount.toInt && dataCount <= 0) {
                      
                      Thread.sleep(config.retryinterval.toInt*1000) //In milliseconds
                      incrementalDF = reader.getIncrementalDF(config.srcTable, config.partitionColumn, partitionValue)
                      dataCount = incrementalDF.count()
                      logInfo("Polling for data. Tried:"+retryCount+" times. Data count:"+dataCount)
                      retryCount +=1
                      
                    }
                    
                    if (dataCount <= 0) {
                      logInfo("Polled "+retryCount+" times. No data for partition:"+partitionValue+".Nothing to process.")
                      throw new SparkException("No data for partition "+partitionValue)
                    }
                    
                  }*/
          
                  //Data is available. Process the partition
                  var successCount, failureCount = None : Option[Long]
                  var writeToDoneFile = "" : String
          
                  //Insert into metadata to started
                  try {
                  metadataDao.replicationStarted(config.srcTable, config.partitionColumn, partitionValue)
                  }
                  catch {
                   case e: Throwable => {
                     logError("Exception occurred when connecting to HBase metadata to write status as STARTED")
                     throw e
                   }
                  }
              
                  
                  if (config.custom) {
    
                    logInfo("Custom logic for Producer")
                    
                    //Load custom Config File
                    val tableConfig = Utils.getTableConfigurations(config.customConfigFile)
                    logInfo("Table config:"+tableConfig)
                    
                    //Generate CustomDF from dataFrame
                    val (customDF,index) = CustomUtils.getModifiedDF(incrementalDF,  tableConfig, partitionValue)
                    logInfo("Custom DF generated.")
                    
                    // Get Writer
                    val writer =  WriterFactory.getCustomWriter()

                    // Write dataframe to Kafka
                    try {
                    val map = writer.writeData(config.kafkaBroker, config.kafkaTopic, customDF, "", "", config.encoding, debugStats, kafkaConfig); 
                    
                    successCount = map.get("SUCCESS")
                    failureCount = map.get("FAILURE")
                    writeToDoneFile = "INDEX="+index
                    }
                    catch {
                       case e: Throwable => {
                         logError("Exception occurred when connecting to Kafka to write data")
                         throw e
                       }
                    }
                    
                  }
                  else {
  
                    //Get Parent Schema
                    logInfo("Connecting to schema registry:"+config.schemaRegistry+"to fetch parent schema")
                    val messageSchema = SchemaFactory.getSchema(config.schemaRegistry,EncodingType.withName(config.encoding))
                    logInfo("Parent message schema from registry :"+messageSchema)
     
                     var pyldSchemaFromReg = None : Option[String]
                     try {
                       pyldSchemaFromReg = Some(Utils.getSchema(config.schemaRegistry, config.srcTable))
                      }
                      catch {
                       case e: RestClientException => {
                         logInfo("Payload schema for this table is not registered in schema")
                       }
                       case e: Throwable => {
                         logError("Exception occurred when connecting to schema registry metadata")
                         throw e
                       }
                      }
                      logInfo("source table schema from registry :"+pyldSchemaFromReg)
      
                       //Generate the AVRO schema from the HIVE table schema
                        val schemaStr = Utils.extractPayloadSchema(config.srcTable, incrementalDF.schema)
                        logInfo("Payload Schema extracted from HIVE schema:"+schemaStr)
                       
                          /**
                           * Dynamically generates the payload schema
                           * 
                           * Check if payload schema is available in registry -
                           * 	- If NO - Upload the schema to registry
                           *  - If YES - Compare existing schema and newly generated schema
                           *  				 - If the schemas are matching proceed
                           *  				 - If the schemas are not matching upload the newly generated 
                           *  					 schema with new version number 
                           * 
                           * 
                           */
                        if ((pyldSchemaFromReg == None) || (!(pyldSchemaFromReg.get == schemaStr))) {
                        
                          logInfo("Schema for "+config.srcTable+" is not updated in registry")
                            try { //Register schema with the source table name as subject
                              Utils.registerSchema(config.schemaRegistry, config.srcTable, schemaStr)
                              logInfo("Registered schema for "+config.srcTable+" in registry")
                              pyldSchemaFromReg = Some(schemaStr)
                            }
                            catch {
                              case ex: RestClientException => 
                              {
                                  logError("RestClientException occurred while trying to register schema for:"+config.srcTable+" Exception:"+ex.getErrorCode)
                                  throw ex
                        
                               }
                               case ex: IOException => {
                                  logError("IOException occurred while trying to register schema for:"+config.srcTable+" Exception:"+ex.getMessage)
                                  throw ex
                               }
                            }
                        }
                        else {
                          logInfo("Schema for "+config.srcTable+" is already in registry")
                        }
                            
                        // Get Writer
                        val writer = WriterFactory.getWriter(WriterType.Kafka)

                        // Write dataframe to Kafka
                        try {
                            val map = writer.writeData(config.kafkaBroker, config.kafkaTopic, incrementalDF, schemaStr, messageSchema, config.encoding, debugStats, kafkaConfig); 
                            logInfo("Written to Kafka")
                            
                            successCount = map.get("SUCCESS")
                            failureCount = map.get("FAILURE")
                        }
                        catch {
                        case e: Throwable => {
                           logError("Exception occurred when connecting to Kafka to write data")
                           throw e
                       }
                    }
                    
                  }
                      
                  //Perform data-count validation. If matching mark status as COMPLETED
                  logInfo("Success Count:"+successCount)
                  logInfo("Failure Count:"+failureCount)
                  logInfo("Data Row Count:"+dataCount)
                  
                  if (successCount.get > 0 ) {
                        //Compare counts and do validation
                        if (dataCount == successCount.get) {
                        
                          logInfo("Data and Kafka ACKS success count matching")
                          
                          //Update replication status to COMPLETED
                          metadataDao.replicationCompleted(config.srcTable, config.partitionColumn, partitionValue, successCount.get)
                          
                        }
                        else {
                          logInfo("Data and Kafka ACKS success count not matching")
                          
                          //This partition is not COMPLETED.
                          metadataDao.replicationPartiallyCompleted(config.srcTable, config.partitionColumn, partitionValue, successCount.get, dataCount)
                          throw new SparkException("Replication partially completed for partition:"+partitionValue)
                         
                        }
                        logInfo("Status updated in table")
                        
                        // FileWriter
                        val file = new File(donefile)
                        val bw = new BufferedWriter(new FileWriter(file, true)) //Append to the file
                        bw.write("COUNT="+successCount.get.toString())
                        bw.newLine()
                        if (!writeToDoneFile.isEmpty()) {
                          bw.write(writeToDoneFile)
                          bw.newLine()
                        }
                        bw.close()
                  }
                  else {
                    //Success count is 0. Fail the job
                    //This partition is FAILED.
                    metadataDao.replicationFailed(config.srcTable, config.partitionColumn, partitionValue, successCount.get, dataCount)
                    throw new SparkException("Replication failed for partition:"+partitionValue)
                    
                  }
                      
                  
            // } //End of processing completion condition
              
       //   } //End of Match condition for COMPLETED vs Any other status

      }) //End of looping thru partition values
      
    }//End of main
  
}
