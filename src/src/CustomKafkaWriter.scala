import java.util.Calendar
import java.util.Properties
import java.util.concurrent.atomic.AtomicReference

import scala.collection._
import scala.collection.immutable.HashMap

import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

import com.paypal.datastreams.common.ContextFactory
import com.paypal.datastreams.model.WriteCdhrcMessage
import com.paypal.datastreams.tools.KafkaWrapperJ


    
/**
  * This is the implementation of the [[com.paypal.datastreams.Writer]] trait 
  * for writing to Kafka destination
  *
  * @author Murugesan Alagusundaram 
  */
protected object CustomKafkaWriter extends Writer with Logging {

  override def writeData(kafkaBroker: String, topic: String, data: DataFrame, schemaStr: String, messageSchema : String , encoding: String , debugStats : Boolean , kafkaConfig : Properties): HashMap[String, Long] = {

    logInfo("In CustomWriter")
    
    //Create the Kafka producer
    val sc = ContextFactory.getSparkContext
    
    val jsonprops = new Properties()
    jsonprops.put("bootstrap.servers", kafkaBroker)
    jsonprops.put("key.serializer", classOf[StringSerializer].getCanonicalName)
    jsonprops.put("value.serializer", classOf[StringSerializer].getCanonicalName)   
    jsonprops.put("acks","all")
    jsonprops.putAll(kafkaConfig)
    logInfo("In CustomWriter - Kafka properties:"+jsonprops)
    
    /*jsonprops.put("batch.size", "1048576");
    jsonprops.put("linger.ms", "1000");
    jsonprops.put("retries", "3");
    jsonprops.put("request.timeout.ms", "480000");
    jsonprops.put(" metadata.fetch.timeout.ms", "240000");*/
    
    val accum = sc.accumulator(0L)
    val failure = sc.accumulator(0L)

    val exceptions = sc.accumulableCollection(mutable.HashSet[(Any, Throwable)]())
    val successAcks = sc.accumulableCollection(mutable.HashSet[(Any, Integer)]())
    
    //val msgCount = sc.accumulator(0L)
    val msgMetrics = sc.accumulableCollection(mutable.HashSet[(Any, Integer)]())
    
    logInfo("Starting to publish to Kafka:::::")
    
    data.toJSON.foreachPartition(partition => {
      
          val kafkaWrapper =  KafkaWrapperJ(jsonprops)
          
          val callback = new Callback() {
                 
                 def onCompletion(metadata : RecordMetadata,  exception: Exception) {
                     if(exception != null) {
                         
                         exception.printStackTrace();
                         
                         logError("Exception occured in onCompletion", exception)
                         
                         failure += 1;
                         
                         exceptions += (System.currentTimeMillis(), exception)
                         
                     }
                     else {
                       
                       accum += 1;
                       
                       successAcks += (System.currentTimeMillis(), 1)
                       
                     }
                     
                 }
                 
                
         }
          
        var msgCount = 0
        partition.foreach(message => {
          kafkaWrapper.send(topic, message, callback)
          msgCount = msgCount + 1
        })
        
        kafkaWrapper.flush()
        
        kafkaWrapper.close
        
        msgMetrics += (System.currentTimeMillis(), msgCount)
        println("Count:::"+msgCount)
        
    })
    

    if (debugStats) { 
      println("Printing message metrics")
      msgMetrics.value.foreach{case (i, e) => {
        println(s"--- Msg metrics time-count: ($i)-($e)")
      }}
      
      
      println("Printing Success Acks ") 
      successAcks.value.foreach{case (i, e) => {
        println(s"--- Ack tx-success: ($i)-($e)")
      }}
    }
          
    println("Printing exceptions if any") 
    exceptions.value.foreach{case (i, e) => {
      println(s"--- Exception on input: ($i)")
      // using org.apache.commons.lang3.exception.ExceptionUtils
      println(ExceptionUtils.getStackTrace(e))
    }}
      
    
    logInfo("Accumulator value:::::"+accum.value )
    
    val map = HashMap("SUCCESS" -> accum.value, "FAILURE" -> failure.value)
    
    map
    
    
  }
  
}
