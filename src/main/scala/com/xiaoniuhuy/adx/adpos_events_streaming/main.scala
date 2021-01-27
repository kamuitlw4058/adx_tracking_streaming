package com.xiaoniuhy.adx.adpos_events_streaming

import scala.collection.mutable.ListBuffer
import java.util.ArrayList;
import scala.collection.JavaConverters._
import java.text.SimpleDateFormat 

import scala.collection.immutable.StringLike


import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.ByteBufferDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import java.nio.ByteBuffer

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.xiaoniuhy.adp.thrift.EventMergeService

import com.xiaoniuhy.adp.pb.utils.TypeConvertUtils

import com.xiaoniuhy.adp.pb.tracking.TrackingLog
import com.xiaoniuhy.adp.pb.tracking.EventType
import com.xiaoniuhy.adp.pb.tracking.BidInfo
import com.xiaoniuhy.adp.pb.clickhouse.AdpTrackingLogEvent
import com.xiaoniuhy.adp.pb.clickhouse.AdpDeviceType
import com.xiaoniuhy.adp.pb.clickhouse.AdpNetworkType
import com.xiaoniuhy.adp.pb.clickhouse.AdpGeoType
import com.xiaoniuhy.adp.pb.clickhouse.AdpSlotType
import com.xiaoniuhy.adp.pb.clickhouse.AdpBidType
import com.xiaoniuhy.adp.pb.clickhouse.AdpTimeType
import com.xiaoniuhy.adp.pb.clickhouse.AdpEventType

import com.xiaoniuhy.adx.pb.adx.adpos_events._;
import com.xiaoniuhy.adp.pb.clickhouse._;
import com.xiaoniuhy.adx.model.midas2._;
import com.xiaoniuhy.adx.thrift.EventMergeClient;
import com.xiaoniuhy.utils.BytesUtils;

import org.apache.spark.TaskContext;
object main {
     def sendBatchClient( partition:String,trackingEvents: List[AdxAdposEvents] ):AdxAdposEvents =  {
        var event:AdxAdposEvents = null;
        var tTransport:TTransport = null;
        try {
            tTransport = new TSocket("localhost", 18989, 30000);
            // 协议要和服务端一致
            var protocol = new TBinaryProtocol(tTransport);
            var client = new EventMergeService.Client(protocol);
            tTransport.open();
            var trackingEventsBytes = new ArrayList[ByteBuffer]();
            for(tmp <- trackingEvents){
                trackingEventsBytes.add( ByteBuffer.wrap(tmp.toByteArray()));
            }
            client.batchEvent( partition,trackingEventsBytes);
        } catch{
          case  ex:TException =>{
            print(" InvalidProtocolBufferException");
            ex.printStackTrace();
          }
        }
        finally {
            if (tTransport != null) {
                tTransport.close();
            }
        }
        return event;
    }


  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local[2]") 
    .setAppName("Spark_Midas2") 
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


    val ssc = new StreamingContext(conf, Seconds(30))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.66.149:9092,192.168.66.147:9092,192.168.66.161:9092,192.168.66.160:9092,192.168.66.148:9092,192.168.66.162:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[ByteBufferDeserializer],
      "group.id" -> "spark_midas2",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("bigdata_xn_point_report")
    val stream = KafkaUtils.createDirectStream[String, ByteBuffer](
      ssc,
      PreferConsistent,
      Subscribe[String, ByteBuffer](topics, kafkaParams)
    )
  stream.print()
    //stream.map(record => print((record.key, record.value)))

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
       var arrayRows = new ListBuffer[AdxAdposEvents]();
        for(  x <- iter ){
          val trackModel =  EventMergeClient.parseFastjson(BytesUtils.decode(x.value()));
          
          val events =  MidasTrackModelConvUtils.ConvToClickhouseAdxAdpos(trackModel).asScala;
          //val log = TrackingLog.parseFrom( x.value())
          //val row =  TypeConvertUtils.trackingLog2ClickhouseLog(log)
          arrayRows ++= events
        }
        if(arrayRows.length != 0){
          sendBatchClient("%d".format(TaskContext.get.partitionId),arrayRows.toList)
        }

        
      }
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()
  }
}
