package com.xiaoniuhy.adp.tmp;
import com.xiaoniuhy.utils.PropertiesUtils;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import com.xiaoniuhy.adp.rocks.EventMergeRocks;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import io.prometheus.client.exporter.HTTPServer;

import java.io.IOException;
import java.util.Properties;

import  com.xiaoniuhy.adp.pb.clickhouse.*;


public class test_pb {

    public static AdpTrackingLogEvent buildEvent(AdpEventType eventType){
        AdpTrackingLogEvent.Builder builder = AdpTrackingLogEvent.newBuilder();
        builder.setEventType(eventType);

        AdpTimeType.Builder timeBuilder = builder.getTimeBuilder();
        timeBuilder.setTimestamp(System.currentTimeMillis());

        AdpInteractionType.Builder intercationBuilder = null;
        switch (builder.getEventType()){
            case Impression:
                intercationBuilder = builder.getImpressionBuilder();
                break;
            case Click:
                intercationBuilder = builder.getClickBuilder();
                break;
            case Landing:
                intercationBuilder = builder.getLandingBuilder();
                break;
        }
        if(intercationBuilder != null){
            intercationBuilder.addIp(0);
            intercationBuilder.addTimestamp(builder.getTime().getTimestamp());
        }

        AdpTrackingLogEvent oldEvent  = builder.build();
        return oldEvent;
    }

    public static AdpTrackingLogEvent setDevice(AdpTrackingLogEvent event,String model){
        AdpTrackingLogEvent.Builder builder = event.toBuilder();
        AdpDeviceType.Builder deviceBuilder  =  builder.getDeviceBuilder();
        deviceBuilder.setModel(model);
        return builder.build();

    }

    public static void main(String[] args){
        System.out.println("start pb test");
        AdpTrackingLogEvent oldEvent = buildEvent(AdpEventType.Impression);
        oldEvent = setDevice(oldEvent,"huawei");

        System.out.println("Old event:");
        System.out.println(oldEvent);

        System.out.println("************");
        AdpTrackingLogEvent newEvent = buildEvent(AdpEventType.Click);
        System.out.println("new event:");
        System.out.println(newEvent);
        try {
        Thread.sleep(10);
        } catch (Exception e) {
            //TODO: handle exception
        }
        System.out.println("************");
        AdpTrackingLogEvent newEvent2 = buildEvent(AdpEventType.Click);
        System.out.println("new event2:");
        System.out.println(newEvent2);

        System.out.println("************");
        AdpTrackingLogEvent mergedEvent = oldEvent.toBuilder().mergeFrom(newEvent).mergeFrom(newEvent2).build();
        System.out.println(mergedEvent);
        System.out.println(mergedEvent.getMergeCount());

        

       
    }
}
