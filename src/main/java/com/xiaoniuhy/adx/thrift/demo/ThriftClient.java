package com.xiaoniuhy.adp.thrift.demo;

import com.google.protobuf.InvalidProtocolBufferException;
import com.xiaoniuhy.adp.pb.clickhouse.*;
import com.xiaoniuhy.adp.thrift.*;
import com.xiaoniuhy.utils.BytesUtils;
import com.xiaoniuhy.utils.PropertiesUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ThriftClient {

    private  String host;
    private int port;
    private int timeout;

    private TTransport tTransport = null;
    private TProtocol protocol = null;

    public AdpTrackingLogEvent startBatchClient(List<AdpTrackingLogEvent> trackingEvents) {
        AdpTrackingLogEvent event = null;
        TTransport tTransport = null;
        try {
            System.out.println("start client!");
            Properties properties = PropertiesUtils.getProperties();
            tTransport = new TSocket("localhost", Integer.parseInt(properties.getProperty("port","8989")), 30000);
            // 协议要和服务端一致
            TProtocol protocol = new TBinaryProtocol(tTransport);
            EventMergeService.Client client = new EventMergeService.Client(protocol);
            tTransport.open();
            List<ByteBuffer> trackingEventsBytes = new ArrayList<>();
            for(AdpTrackingLogEvent tmp:trackingEvents){
                trackingEventsBytes.add(ByteBuffer.wrap(tmp.toByteArray()));
            }
            client.batchEvent("test", trackingEventsBytes);
        } catch ( TException ex) {
            System.out.println(" InvalidProtocolBufferException");
            ex.printStackTrace();
        } finally {
            if (tTransport != null) {
                tTransport.close();
            }
        }
        return event;
    }


    public AdpTrackingLogEvent startClient(AdpTrackingLogEvent trackingEvent) {
        AdpTrackingLogEvent event = null;
        TTransport tTransport = null;
        try {
            System.out.println("start client!");
            Properties properties = PropertiesUtils.getProperties();
            tTransport = new TSocket("localhost", Integer.parseInt(properties.getProperty("port","8989")), 30000);
            // 协议要和服务端一致
            TProtocol protocol = new TBinaryProtocol(tTransport);
            EventMergeService.Client client = new EventMergeService.Client(protocol);
            tTransport.open();
            ByteBuffer result = client.singleEvent( ByteBuffer.wrap(trackingEvent.toByteArray()));
            System.out.println("Thrify result bytes = " + result);
            byte[] bs = BytesUtils.toBytes(result);
            event = AdpTrackingLogEvent.parseFrom(bs);
            System.out.println("Thrify client result = " + event);
        } catch (InvalidProtocolBufferException | TException ex) {
            System.out.println(" InvalidProtocolBufferException");
            ex.printStackTrace();
        } finally {
            if (tTransport != null) {
                tTransport.close();
            }
        }
        return event;
    }

    public static AdpTrackingLogEvent  getEvent(ThriftClient client, String reqestId, AdpEventType eventType){
        AdpTrackingLogEvent.Builder trackingEventBuilder = AdpTrackingLogEvent.newBuilder();
        trackingEventBuilder.setRequestId(reqestId);
        long ts = System.currentTimeMillis();
        trackingEventBuilder.getTimeBuilder().setTimestamp(ts);
        trackingEventBuilder.setEventType(eventType);
        return client.startClient(trackingEventBuilder.build());
    }

    public  static AdpTrackingLogEvent buildEvent( String reqestId, AdpEventType eventType){
        AdpTrackingLogEvent.Builder trackingEventBuilder = AdpTrackingLogEvent.newBuilder();
        trackingEventBuilder.setRequestId(reqestId);
        long ts = System.currentTimeMillis();
        System.out.println(ts);
        trackingEventBuilder.getTimeBuilder().setTimestamp(ts);
        trackingEventBuilder.setEventType(eventType);
        return trackingEventBuilder.build();
    }

    public static void main(String[] args) throws IOException {
        ThriftClient client = new ThriftClient();

//        AdpTrackingLogEvent event = getEvent(client,"1", AdpLogEvent.AdpEventType.Click);
//        AdpTrackingLogEvent event2 = getEvent(client,"2",AdpLogEvent.AdpEventType.Impression);
//        AdpTrackingLogEvent event3 = getEvent(client,"2",AdpLogEvent.AdpEventType.Impression);
//        AdpTrackingLogEvent event4 = getEvent(client,"4",AdpLogEvent.AdpEventType.Impression);
//        AdpTrackingLogEvent event5 = getEvent(client,"4",AdpLogEvent.AdpEventType.Impression);


        List<AdpTrackingLogEvent> events = new ArrayList<>();

        AdpTrackingLogEvent event = buildEvent("1", AdpEventType.Click);
        AdpTrackingLogEvent event2 = buildEvent("2",AdpEventType.Click);
        AdpTrackingLogEvent event3 = buildEvent("2",AdpEventType.Landing);
        AdpTrackingLogEvent event4 = buildEvent("4",AdpEventType.Click);
        AdpTrackingLogEvent event5 = buildEvent("4",AdpEventType.Impression);
        events.add(event);
        events.add(event2);
        events.add(event3);
        events.add(event4);
        events.add(event5);


        client.startBatchClient(events);




    }
}
