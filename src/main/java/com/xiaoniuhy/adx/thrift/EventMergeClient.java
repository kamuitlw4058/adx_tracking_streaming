package com.xiaoniuhy.adx.thrift;

import com.google.protobuf.InvalidProtocolBufferException;
import com.xiaoniuhy.adp.pb.clickhouse.*;
import com.xiaoniuhy.adp.thrift.*;
import com.xiaoniuhy.utils.FileUtils;
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

import com.xiaoniuhy.adx.pb.adx.adpos_events.*;
import com.xiaoniuhy.adp.pb.clickhouse.*;
import com.xiaoniuhy.adx.model.midas2.*;
import org.rocksdb.RocksDBException;


import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.JSON;


public class EventMergeClient {

    private  String host;
    private int port;
    private int timeout;

    private TTransport tTransport = null;
    private TProtocol protocol = null;

    public static void startBatchClient(List<AdxAdposEvents> trackingEvents) {
        TTransport tTransport = null;
        try {
            System.out.println("start client!");
            Properties properties = PropertiesUtils.getProperties();
            tTransport = new TSocket("localhost", Integer.parseInt(properties.getProperty("port","18989")), 30000);
            // 协议要和服务端一致
            TProtocol protocol = new TBinaryProtocol(tTransport);
            EventMergeService.Client client = new EventMergeService.Client(protocol);
            tTransport.open();
            List<ByteBuffer> trackingEventsBytes = new ArrayList<>();
            for(AdxAdposEvents tmp:trackingEvents){
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
    }


    // public AdpTrackingLogEvent startClient(AdpTrackingLogEvent trackingEvent) {
    //     AdpTrackingLogEvent event = null;
    //     TTransport tTransport = null;
    //     try {
    //         System.out.println("start client!");
    //         Properties properties = PropertiesUtils.getProperties();
    //         tTransport = new TSocket("localhost", Integer.parseInt(properties.getProperty("port","8989")), 30000);
    //         // 协议要和服务端一致
    //         TProtocol protocol = new TBinaryProtocol(tTransport);
    //         EventMergeService.Client client = new EventMergeService.Client(protocol);
    //         tTransport.open();
    //         ByteBuffer result = client.singleEvent( ByteBuffer.wrap(trackingEvent.toByteArray()));
    //         System.out.println("Thrify result bytes = " + result);
    //         byte[] bs = BytesUtils.toBytes(result);
    //         event = AdpTrackingLogEvent.parseFrom(bs);
    //         System.out.println("Thrify client result = " + event);
    //     } catch (InvalidProtocolBufferException | TException ex) {
    //         System.out.println(" InvalidProtocolBufferException");
    //         ex.printStackTrace();
    //     } finally {
    //         if (tTransport != null) {
    //             tTransport.close();
    //         }
    //     }
    //     return event;
    // }

    // public static AdpTrackingLogEvent  getEvent(ThriftClient client, String reqestId, AdpEventType eventType){
    //     AdpTrackingLogEvent.Builder trackingEventBuilder = AdpTrackingLogEvent.newBuilder();
    //     trackingEventBuilder.setRequestId(reqestId);
    //     long ts = System.currentTimeMillis();
    //     trackingEventBuilder.getTimeBuilder().setTimestamp(ts);
    //     trackingEventBuilder.setEventType(eventType);
    //     return client.startClient(trackingEventBuilder.build());
    // }
    public static MidasTrackModel parseFastjson(String j){
        ParserConfig.getGlobalInstance().putDeserializer(MidasEventTrack.class, new MidasEventTrackParser());
        //ParserConfig.getGlobalInstance().putDeserializer(int.class, new IntegerParser());
        MidasTrackModel trackModel = JSON.parseObject(j ,MidasTrackModel.class);
        //System.out.println(trackModel);
        return trackModel;
    }


    public  static  List<AdxAdposEvents> buildEvent() throws IOException{
        String j = FileUtils.readFile("data/midas_track2.json");
        //System.out.println(j);
        MidasTrackModel trackModel =  parseFastjson(j);
        List<AdxAdposEvents> events =  MidasTrackModelConvUtils.ConvToClickhouseAdxAdpos(trackModel);
        return events;
    }

    public static void main(String[] args) throws IOException {
        EventMergeClient client = new EventMergeClient();

        List<AdxAdposEvents> events = buildEvent();
        if(events != null){
            System.out.println(String.format("len:%d",events.size()));
            for(AdxAdposEvents event:events){
                System.out.print(String.format("event:%s",event.toString()));
            }
        }

        startBatchClient(events);

    }
}
