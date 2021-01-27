package com.xiaoniuhy.adp.thrift;

import com.xiaoniuhy.utils.BytesUtils;
import com.xiaoniuhy.utils.PropertiesUtils;
import com.xiaoniuhy.utils.IpUtils;
import org.apache.thrift.TException;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

import com.xiaoniuhy.adx.pb.adx.adpos_events.*;
import com.xiaoniuhy.adp.pb.clickhouse.*;
import org.rocksdb.RocksDBException;
import io.prometheus.client.Summary;
import com.google.protobuf.InvalidProtocolBufferException;

public class EventMergeServiceImpl implements EventMergeService.Iface {
    private final RocksDB rocksDB;
    public EventMergeServiceImpl(RocksDB rocksDB){
        this.rocksDB = rocksDB;
    }

    static final Summary requestLatency = Summary.build()
    .quantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
    .quantile(0.8, 0.01)   // Add 90th percentile with 1% tolerated error
    .quantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
    .quantile(0.95, 0.01)   // Add 90th percentile with 1% tolerated error
    .quantile(0.99, 0.01)   // Add 90th percentile with 1% tolerated error
    .name("EventMerge_requests_latency_seconds")
    .help("Event Merge Request latency in seconds.").register();

    public byte[] process(ByteBuffer log,FileOutputStream fos) {
        byte[] ret = null;
        byte[] bs = BytesUtils.toBytes(log);
        processRow(bs,fos);
        return ret;
    }

    public void  processSourceEvent(AdxAdposEvents.Builder builder,FileOutputStream fos){
        try {
            byte[] sourceKeyBytes = getSourceIdKey(builder.getAdSource().getId());
            byte[] oldSourceValue =  rocksDB.get(sourceKeyBytes);

            if(oldSourceValue != null && oldSourceValue.length > 0){
                AdxAdposEvents.Builder oldSourceAdxAdposEvents = AdxAdposEvents.parseFrom(oldSourceValue).toBuilder();
                oldSourceAdxAdposEvents = oldSourceAdxAdposEvents.clearImpression();
                oldSourceAdxAdposEvents = oldSourceAdxAdposEvents.clearClick();
                oldSourceAdxAdposEvents = oldSourceAdxAdposEvents.clearReward();
                builder =  oldSourceAdxAdposEvents.mergeFrom(builder.build());
            }
            builder =updateEventDate(builder);
            builder =updateMergeCount(builder);
            AdxAdposEvents mergedEvent =  builder.build();
            byte[]  value = mergedEvent.toByteArray();
            rocksDB.put(sourceKeyBytes,value);
            //mergedEvent.writeDelimitedTo(fos);
        } catch (RocksDBException | IOException e) {
            e.printStackTrace();
        }
        
    }
    public byte[] getSourceId2SessionIdKey(String sourceSessionId){
        return  String.format("source_session_%s",sourceSessionId).getBytes();
    }


    public byte[] getSessionIdKey(String sessionId){
        return  String.format("session_%s",sessionId).getBytes();
    }

    public byte[] getSourceIdKey(String sessionId){
        return  String.format("source_%s",sessionId).getBytes();
    }

    public AdxAdposEvents.Builder updateMergeCount(AdxAdposEvents.Builder builder){
        Integer mergeCount = builder.getMergeCount();
        if(mergeCount != null){
            mergeCount = mergeCount +1;
        }
        else{
            mergeCount = 1;
        }

        builder.setMergeCount(mergeCount);
        return builder;
    }


    public AdxAdposEvents.Builder updateEventDate(AdxAdposEvents.Builder builder){
        if(builder.getEventDate() == null){
            String eventDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date(builder.getTime().getTimestamp()));
            builder.setEventDate(eventDate);
        }
        return builder;
    }

    public void processAdposEvent(AdxAdposEvents.Builder builder,FileOutputStream fos){

        try {

            byte[] keyBytes = getSessionIdKey(builder.getSession().getId());
            byte[] oldValue =  rocksDB.get(keyBytes);
            byte[] sourceKeyBytes = null;
            byte[] oldSourceValue = null;

            AdxAdposEvents.Builder oldSourceAdxAdposEvents = null;
            if( builder.hasAdSource() && builder.getAdSource().getId() != null) {
                sourceKeyBytes = getSourceIdKey(builder.getAdSource().getId());
                oldSourceValue =  rocksDB.get(sourceKeyBytes);
                if(oldSourceValue != null && oldSourceValue.length > 0){
                    oldSourceAdxAdposEvents = AdxAdposEvents.parseFrom(oldSourceValue).toBuilder();
                    //rocksDB.put(getSourceId2SessionIdKey(builder.getAdSource().getId()),builder.getSession().getId());
                }
            }



            if(oldValue != null && oldValue.length > 0){
                AdxAdposEvents.Builder oldAdxAdposEvents = AdxAdposEvents.parseFrom(oldValue).toBuilder();
                if(oldSourceAdxAdposEvents != null){
                    oldSourceAdxAdposEvents = oldSourceAdxAdposEvents.clearImpression();
                    oldSourceAdxAdposEvents = oldSourceAdxAdposEvents.clearClick();
                    oldSourceAdxAdposEvents = oldSourceAdxAdposEvents.clearReward();
                    oldAdxAdposEvents =  oldAdxAdposEvents.mergeFrom(oldSourceAdxAdposEvents.build());
                }

                builder =  oldAdxAdposEvents.mergeFrom(builder.build());
            }
            else{
                if(oldSourceAdxAdposEvents != null){
                    oldSourceAdxAdposEvents = oldSourceAdxAdposEvents.clearImpression();
                    oldSourceAdxAdposEvents = oldSourceAdxAdposEvents.clearClick();
                    oldSourceAdxAdposEvents = oldSourceAdxAdposEvents.clearReward();
                    builder =  oldSourceAdxAdposEvents.mergeFrom(builder.build());
                }
            }


            AdpInteractionType.Builder intercationBuilder = null;   
            switch (builder.getEventCode()){
                case MIDAS_IMPRESSION:
                    intercationBuilder = builder.getImpressionBuilder();
                    break;
                case MIDAS_CLICK:
                    intercationBuilder = builder.getClickBuilder();
                    break;
                case MIDAS_REWARDED:
                    intercationBuilder = builder.getRewardBuilder();
                    break;
            }
            if(intercationBuilder != null){
                long ts = builder.getTime().getTimestamp();
                if(!intercationBuilder.getTimestampList().contains(ts)){
                    intercationBuilder.addIp(0);
                    intercationBuilder.addTimestamp(ts);
                }
     
            }
            builder = updateEventDate(builder);
            builder = updateMergeCount(builder);
            AdxAdposEvents mergedEvent =  builder.build();

            byte[]  value = mergedEvent.toByteArray();
            rocksDB.put(keyBytes,value);
            switch (builder.getEventCode()){
                case MIDAS_APP_OFFER:
                case MIDAS_IMPRESSION:
                case MIDAS_CLICK:
                case MIDAS_REWARDED:
                    mergedEvent.writeDelimitedTo(fos);
                    break;
            }
           

            if(oldSourceAdxAdposEvents != null){
               oldSourceAdxAdposEvents = oldSourceAdxAdposEvents.clearImpression();
               oldSourceAdxAdposEvents = oldSourceAdxAdposEvents.clearClick();
               oldSourceAdxAdposEvents = oldSourceAdxAdposEvents.clearReward();
               oldSourceAdxAdposEvents = oldSourceAdxAdposEvents.mergeFrom(builder.build());
               rocksDB.put(sourceKeyBytes,oldSourceAdxAdposEvents.build().toByteArray());
           }
        } catch (RocksDBException | IOException e) {
            e.printStackTrace();
        }
    }

    public void processRow( byte[] bs,FileOutputStream fos){
        try{
            AdxAdposEvents.Builder builder = AdxAdposEvents.parseFrom(bs).toBuilder();
            AdxEventCode eventCode = builder.getEventCode();
            switch(eventCode){
                case MIDAS_APP_REQUEST:
                case MIDAS_APP_OFFER:
                case MIDAS_IMPRESSION:
                case MIDAS_CLICK:
                case MIDAS_CLOSE:
                case MIDAS_REWARDED:
                    processAdposEvent(builder,fos);
                    break;
                case MIDAS_CONFIG_REQUEST:
                case MIDAS_CONFIG_OFFER:
                case MIDAS_SOURCE_REQUEST:
                case MIDAS_SOURCE_OFFER:
                case MIDAS_AD_OFFER:
                    processSourceEvent(builder,fos);
                    break;
                default:
                    break;
            }
        }
        catch(InvalidProtocolBufferException e){
            e.printStackTrace();
        }


    }


    public byte[] multiProcess(List<ByteBuffer> logs,FileOutputStream fos) {
        byte[] ret = null;

        for (ByteBuffer log : logs) {
            byte[] bs = BytesUtils.toBytes(log);
            //AdxAdposEvents.Builder builder = AdxAdposEvents.parseFrom(bs).toBuilder();
            processRow(bs,fos);
        }

        return ret;

    }




    @Override
    public void batchEvent(java.lang.String topic, List<ByteBuffer> logs) throws TException {
        Summary.Timer requestTimer = requestLatency.startTimer();
        long ts = System.currentTimeMillis();
        String timestamp = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(ts);
        System.out.println(String.format( "[%s] recv len:%d",timestamp,logs.size()));
        FileOutputStream fos = null;
        try {
           
            Properties properties =  PropertiesUtils.getProperties();
            String baseDir = properties.getProperty("output_dir","data" + File.separator +"EventMergeLogs");
            File baseDirFile = new File(baseDir);
            String filePath = String.format("%s" +File.separator + "%s.bin", baseDir,timestamp);

            if(logs != null && logs.size() > 0)
            {
                //System.out.println(String.format( "will write:%s len:%d",filePath,logs.size()));
                File file = new File(filePath);
                if (!file.exists()) {
                    //System.out.println("filePath = " + filePath);
                    baseDirFile.mkdirs();
                    file.createNewFile();
                }
                fos = new FileOutputStream(file);
                multiProcess(logs,fos);
                if (fos != null) {
                    fos.close();
                }
            }

        } catch (Exception e) {
            System.out.println("process batch error: "+ e.getMessage());
            e.printStackTrace();
        }
        finally {
            requestTimer.observeDuration();
          }
    }

    @Override
    public ByteBuffer singleEvent(ByteBuffer log) throws TException {
        Summary.Timer requestTimer = requestLatency.startTimer();
        ByteBuffer ret;
        try{
            byte[] retBytes =process(log,null);
            ret = ByteBuffer.wrap( retBytes);
        }
        finally{
            requestTimer.observeDuration();
        }
        return ret;
    }

    @Override
    public ByteBuffer getEvent(String requestId) throws TException {
        return null;
    }
}
