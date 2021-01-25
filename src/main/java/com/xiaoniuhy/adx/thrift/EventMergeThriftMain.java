package com.xiaoniuhy.adp.thrift;
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

import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.JSON;
import com.xiaoniuhy.adx.model.midas2.MidasTrackModel;
import com.xiaoniuhy.adx.model.midas2.MidasEventTrack;
import com.xiaoniuhy.adx.model.midas2.MidasEventTrackParser;

public class EventMergeThriftMain {
    public void startServer(){
        try {
            Properties properties = PropertiesUtils.getProperties();
            int port = Integer.parseInt( properties.getProperty("port","18989"));

            System.out.println("init fastjson start ...");
            ParserConfig.getGlobalInstance().putDeserializer(MidasEventTrack.class, new MidasEventTrackParser());

            System.out.println(String.format("Prometheus start at %d ...", port +1));
            HTTPServer httpServer = new HTTPServer(port+1);
            System.out.println("Rocks start ... ");
            EventMergeRocks.init();
            RocksDB rocksDB = EventMergeRocks.getRocks();
            System.out.printf("EventMergeRocks start at %d... %n",port);
            TServerSocket serverTransport = new TServerSocket(port);
            TServer.Args tArgs = new TServer.Args(serverTransport);
            TProcessor tProcessor = new EventMergeService.Processor<EventMergeService.Iface>(new EventMergeServiceImpl(rocksDB));
            tArgs.processor(tProcessor);
            tArgs.protocolFactory(new TBinaryProtocol.Factory());
            TServer server = new TSimpleServer(tArgs);

           

            server.serve();
        } catch (TTransportException | RocksDBException  | IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args){
        EventMergeThriftMain serviceServer = new EventMergeThriftMain();
        serviceServer.startServer();
    }
}
