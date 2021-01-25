package com.xiaoniuhy.adp.rocks;

import java.util.Properties;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.TtlDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.io.File;

import com.xiaoniuhy.utils.PropertiesUtils;

public class EventMergeRocks {
    static RocksDB rocksDB;
    static String path;
    static{
        RocksDB.loadLibrary();
        Properties properties = PropertiesUtils.getProperties();
        path = properties.getProperty("rocks_dir","data/EventMergeRocks");
    }
    public  static  void init() throws RocksDBException {
        init(60 * 30);
    }

    public  static  void init(int ttl) throws RocksDBException {
        Options options = new Options();
        options.setCreateIfMissing(true);
        File file = new File(path);
        if (!file.exists()) {
            file.mkdirs();
        }
        rocksDB = TtlDB.open(options, path,ttl, false);
    }

    public  static RocksDB getRocks(){
        return rocksDB;
    }


    public static void main(String[] args) throws Exception {
        init();


        byte[] key = "Hello".getBytes();
        byte[] value = "World".getBytes();
        rocksDB.put(key, value);

        byte[] getValue = rocksDB.get(key);
        System.out.println(new String(getValue));

        /**
         * 通过List做主键查询
         */
        rocksDB.put("SecondKey".getBytes(), "SecondValue".getBytes());

        List<byte[]> keys = new ArrayList<>();
        keys.add(key);
        keys.add("SecondKey".getBytes());

        Map<byte[], byte[]> valueMap = rocksDB.multiGet(keys);
        for (Map.Entry<byte[], byte[]> entry : valueMap.entrySet()) {
            System.out.println(new String(entry.getKey()) + ":" + new String(entry.getValue()));
        }

        /**
         *  打印全部[key - value]
         */
        RocksIterator iter = rocksDB.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println("iter key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
        }



    }
}