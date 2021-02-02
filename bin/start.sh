base_path=$(cd `dirname $0`; cd ..; pwd)
cd $base_path
pkill AdposEventsMergerRocksdb -f
sleep 1
nohup java  -DApp=AdposEventsMergerRocksdb -cp target/lib:target/AdposEventsMerger-1.0-SNAPSHOT.jar com.xiaoniuhy.adx.thrift.EventMergeThriftMain >logs/rocksdb/EventMerger.log &