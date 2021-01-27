base_path=$(cd `dirname $0`; cd ..; pwd)
cd $base_path
 java   -cp target/lib:target/AdposEventsMerger-1.0-SNAPSHOT.jar com.xiaoniuhy.adx.thrift.EventMergeClient 