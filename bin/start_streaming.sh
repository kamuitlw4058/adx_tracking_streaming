base_path=$(cd `dirname $0`; cd ..; pwd)
cd $base_path
export SPARK_CLASSPATH=$base_path/lib:$SPARK_CLASSPATH
datetime=`date '+%Y%m%d%H%M%S'`
nohup /opt/spark-2.4.5-hadoop3.1-1.0.2/bin/spark-submit \
                --driver-java-options '-DApp=AdposEventMergerStreaming' \
                --jars /opt/spark_jars/spark-streaming-kafka-0-10_2.11-2.4.5.jar,/opt/spark_jars/protobuf-java-3.8.0.jar,/opt/spark_jars/kafka-clients-0.10.2.2.jar,/opt/spark_jars/adp_common-1.0.jar \
                target/AdposEventsMerger-1.0-SNAPSHOT.jar > logs/spark.log & 
 
                


