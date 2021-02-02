base_path=$(cd `dirname $0`; cd ..; pwd)
cd $base_path
app="AdposEventMergerStreaming"
cur_time=$(date +%Y%m%d%H%M)
pid=`ps -ef | grep $app | grep -v grep | awk '{print $2}'`
if  [ ! -n "$pid" ] ;then
    echo "服务进程失败重启进程..."
    echo "restart at $cur_time" > logs/spark/restart.log
    cp logs/spark/spark.log logs/spark/spark_${cur_time}.log
    bin/start_streaming.sh
else
    echo "服务运行正常"
fi