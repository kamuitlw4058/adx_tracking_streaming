base_path=$(cd `dirname $0`; cd ..; pwd)
cd $base_path
app="AdposEventMergerStreaming"
pid=`ps -ef | grep HiveServer2 | grep -v grep | awk '{print $2}'`
if  [ ! -n "$pid" ] ;then
    echo "服务进程失败重启进程"
else
    echo "服务运行正常"
fi