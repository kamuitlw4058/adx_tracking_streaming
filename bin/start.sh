base_path=$(cd `dirname $0`; cd ..; pwd)
cd $base_path
pkill AdposEventsMerger-1.0-SNAPSHOT.jar -f
sleep 1
nohup java   -cp target/lib  -jar target/AdposEventsMerger-1.0-SNAPSHOT.jar >logs/EventMerger.log &