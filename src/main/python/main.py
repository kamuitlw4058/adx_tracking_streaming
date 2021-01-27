import os
from datetime import datetime,timedelta
import time 

from python_common.utils.shell_utils import run_cli

base_path = '../java'
datapath= f'{base_path}/data/'
last_flag = 'last'
module ='EventMergeLogs'
module_data_dir = os.path.join(datapath,module)
datalist  =os.listdir(module_data_dir)
module_last_file = os.path.join(datapath,f'{module}.{last_flag}')

pb_path = f'/home/wls81/workspace/xuyang/adp_common/src/main/proto/clickhouse.proto'

pb_class = 'AdpTrackingLogEvent'


def string_toDatetime(string):
    return datetime.strptime(string, "%Y%m%d%H%M%S")

formated_datalist = [string_toDatetime(i[:14]).timestamp() for i in datalist]


last_ts = 0
final_ts = 0
if os.path.exists(module_last_file):
    with open(module_last_file,'r') as f:
        try:
            d = f.read()
            last_ts =  float(d)
        except Exception  as e:
            pass
print('start to write pb data...')

for f,ts,filepath in zip(datalist,formated_datalist,datalist):
    if ts > last_ts:
        print(f'start write:{f}')
        cmd_format = f"""cat {os.path.join(module_data_dir,f)} | clickhouse-client -h  cc-uf6tj4rjbu5ez10lb.ads.aliyuncs.com -u chaixiaohui  --password AAAaaa111! --port 3306 --query "INSERT INTO xn_adp.imp_all FORMAT Protobuf SETTINGS format_schema='{pb_path}:{pb_class}'" """
        r =  run_cli(cmd_format)
        if ts > final_ts:
            final_ts = ts
    if ts < (datetime.now() - timedelta(minutes=30)).timestamp():
        os.remove(os.path.join(module_data_dir,filepath))
        print(f' expire ts:{ts} filepath:{filepath}')



if final_ts != 0:
    with open(module_last_file,'w') as fp:
        write_ts = str(final_ts)
        print(f'write ts:{write_ts}')
        fp.write(write_ts)


print(f'end write pb data. final_ts:{final_ts}')
