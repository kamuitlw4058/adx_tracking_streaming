from datetime import datetime,timedelta

from python_common.utils.datetime_utils import  str_to_datetime
from python_common.io.scan_datafiles import ScanDataFiles,true_filter,false_filter

from datetime import datetime,timedelta


def file_filter(filepath,filename,dir_list,extend_params):
    try:
        if filename.endswith('bin'):
            return True
    except Exception as e:
        print(e)

    return False    

def delete_file_filter(filepath,filename,dir_list,extend_params):
    if filename.endswith('.bin'):
        try:
            file_create_dt =  datetime.strptime(str(filename.split('-')[0]), '%Y%m%d%H%M%S')
            delete_dt = datetime.now() - timedelta(minutes=30)
            if file_create_dt < delete_dt:
                print(f'will delete  file filter:{filepath}')
        except Exception as e:
            print(f'process delete failed!{e}')
    return False


def process_func(data_file,extend_params,data_file_logger):
    data_file_logger.info(f'data_file:{data_file}, extend_params:{extend_params}')
    return True


base_path = '/home/wls81/workspace/kimi/adx_tracking_streaming/data/EventMergeLogs'

s = ScanDataFiles('midas2_pb',base_path,file_filters=file_filter,delete_file_filters=delete_file_filter,dir_filters=false_filter)
s.scan(process_func)
s.process_files()
