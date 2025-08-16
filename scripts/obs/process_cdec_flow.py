''' Download and process River Flow and Stage from CDEC

Usage:
    python process_cdec_flow.py
Default values:
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Prototype'

import os, sys

from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
import time
import pytz
import pandas as pd
import numpy as np
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time


## main function
def main(argv):
    
    '''main loop'''

    sensors = [{'name': 'Flow', 'no': 20}, {'name': 'Stage', 'no': 1}]
    
    cdec_dir  = f'{config["base_dir"]}/obs/cdec'

    date_0 = datetime(1984, 1, 1, 0)
    
    os.chdir(cdec_dir)
    site_list = pd.read_csv('site_list_flow_available.csv')

    curr_time = datetime.utcnow()
    curr_daystart = curr_time - timedelta(hours=curr_time.hour, minutes=curr_time.minute, seconds=curr_time.second, microseconds=curr_time.microsecond)
    curr_dayend   = curr_daystart + timedelta(hours=23)
    # back_daystart = date_0
    # rewind 4 days
    back_daystart = curr_daystart - timedelta(days=4)

    idx_h = pd.date_range(back_daystart, curr_dayend, freq='H')

    query = f'https://cdec.water.ca.gov/dynamicapp/req/CSVDataServlet?Start={back_daystart:%Y-%m-%dT%H}&End={curr_dayend:%Y-%m-%dT%H}'

    for row in site_list.itertuples():

        site_cdec = row.CDEC_ID
        site_nws  = row.NWS_ID

        message = f'CDEC ID {site_cdec} / NWS ID {site_nws}.'

        cnt = 0
        for sensor in sensors:
            data = pd.read_csv(f'{query}&dur_code=H&SensorNums={sensor["no"]:d}&Stations={site_cdec}', usecols=['DATE TIME', 'VALUE'], index_col=['DATE TIME'], parse_dates=['DATE TIME'])
            message += f' {sensor["name"]}: {data.shape[0]} records'
            data.columns=[sensor['name']]; data.index.names=['Date']
            data2 = pd.DataFrame(pd.to_numeric(data[sensor['name']], errors='coerce').reindex(idx_h, fill_value=pd.NA)).fillna(pd.NA)
            data2.index.names=['Date']
            #data2[sensor['name']] = data2[sensor['name']].multiply(1)

            if cnt==0:
                data_all = data2.copy()
            else:
                data_all[sensor['name']] = data2[sensor['name']]
                
            cnt += 1
        
        print(message)
        fout = f'flow/Flow_{site_nws}.csv'
        df0 = pd.read_csv(fout, parse_dates=True, index_col='Date')
        df1 = pd.concat([df0, data_all])
        df1 = df1.loc[~df1.index.duplicated(keep='last')]
        df1.index.name = 'Date'
        df1.to_csv(fout, na_rep='NaN', float_format='%g')
        time.sleep(2)


if __name__ == '__main__':
    main(sys.argv[1:])

