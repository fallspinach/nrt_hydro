''' Download and process Full Natural Flow from CDEC

Usage:
    python process_cdec_fnf.py
Default values:
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Prototype'

import os, sys

from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
import pytz
import pandas as pd
import numpy as np
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time


## main function
def main(argv):
    
    '''main loop'''
    
    cdec_dir  = f'{config["base_dir"]}/obs/cdec'

    date_0 = datetime(1979, 1, 1)
    
    os.chdir(cdec_dir)
    site_list = pd.read_csv('site_list_fnf.csv')

    yesterday = datetime.today() - timedelta(days=1)
    lastmonth = datetime.today() - relativedelta(months=1)

    idx_d = pd.date_range(f'{date_0:%Y-%m-%d}', f'{yesterday:%Y-%m-%d}', freq='D')
    idx_m = pd.date_range(f'{date_0:%Y-%m-%d}', f'{lastmonth:%Y-%m}-01', freq='MS')

    query = f'https://cdec.water.ca.gov/dynamicapp/req/CSVDataServlet?Start={date_0:%Y-%m-%d}&End={yesterday:%Y-%m-%d}'

    for site in site_list['name']:

        if site=='TRF1':
            site = 'TRF'
        if site=='TRF2':
            continue
            
        if site=='SJF':
            site1 = 'SBF'
        else:
            site1 = site

        # daily FNF
        if site=='AMF':
            sn = 290
        else:
            sn = 8
        data = pd.read_csv(f'{query}&dur_code=D&SensorNums={sn:d}&Stations={site1}', usecols=['DATE TIME', 'VALUE'], index_col=['DATE TIME'], parse_dates=['DATE TIME'])
        data.columns=['Flow']; data.index.names=['Date']
        data2 = pd.DataFrame(pd.to_numeric(data['Flow'], errors='coerce').reindex(idx_d, fill_value=pd.NA)).fillna(pd.NA)
        data2.index.names=['Date']
        
        if site=='AMF':
            # sensor 290 provides cumulative values from Oct 1 on
            data2.rename(columns={'Flow': 'CumFlow'}, inplace=True)
            data2['Flow'] = data2['CumFlow'].diff()
            data2.loc[data2.index.strftime('%m-%d')=='10-01', 'Flow'] = data2['CumFlow']
            data2.drop(columns=['CumFlow'], inplace=True)
            # AF to KAF
            data2['Flow'] = data2['Flow'].multiply(0.001)
        else:
            # CFS to KAF per day
            data2['Flow'] = data2['Flow'].multiply(1.983459/1000)
            
        data2.to_csv(f'fnf/FNF_daily_{site}.csv', na_rep='NaN', float_format='%g')

        # monthly sum derived from daily, just in case the official monthly is not out in time
        data3 = data2.resample('1M').agg(pd.Series.sum, min_count=25)
        data3.to_csv(f'fnf/FNF_derived_monthly_{site}.csv', na_rep='NaN', float_format='%g')

        # monthly FNF
        sn = 65
        data = pd.read_csv(f'{query}&dur_code=M&SensorNums={sn:d}&Stations={site1}', usecols=['DATE TIME', 'VALUE'], index_col=['DATE TIME'], parse_dates=['DATE TIME'])
        data.columns=['Flow']; data.index.names=['Date']
        data2 = pd.DataFrame(pd.to_numeric(data['Flow'], errors='coerce').reindex(idx_m, fill_value=pd.NA)).fillna(pd.NA)
        data2.index.names=['Date']
        # AF to KAF
        data2['Flow'] = data2['Flow'].multiply(0.001)
        # fill the last record with values derived from daily data if monthly data not released yet
        if data2.index[-1].month==data3.index[-2].month and np.isnan(data2.iloc[-1]['Flow']):
            #data2.iloc[-1]['Flow'] = data3.iloc[-2]['Flow']
            data2.loc[data2.index[-1], 'Flow'] = data3.loc[data3.index[-2], 'Flow']
        data2.to_csv(f'fnf/FNF_monthly_{site}.csv', na_rep='NaN', float_format='%g')


if __name__ == '__main__':
    main(sys.argv[1:])

