''' Download and process snow course and snow pillow data from CDEC

Usage:
    python process_cdec_snow.py
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
    retrieve_cdec('snow_pillow', 'SWE', 82, 'daily')
    retrieve_cdec('snow_course', 'SWE', 3, 'monthly')

def retrieve_cdec(netname, vname, vnum, freq):
    
    cdec_dir  = f'{config["base_dir"]}/obs/cdec'

    date_0 = datetime(1979, 1, 1)
    
    os.chdir(cdec_dir)
    site_list = pd.read_csv(f'site_list_{netname}.csv')

    yesterday = datetime.today() - timedelta(days=1)
    lastmonth = datetime.today() - relativedelta(months=1)

    if freq=='daily':
        idx = pd.date_range(f'{date_0:%Y-%m-%d}', f'{yesterday:%Y-%m-%d}', freq='D')
        dcode = 'D'
    else:
        idx = pd.date_range(f'{date_0:%Y-%m-%d}', f'{lastmonth:%Y-%m}-01', freq='MS')
        dcode = 'M'

    query = f'https://cdec.water.ca.gov/dynamicapp/req/CSVDataServlet?Start={date_0:%Y-%m-%d}&End={yesterday:%Y-%m-%d}'

    sn = vnum
    for site in site_list['STA']:

        data = pd.read_csv(f'{query}&dur_code={dcode}&SensorNums={sn:d}&Stations={site}', usecols=['DATE TIME', 'VALUE'], index_col=['DATE TIME'], parse_dates=['DATE TIME'])
        data.columns=[vname]; data.index.names=['Date']
        data2 = pd.DataFrame(pd.to_numeric(data[vname], errors='coerce').reindex(idx, fill_value=pd.NA)).fillna(pd.NA)
        data2.index.names=['Date']
                    
        data2.to_csv(f'{netname}/{vname}_{freq}_{site}.csv', na_rep='NaN', float_format='%g')


if __name__ == '__main__':
    main(sys.argv[1:])
