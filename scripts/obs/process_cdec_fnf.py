''' Download and process Full Natural Flow from CDEC

Usage:
    python process_cdec_fnf.py [domain]
Default values:
    [domain]: "cnrfc"
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import os, sys

from datetime import datetime, timedelta, timezone
import pytz
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time


## main function
def main(argv):
    
    '''main loop'''
    
    if len(argv)>=1:
        domain = argv[0]
    else:
        domain = 'cnrfc'
    
    cdec_dir  = f'{config["base_dir"]}/wrf_hydro/{domain}/obs/cdec'
    site_list = pd.read_csv(f'{config["base_dir"]}/wrf_hydro/{domain}/b-120/site_list_25.csv')

    date_0 = datetime(1979, 1, 1)

    query = f'https://cdec.water.ca.gov/dynamicapp/req/CSVDataServlet?SensorNums=8&dur_code=D&Start={date_0:%Y-%m-%d}'
    
    os.chdir(cdec_dir)

    yesterday = datetime.today() - timedelta(days=1)

    idx = pd.date_range(f'{date_0:%Y-%m-%d}', f'{yesterday:%Y-%m-%d}')

    for site in site_list['name']:

        data = pd.read_csv(f'{query}&Stations={site}&&End={yesterday:%Y-%m-%d}', usecols=['DATE TIME', 'VALUE'], index_col=['DATE TIME'], parse_dates=['DATE TIME'])
        data.columns=['Flow']; data.index.names=['Date']
        data2 = pd.DataFrame(pd.to_numeric(data['Flow'], errors='coerce').reindex(idx, fill_value=pd.NA)).fillna(pd.NA)
        data2.index.names=['Date']
        data2.to_csv(f'fnf/FNF_daily_{site}.csv', na_rep='NaN')
        
if __name__ == '__main__':
    main(sys.argv[1:])

