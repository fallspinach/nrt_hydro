''' Download and process streamflow data from USGS

Usage:
    python process_usgs_streamflow.py
Default values:
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Prototype'

import os, sys

from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
import pytz, re
import pandas as pd
import dataretrieval.nwis as nwis
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time


## main function
def main(argv):
    
    '''main loop'''
    
    cdec_dir  = f'{config["base_dir"]}/obs/usgs'

    date_0 = datetime(1979, 1, 1)
    
    os.chdir(cdec_dir)
    site_list = pd.read_csv('feature_gage_row_us_clean.csv', dtype={'gage_id': str})

    yesterday = datetime.today() - timedelta(days=1)
    lastmonth = datetime.today() - relativedelta(months=1)

    idx_d = pd.date_range(f'{date_0:%Y-%m-%d}', f'{yesterday:%Y-%m-%d}', freq='D')
    idx_m = pd.date_range(f'{date_0:%Y-%m-%d}', f'{lastmonth:%Y-%m}-01', freq='MS')

    #for site in site_list['gage_id']:
    for site in ["07344210", "08073500", "08068900", "08168932", "03254693", "06208500", "01011500", "01465500", "01467087", "01574500", "08143500", "05051500", "021473428", "02145000", "04279000", "02196000", "04084445", "06294000", "05536890", "07358280", "09482500", "02479310", "03333700", "12010000", "02306000", "06903900"]:

        df = nwis.get_record(sites=site, service='dv', parameterCd='00060', start=f'{date_0:%Y-%m-%d}', end=f'{yesterday:%Y-%m-%d}')
        df.index = df.index.date
        df.index.name = 'Date'
        df.drop(columns=['site_no'], inplace=True)
        qs = []; ls=[]; rs=[]
        for v in df.columns:
            if re.match(r'^00060_.*Mean$', v):
                qs.append(v); ls.append(df[v].count()); rs.append(df[v].last_valid_index())
        if len(qs)==0:
            print(f'{site}: No 00060_Mean variable found. Skipping.')
        else:
            print(f'{site}')
            for i in zip(qs, ls, rs):
                print(i)
                
            if len(qs)==1:
                sel = qs[0]; reason = 'only choice'
            else:
                if df[qs[0]].last_valid_index() > df[qs[1]].last_valid_index():
                    sel = qs[0]; reason = 'more recent'
                elif df[qs[0]].last_valid_index() == df[qs[1]].last_valid_index():
                    if df[qs[0]].count() > df[qs[1]].count():
                        sel = qs[0]; reason = 'longer'
                    else:
                        sel = qs[1]; reason = 'second one'
                else:
                    sel = qs[1]; reason = 'more recent'
                
            print(f'{sel} is selected because it is the {reason}.')
            df.rename(columns={sel: 'Qobs', f'{sel}_cd': 'Qobs_cd'}, inplace=True)
            df.to_csv(f'streamflow/{site}.csv', float_format='%g')


if __name__ == '__main__':
    main(sys.argv[1:])

