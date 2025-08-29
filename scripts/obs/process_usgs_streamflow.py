''' Download and process streamflow data from USGS

Usage:
    python process_usgs_streamflow.py
Default values:
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Prototype'

import os, sys, time

from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
import pytz, re
import pandas as pd
import dataretrieval.nwis as nwis
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

def run_with_retries(func, retries=5, wait=10, *args, **kwargs):
    """
    Run a function with retries.
    
    Parameters:
        func    : callable   → the function to run
        retries : int        → how many total attempts
        wait    : int/float  → seconds to wait between attempts
        *args, **kwargs      → arguments to pass to func
    """
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"Attempt {attempt} failed: {e}")
            if attempt < retries:
                print(f"Retrying in {wait} seconds...")
                time.sleep(wait)
            else:
                print("All retries failed.")
                raise

## main function
def main(argv):
    
    '''main loop'''
    
    yesterday = datetime.today() - timedelta(days=1)
    lastmonth = datetime.today() - relativedelta(months=1)

    if len(argv)>0:
        if argv[0]=='retro':
            date0 = datetime(1979, 1, 1)
        else:
            date0 = datetime.strptime(argv[0], '%Y%m%d')
    else:
        date0 = lastmonth
    
    cdec_dir  = f'{config["base_dir"]}/obs/usgs'
    
    os.chdir(cdec_dir)
    site_list = pd.read_csv('feature_gage_row_us_clean.csv', dtype={'gage_id': str})

    sites = site_list['gage_id'].to_list()
    #sites = ["07344210", "08073500", "08068900", "08168932", "03254693", "06208500", "01011500", "01465500", "01467087", "01574500", "08143500", "05051500", "021473428", "02145000", "04279000", "02196000", "04084445", "06294000", "05536890", "07358280", "09482500", "02479310", "03333700", "12010000", "02306000", "06903900"]
    #sites = ["07344210"]

    var_sels = pd.read_csv('variable_selections.csv', index_col='gage_id', dtype={'gage_id': str, 'v': str})

    for s,site in enumerate(sites):

        fout = f'streamflow/{site}.csv'
        #if s%10==0:
        print(f'Retrieving {s+1}th site {site}.')
        time.sleep(1)
        
        df = run_with_retries(nwis.get_record, retries=5, wait=30, sites=site, service='dv', parameterCd='00060', start=f'{date0:%Y-%m-%d}', end=f'{yesterday:%Y-%m-%d}')
        df.index = df.index.date
        df.index.name = 'Date'
        df.drop(columns=['site_no'], inplace=True)
        qs = []; ls=[]; rs=[]
        for v in df.columns:
            if re.match(r'^00060_.*Mean$', v):
                qs.append(v); ls.append(df[v].count()); rs.append(df[v].last_valid_index())

        if date0 == datetime(1979, 1, 1):
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
                df.to_csv(fout, float_format='%g')
                
        else:
            if len(qs)==0:
                continue
            if len(qs)==1:
                varname = qs[0]
            else:
                print(f'{site}: {qs}')
                varname = var_sels.loc[site, 'v']
            df0 = pd.read_csv(fout, parse_dates=True, index_col='Date')
            df0.index = df0.index.date
            df.rename(columns={varname: 'Qobs', f'{varname}_cd': 'Qobs_cd'}, inplace=True)
            df1 = pd.concat([df0, df])
            df1 = df1.loc[~df1.index.duplicated(keep='last')]
            df1.index.name = 'Date'
            df1.to_csv(fout, float_format='%g')


if __name__ == '__main__':
    main(sys.argv[1:])

