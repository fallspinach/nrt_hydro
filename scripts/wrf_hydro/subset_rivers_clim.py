import sys, os, pytz, time
import netCDF4 as nc
from glob import glob
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

from mpi4py import MPI
import sqlite3

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

## main function
def main(argv):

    '''main loop'''

    domain = argv[0]
    
    t1 = datetime.strptime(argv[1], '%Y%m')
    t2 = datetime.strptime(argv[2], '%Y%m')
    t2 = t2 + relativedelta(months=1) - timedelta(days=1)
    freq = 'daily'
    
    if t2-t1>timedelta(days=366) or t2-t1<timedelta(days=1):
        print('Request longer than a year or dates switched.')
        sys.exit(0)
    
    y1 = 1979
    y2 = 2023
    
    pctls = [5, 10, 20, 50, 80, 90, 95]
    #pctls = [80, 90, 95]
    
    din = f'{config["base_dir"]}/wrf_hydro/{domain}/retro/output/rivers'

    dout = f'{config["base_dir"]}/wrf_hydro/{domain}/nrt/output/rivers'
    if not os.path.isdir(dout):
        os.system(f'mkdir -p {dout}')
    
    kafperday = 86400/1233.48/1000
    
    site_list = pd.read_csv(f'{config["base_dir"]}/wrf_hydro/{domain}/domain/rivers/line_numbers_{domain}_order4plus.csv')
    nsites = site_list.shape[0]
    
    for pctl in pctls[rank::size]:
        
        print(f'Subsetting/matching {pctl} percentile.')
        
        fin   = f'{din}/CHRTOUT_{y1}-{y2}.{freq}.pctl{pctl:02d}.csv.gz'
        fnout = f'{dout}/CHRTOUT_{t1:%Y%m}-{t2:%Y%m}.{freq}.pctl{pctl:02d}.csv.gz'
        
        df = pd.read_csv(fin)
        
        if t2.year%4!=0 or (t2.year%4==0 and t2.month<2):
            leapday = df[(df['Date']=='2020-02-29')].index
            df.drop(leapday, inplace=True)
        
        df['Date'] = df['Date'].apply(lambda x: x.replace('2020', t1.strftime('%Y')))
        
        if t1.year==t2.year:
            badrange = df[(df['Date']<t1.strftime('%Y-%m-%d')) | (df['Date']>t2.strftime('%Y-%m-%d'))].index
        else:
            badrange = df[(df['Date']>t1.strftime('%Y')+t2.strftime('-%m-%d')) & (df['Date']<t1.strftime('%Y-%m-%d'))].index
            
        df.drop(badrange, inplace=True)
        
        if t1.year!=t2.year:
            df['Date'] = df['Date'].apply(lambda x: x if x>=t1.strftime('%Y-%m-%d') else x.replace(t1.strftime('%Y'), t2.strftime('%Y')))
            
        df = df.sort_values('Date')
        df.drop_duplicates(subset=['Date'], inplace=True)
        
        df.to_csv(fnout, index=False, float_format='%.3f', date_format='%Y-%m-%d', compression='gzip')
        df = pd.read_csv(fnout, index_col='Date')
        conn = sqlite3.connect(fnout.replace('csv.gz', 'db'))
        df.T.to_sql('streamflow', conn, if_exists='replace')
        conn.close()
        
    return 0

    
if __name__ == '__main__':
    main(sys.argv[1:])

        
