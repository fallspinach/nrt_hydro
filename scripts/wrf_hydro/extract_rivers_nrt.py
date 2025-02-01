''' Extract flows from NRT WRF-Hydro output for all river reaches in the domain

Usage:
    python extract_rivers_nrt.py [domain] [yyyymm1] [yyyymm2]
Default values:
    [domain]: "cnrfc"
    [yyyymm1]: Oct 1 (start of current water year) if before Oct or Jan 1 if on/after Oct 1
    [yyyymm2]: latest WRF-Hydro NRT date
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

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

    if len(argv)>=1:
        domain = argv[0]
    else:
        domain = 'cnrfc'

    if len(argv)>=3:
        t1 = datetime.strptime(argv[1], '%Y%m')
        t2 = datetime.strptime(argv[2], '%Y%m')
    else:
        curr_time = datetime.utcnow()
        curr_time = curr_time.replace(tzinfo=pytz.utc)
        curr_day  = curr_time - timedelta(hours=curr_time.hour, minutes=curr_time.minute, seconds=curr_time.second, microseconds=curr_time.microsecond)
        t2 = curr_day - timedelta(days=1)
        
        t2  = find_last_time(f'{config["base_dir"]}/wrf_hydro/{domain}/nrt/output/1km_daily/202?????00.CHRTOUT_DOMAIN1', '%Y%m%d%H.CHRTOUT_DOMAIN1') - timedelta(hours=1)
        if t2.month>=10:
            t1 = datetime(t2.year, 1, 1, tzinfo=pytz.utc)
        else:
            t1 = datetime(t2.year-1, 10, 1, tzinfo=pytz.utc)

    freq = 'daily'
    
    din  = f'{config["base_dir"]}/wrf_hydro/{domain}/nrt/output/1km_{freq}'
    dout = f'{config["base_dir"]}/wrf_hydro/{domain}/nrt/output/rivers'
    if not os.path.isdir(dout):
        os.system(f'mkdir -p {dout}')
    
    kafperday = 86400/1233.48/1000
    
    site_list = pd.read_csv(f'{config["base_dir"]}/wrf_hydro/{domain}/domain/rivers/line_numbers_{domain}_order4plus.csv')
    nsites = site_list.shape[0]
    
    cmd = 'cdo -O -f nc4 -z zip mergetime'
    step = relativedelta(months=1)
    alltimes=[]
    t = t1
    while t <= t2:
        if freq=='daily':
            fnin = f'{din}/{t:%Y%m}.CHRTOUT_DOMAIN1'
        else:
            fnin = f'{din}/{t:%Y%m}.CHRTOUT_DOMAIN1.{freq}'
        cmd += ' ' + fnin
        alltimes.append(t)
        t += step
        
    if freq=='daily':
        fnout = f'{din}/{t1:%Y%m}-{t2:%Y%m}.CHRTOUT_DOMAIN1'
    else:
        fnout = f'{din}/{t1:%Y%m}-{t2:%Y%m}.CHRTOUT_DOMAIN1.{freq}'
    cmd += ' ' + fnout
    print(cmd); os.system(cmd)
    
    #for t in alltimes[rank::size]:
    if True:
        
        print(f'{t1:%Y-%m} {t2:%Y-%m}')
        fnin = fnout
        
        if os.path.isfile(fnin):
            fin  = nc.Dataset(fnin, 'r')
            nt = fin['time'].size

            data_all = fin['streamflow'][:, :round(site_list['row'].max())+1]
            data = data_all[:, site_list['row'].to_numpy().astype(np.int32)].transpose()
            if freq=='daily':
                tstamps = [nc.num2date(fin['time'][0]+1440*i, fin['time'].units).strftime('%Y-%m-%d') for i in range(nt)]
            else:
                tstamps = [nc.num2date(fin['time'][i], fin['time'].units).strftime('%Y-%m-%d') for i in range(nt)]
            
            fin.close()
                
        #data *= kafperday

        df = pd.DataFrame({'Date': tstamps})
        df.set_index('Date', inplace=True, drop=True)
        
        for i,name in zip(site_list.index, site_list['feature_id']):
            df[name] = np.squeeze(data[i, :])

        #fnout = '%s/CHRTOUT_%s-%s.%s.csv.gz' % (dout, t1.strftime('%Y%m'), t2.strftime('%Y%m'), freq)
        #df.to_csv(fnout, index=True, float_format='%.3f', date_format='%Y-%m-%d', compression='gzip')
        
        fnout = f'{dout}/CHRTOUT_{t1:%Y%m}-{t2:%Y%m}.{freq}.db'
        conn = sqlite3.connect(fnout)
        df.T.to_sql('streamflow', conn, if_exists='replace')
        conn.close()

        #df.reset_index(inplace=True, drop=False)
        df.T.to_csv(fnout.replace('db', 't.csv.gz'), header=True, index=True, float_format='%.3f', date_format='%Y-%m-%d', compression='gzip')
        
    return 0

    
if __name__ == '__main__':
    main(sys.argv[1:])

        
