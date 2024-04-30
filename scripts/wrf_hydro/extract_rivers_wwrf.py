''' Extract flows from West-WRF driven WRF-Hydro forecast for all river reaches in the domain

Usage:
    python extract_rivers_wwrf.py [domain] [fcst_start] [fcst_end] [ens1] [ens2]
Default values:
    [domain]: "cnrfc"
    [fcst_start]: latest West-WRF initialization
    [fcst_end]: last West-WRF full-day forecast (deterministic at the moment)
    [ens1]: 41 (fixed at the moment)
    [ens2]: 41 (fixed at the moment)
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
    
    fcst_type = 'wwrf'
    din  = f'{config["base_dir"]}/wrf_hydro/{domain}/fcst/{fcst_type}/output'

    if len(argv)>=5:
        t1 = datetime.strptime(argv[1], '%Y%m%d')
        t2 = datetime.strptime(argv[2], '%Y%m%d')
        ens1 = int(argv[3])
        ens2 = int(argv[4])
    else:
        files = glob(din+'/41/*.CHRTOUT_DOMAIN1')
        files.sort()
        lastfcst = os.path.basename(files[-1]).split('.')[0]
        t1 = datetime.strptime(lastfcst.split('-')[0], '%Y%m%d')
        t2 = datetime.strptime(lastfcst.split('-')[1], '%Y%m%d')
        ens1 = 41
        ens2 = 41
    
    freq = 'daily'
    
    dout = din
    
    kafperday = 86400/1233.48/1000
    
    site_list = pd.read_csv(f'{config["base_dir"]}/wrf_hydro/{domain}/domain/rivers/line_numbers_{domain}_order4plus.csv')
    nsites = site_list.shape[0]
        
    for ens in range(ens1, ens2+1)[rank::size]:
        
        print(f'{t1:%Y-%m-%d} {t2:%Y-%m-%d} {fcst_type}')
        fnin = f'{din}/{ens:02d}/{t1:%Y%m%d}-{t2:%Y%m%d}.CHRTOUT_DOMAIN1'
        print(fnin)
        
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

        #fnout = '%s/%02d/CHRTOUT_%s-%s.%s.csv.gz' % (dout, ens, t1.strftime('%Y%m%d'), t2.strftime('%Y%m%d'), freq)
        #df.to_csv(fnout, index=True, float_format='%.3f', date_format='%Y-%m-%d', compression='gzip')
        
        fnout = f'{dout}/{ens:02d}/CHRTOUT_{t1:%Y%m%d}-{t2:%Y%m%d}.{freq}.db'
        conn = sqlite3.connect(fnout)
        df.T.to_sql('streamflow', conn, if_exists='replace')
        conn.close()
        
    return 0

    
if __name__ == '__main__':
    main(sys.argv[1:])

        
