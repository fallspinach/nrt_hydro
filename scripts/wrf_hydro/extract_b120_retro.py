''' Extract flows from WRF-Hydro retro simulation for all B-120 sites

Usage:
    python extract_b120_retro.py [domain] [yyyy1] [yyyy2]
Default values:
    must specify all
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

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

## main function
def main(argv):

    '''main loop'''

    domain = argv[0]

    t1 = datetime.strptime(argv[1], '%Y')
    t2 = datetime.strptime(argv[2], '%Y')
    
    workdir = f'{config["base_dir"]}/wrf_hydro/{domain}/retro/output'
    os.chdir(workdir)
    
    kafperday = 86400/1233.48/1000
    
    site_list = pd.read_csv(f'{config["base_dir"]}/wrf_hydro/{domain}/b-120/site_list_25.csv')
    nsites = site_list.shape[0]

    fnin = f'1km_monthly/{t2:%Y}.CHRTOUT_DOMAIN1.monthly'
    fin  = nc.Dataset(fnin, 'r')
    nt  = fin['time'].size
    fin.close()
    
    ntimes = (t2.year-t1.year)*12 + nt
    
    data = np.zeros((nsites, ntimes))
    tstamps = []
    mcnt = 0
    t = t1
    while t<=t2:
        
        fnin = f'1km_monthly/{t:%Y}.CHRTOUT_DOMAIN1.monthly'
        fin  = nc.Dataset(fnin, 'r')
        print(f'Year {t:%Y}')

        nt = fin['time'].size
        
        for i,row in zip(site_list.index, site_list['row']):
            data[i, mcnt:mcnt+nt] = fin['streamflow'][:, row]
            if t==t1:
                print(f'{site_list["name"][i]} {site_list["id"][i]} {fin["feature_id"][row]}')
            
        tstamps.extend([nc.num2date(fin['time'][i], fin['time'].units).strftime('%Y-%m-16') for i in range(nt)])
        fin.close()

        mcnt += 12
        t += relativedelta(years=1)
    
    data *= kafperday
    for m in range(ntimes):
        mds = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        month = int(tstamps[m].split('-')[1])
        year  = int(tstamps[m].split('-')[0])
        if year%4==0 and month==2:
            md = 29
        else:
            md = mds[month-1]
        data[:, m] *= md

    for i,name in zip(site_list.index, site_list['name']):

        if name=='TRF2':
            continue
        df = pd.DataFrame({'Date': tstamps})
        df.set_index('Date', inplace=True, drop=True)
        if name=='TRF1':
            df['Qsim'] = np.squeeze(data[i, :]) - np.squeeze(data[i+1, :])
        else:
            df[f'Qsim'] = np.squeeze(data[i, :])
        if name=='TRF1':
            name = 'TRF'
        
        os.system(f'mkdir -p basins/{t1:%Y}-{t2:%Y}/simulated')
        fnout = f'basins/{t1:%Y}-{t2:%Y}/simulated/{name}.csv'
        df.to_csv(fnout, index=True, float_format='%.3f', date_format='%Y-%m-%d')
        
    return 0


if __name__ == '__main__':
    main(sys.argv[1:])

        
