''' Extract point time series from WRF-Hydro retrospective simulation for given sites

Usage:
    python extract_points_retro.py [domain] [yyyymm1] [yyyymm2]
Default values:
    must specify all
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Prototype'

import sys, os, pytz, time
import netCDF4 as nc
from glob import glob
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from calendar import monthrange
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

flag_include_forcing = False

## main function
def main(argv):

    '''main loop'''

    domain = argv[0]

    t1 = datetime.strptime(argv[1], '%Y%m')
    t2 = datetime.strptime(argv[2], '%Y%m')
    
    workdir = f'{config["base_dir"]}/wrf_hydro/{domain}/retro/output'
    os.chdir(workdir)
    
    site_list = pd.read_csv(f'{config["base_dir"]}/obs/cdec/site_list_combined_ij.csv')
    nsites = site_list.shape[0]

    # daily output
    fnins = ''
    tmp_out = f'tmp_out_{t1:%Y%m}-{t2:%Y%m}'
    t = t1
    while t<=t2:
        fnins += f' 1km_daily/{t:%Y/%Y%m}.LDASOUT_DOMAIN1'
        t += relativedelta(months=1)
    cmd = f'cdo --sortname -f nc4 -z zip mergetime {fnins} {tmp_out}'
    print(cmd); os.system(cmd)
    
    # daily forcing
    if flag_include_forcing:
        fnins = ''
        tmp_for = f'tmp_for_{t1:%Y%m}-{t2:%Y%m}'
        t = t1
        while t<=t2:
            fnins += f' ../forcing/1km_daily/{t:%Y%m}.LDASIN_DOMAIN1.daily'
            t += relativedelta(months=1)
        cmd = f'cdo --sortname -f nc4 -z zip mergetime {fnins} {tmp_for}'
        print(cmd); os.system(cmd)

    fdata = nc.Dataset(tmp_out, 'r')
    ntimes = fdata['time'].size
    tstamps = [nc.num2date(t, fdata['time'].units).strftime('%Y-%m-%d') for t in fdata['time'][:]]

    data_site = np.zeros((nsites, ntimes))

    for t in range(ntimes):
        for k,row in site_list.iterrows():
            i = row['i']
            j = row['j']
            data_site[k, t] = fdata['SNEQV'][t, i, j]
    fdata.close()
    
    dout = f'basins/{t1:%Y%m}-{t2:%Y%m}/sites'
    if not os.path.isdir(dout):
        os.system(f'mkdir -p {dout}')

    for k,row in site_list.iterrows():
        df = pd.DataFrame({'Date': tstamps, 'SWE': np.squeeze(data_site[k, :])})
        fnout = f'{dout}/{row["STA"]}.csv'
        df.to_csv(fnout, index=False, float_format='%.3f', date_format='%Y-%m-%d')

    #os.system(f'rm -f {tmp_out}')
        
    return 0


if __name__ == '__main__':
    main(sys.argv[1:])
