''' Calculate monthly percentile ranks and append to WRF-Hydro output

Usage:
    imported by other scripts most of the time
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, pytz, time
import numpy as np
import netCDF4 as nc
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

## main function
def main(argv):

    '''main loop'''

    domain = argv[0]
    fname = argv[1]
    print(fname)

    if 'LDASOUT' in os.path.basename(fname):
        fpctl = nc.Dataset(f'{config["base_dir"]}/wrf_hydro/{domain}/retro/output/1km_monthly/stat/1979-2023.SMTOT_SWE.monthly.ymonpctl.00-99', 'r')
        vs = ['SOIL_M', 'SNEQV']
    elif 'CHRTOUT' in os.path.basename(fname):
        fpctl = nc.Dataset(f'{config["base_dir"]}/wrf_hydro/{domain}/retro/output/1km_monthly/stat/1979-2023.STREAMFLOW.monthly.ymonpctl.00-99', 'r')
        vs = ['streamflow']
    elif 'LDASIN_DOMAIN1' in os.path.basename(fname):
        fpctl = nc.Dataset(f'{config["base_dir"]}/wrf_hydro/{domain}/retro/forcing/1km_monthly/stat/1979-2023.RAINRATE_T2D.monthly.ymonpctl.00-99', 'r')
        vs = ['RAINRATE', 'T2D']
    else:
        print('We process either LDASOUT or CHRTOUT or LDASIN files.')
        return 1

    fdata = nc.Dataset(fname, 'a')

    # copy all file data except for the excluded
    for name in vs:
        if name+'_r' in fdata.variables:
            print(f'Rank variable {name}_r already exists. Overwriting.')
            continue
        if name != 'SOIL_M':
            x = fdata.createVariable(name+'_r', 'f4', fdata[name].dimensions, zlib=True)
        else:
            x = fdata.createVariable(name+'_r', 'f4', ('time', 'y', 'x',), zlib=True)
        fdata[name+'_r'].long_name = f'Percentile rank of {fdata[name].long_name}'
        fdata[name+'_r'].units = 'percentile'

    for t in range(fdata['time'].size):
        dtime = nc.num2date(fdata['time'][t], fdata['time'].units)
        for v in vs:
            if v != 'SOIL_M':
                fdata[v+'_r'][t] = (fdata[v][t]>fpctl[v][dtime.month-1]).sum(axis=0).astype(float)
            else:
                fdata[v+'_r'][t] = ((fdata[v][t, :, 0, :]*0.05+fdata[v][t, :, 1, :]*0.15+fdata[v][t, :, 2, :]*0.3+fdata[v][t, :, 3, :]*0.5)>fpctl[v][dtime.month-1]).sum(axis=0).astype(float)
            fdata.sync()

    fpctl.close()
    fdata.close()

    return 0

if __name__ == '__main__':
    main(sys.argv[1:])

