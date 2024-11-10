''' Extract basin averaged quantities from WRF-Hydro NRT simulation for all B-120 basins

Usage:
    python extract_average_nrt.py [domain] [yyyymm1] [yyyymm2]
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

## main function
def main(argv):

    '''main loop'''

    domain = argv[0]

    t1 = datetime.strptime(argv[1], '%Y%m')
    t2 = datetime.strptime(argv[2], '%Y%m')
    
    workdir = f'{config["base_dir"]}/wrf_hydro/{domain}/nrt/output'
    os.chdir(workdir)

    cdocmd = 'cdo -s -w -outputtab,date,value -fldmean'
    
    site_list = pd.read_csv(f'{config["base_dir"]}/wrf_hydro/{domain}/b-120/site_list_25.csv')
    site_names = site_list['name'].tolist()
    site_names.remove('TRF1'); site_names.remove('TRF2')
    site_names.append('TRF')

    # daily output
    fnins = ''
    t = t1
    while t<=t2:
        fnins += f' 1km_daily/{t:%Y%m}.LDASOUT_DOMAIN1'
        t += relativedelta(months=1)
    cmd = f'cdo --sortname -f nc4 -z zip mergetime {fnins} tmp_out.nc'
    print(cmd); os.system(cmd)
    
    # daily forcing
    fnins = ''
    t = t1
    while t<=t2:
        fnins += f' ../forcing/1km_daily/{t:%Y%m}.LDASIN_DOMAIN1.daily'
        t += relativedelta(months=1)
    cmd = f'cdo --sortname -f nc4 -z zip mergetime {fnins} tmp_for.nc'
    print(cmd); os.system(cmd)
    
    if not os.path.isdir('basins/averaged'):
        os.system('mkdir -p basins/averaged')
    
    for name in site_names:
        
        fnout = f'basins/averaged/{name}_SWE.txt'
        cmd = f'{cdocmd} -mul -selname,SNEQV tmp_out.nc ../../domain/masks/{name}.nc > {fnout}'
        print(cmd); os.system(cmd)
        df_swe = pd.read_csv(fnout, sep='\s+', skiprows=1, header=None, names=['Date', 'SWE'], index_col='Date')
        
        fnout = f'basins/averaged/{name}_SMTOT.txt'
        cmd = f'{cdocmd} -mul -vertmean -selname,SOIL_M tmp_out.nc ../../domain/masks/{name}.nc > {fnout}'
        print(cmd); os.system(cmd)
        df_smtot = pd.read_csv(fnout, sep='\s+', skiprows=1, header=None, names=['Date', 'SMTOT'], index_col='Date')
        
        fnout = f'basins/averaged/{name}_T2D.txt'
        cmd = f'{cdocmd} -mul -subc,273.15 -selname,T2D tmp_for.nc ../../domain/masks/{name}.nc > {fnout}'
        print(cmd); os.system(cmd)
        df_t2d = pd.read_csv(fnout, sep='\s+', skiprows=1, header=None, names=['Date', 'T2D'], index_col='Date')
        
        fnout = f'basins/averaged/{name}_PREC.txt'
        cmd = f'{cdocmd} -mul -mulc,86400 -selname,RAINRATE tmp_for.nc ../../domain/masks/{name}.nc > {fnout}'
        print(cmd); os.system(cmd)
        df_prec = pd.read_csv(fnout, sep='\s+', skiprows=1, header=None, names=['Date', 'PREC'], index_col='Date')
        
        fnout = f'basins/averaged/{name}_daily.csv'
        df_swe['SMTOT'] = df_smtot['SMTOT']
        df_swe['T2D']   = df_t2d['T2D']
        df_swe['PREC']  = df_prec['PREC']
        df_swe.to_csv(fnout, index=True, float_format='%.3f', date_format='%Y-%m-%d')
    
    os.system('rm -f tmp_*.nc basins/averaged/*.txt')
    
    return 0


if __name__ == '__main__':
    main(sys.argv[1:])
