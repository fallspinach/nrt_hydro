''' Extract basin averaged quantities from WRF-Hydro retrospective simulation for all B-120 basins

Usage:
    python extract_average_retro.py [domain] [yyyymm1] [yyyymm2]
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

from mpi4py import MPI

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
    
    workdir = f'{config["base_dir"]}/wrf_hydro/{domain}/retro/output'
    os.chdir(workdir)

    #cdocmd = 'cdo -s -w -outputtab,date,value -fldmean'
    cdocmd = 'cdo -s -w -outputtab,date,name,value -fldmean'
    
    site_list = pd.read_csv(f'{config["base_dir"]}/wrf_hydro/{domain}/b-120/site_list_25.csv')
    site_names = site_list['name'].tolist()
    site_names.remove('TRF1'); site_names.remove('TRF2')
    site_names.append('TRF')

    tmp_out = f'tmp_out_avg_{t1:%Y%m}-{t2:%Y%m}'
    tmp_for = f'tmp_for_avg_{t1:%Y%m}-{t2:%Y%m}'
    tmp_out_mon = f'tmp_out_avg_{t1:%Y%m}-{t2:%Y%m}.monthly'
    tmp_for_mon = f'tmp_for_avg_{t1:%Y%m}-{t2:%Y%m}.monthly'

    dout = f'basins/{t1:%Y%m}-{t2:%Y%m}/averaged'
    if rank==0:
        if not os.path.isdir(dout):
            os.system(f'mkdir -p {dout}')
        # daily output
        fnins = ''
        t = t1
        while t<=t2:
            fnins += f' 1km_daily/{t:%Y/%Y%m}.LDASOUT_DOMAIN1'
            t += relativedelta(months=1)
        cmd = f'cdo -O --sortname -f nc4 -z zip mergetime -apply,-expr,"SNEQV=SNEQV;SMTOT=sellevel(SOIL_M,1)*0.05+sellevel(SOIL_M,2)*0.15+sellevel(SOIL_M,3)*0.3+sellevel(SOIL_M,4)*0.5" [ {fnins} ] {tmp_out}'
        print(cmd); os.system(cmd)
        # monthly output
        fnins = ''
        t = t1
        while t<=t2:
            fnins += f' 1km_monthly/{t:%Y%m}.LDASOUT_DOMAIN1.monthly'
            t += relativedelta(months=1)
        cmd = f'cdo -O --sortname -f nc4 -z zip mergetime -apply,-expr,"SNEQV=SNEQV;SMTOT=sellevel(SOIL_M,1)*0.05+sellevel(SOIL_M,2)*0.15+sellevel(SOIL_M,3)*0.3+sellevel(SOIL_M,4)*0.5" [ {fnins} ] {tmp_out_mon}'
        print(cmd); os.system(cmd)
    
    if rank==size-1:
        # daily forcing
        fnins = ''
        t = t1
        while t<=t2:
            fnins += f' ../forcing/1km_daily/{t:%Y%m}.LDASIN_DOMAIN1.daily'
            t += relativedelta(months=1)
        cmd = f'cdo -O --sortname -f nc4 -z zip mergetime -apply,-expr,"T2D=T2D-273.15;RAINRATE=RAINRATE*86400" [ {fnins} ] {tmp_for}'
        print(cmd); os.system(cmd)
        # monthly forcing
        fnins = ''
        t = t1
        while t<=t2:
            fnins += f' ../forcing/1km_monthly/{t:%Y%m}.LDASIN_DOMAIN1.monthly'
            t += relativedelta(months=1)
        cmd = f'cdo -O --sortname -f nc4 -z zip mergetime -apply,-expr,"T2D=T2D-273.15;RAINRATE=RAINRATE*86400;RAD=SWDOWN+LWDOWN" [ {fnins} ] {tmp_for_mon}.nc'
        print(cmd); os.system(cmd)
        cmd = f'cdo -O --sortname -f nc4 -z zip merge -selname,T2D,RAD {tmp_for_mon}.nc -muldpm -selname,RAINRATE {tmp_for_mon}.nc {tmp_for_mon}'
        print(cmd); os.system(cmd)

    comm.Barrier()    
    for name in site_names[rank::size]:

        # daily output
        fnout = f'{dout}/{name}_out.txt'
        cmd = f'{cdocmd} -ifthen ../../domain/masks/{name}.nc {tmp_out} | awk \'BEGIN {{print "Date,SNEQV,SMTOT"}} {{if (NR>2&&$1!=lastdate) print lastdate","a["SNEQV"]","a["SMTOT"]; a[$2]=$3; lastdate=$1}} END {{print lastdate","a["SNEQV"]","a["SMTOT"]}}\' > {fnout}'
        os.system(cmd)
        df_out = pd.read_csv(fnout, index_col='Date')
        
        # daily forcing
        fnout = f'{dout}/{name}_for.txt'
        cmd = f'{cdocmd} -ifthen ../../domain/masks/{name}.nc {tmp_for} | awk \'BEGIN {{print "Date,T2D,PREC"}} {{if (NR>2&&$1!=lastdate) print lastdate","a["T2D"]","a["RAINRATE"]; a[$2]=$3; lastdate=$1}} END {{print lastdate","a["T2D"]","a["RAINRATE"]}}\' > {fnout}'
        os.system(cmd)
        df_for = pd.read_csv(fnout, index_col='Date')
        
        fnout = f'{dout}/{name}_daily.csv'
        df_out['T2D']  = df_for['T2D']
        df_out['PREC'] = df_for['PREC']
        df_out.to_csv(fnout, index=True, float_format='%.3f', date_format='%Y-%m-%d')
    
        # monthly output
        fnout = f'{dout}/{name}_out.txt'
        cmd = f'{cdocmd} -ifthen ../../domain/masks/{name}.nc {tmp_out_mon} | awk \'BEGIN {{print "Date,SNEQV,SMTOT"}} {{if (NR>1) sub(/..$/, "01", $1); if (NR>2&&$1!=lastdate) print lastdate","a["SNEQV"]","a["SMTOT"]; a[$2]=$3; lastdate=$1}} END {{print lastdate","a["SNEQV"]","a["SMTOT"]}}\' > {fnout}'
        os.system(cmd)
        df_out_mon = pd.read_csv(fnout, index_col='Date')
        
        # monthly forcing
        fnout = f'{dout}/{name}_for.txt'
        cmd = f'{cdocmd} -ifthen ../../domain/masks/{name}.nc {tmp_for_mon} | awk \'BEGIN {{print "Date,T2D,PREC,RAD"}} {{if (NR>1) sub(/..$/, "01", $1); if (NR>2&&$1!=lastdate) print lastdate","a["T2D"]","a["RAINRATE"]","a["RAD"]; a[$2]=$3; lastdate=$1}} END {{print lastdate","a["T2D"]","a["RAINRATE"]","a["RAD"]}}\' > {fnout}'
        os.system(cmd)
        df_for_mon = pd.read_csv(fnout, index_col='Date')

        df_q = pd.read_csv(f'basins/by_year/{t1:%Y%m}-{t2:%Y%m}/simulated/{name}_monthly.csv', index_col='Date')
        
        fnout = f'{dout}/{name}_monthly.csv'
        df_out_mon['T2D']  = df_for_mon['T2D']
        df_out_mon['PREC'] = df_for_mon['PREC']
        df_out_mon['RAD']  = df_for_mon['RAD']
        df_out_mon['SNEQV'] = df_out['SNEQV']
        df_out_mon['SMTOT'] = df_out['SMTOT']
        df_out_mon['Qsim'] = df_q['Qsim']
        df_out_mon.to_csv(fnout, index=True, float_format='%.3f', date_format='%Y-%m-%d')
    
    comm.Barrier()
    if rank==0:
        os.system(f'rm -f {tmp_out} {tmp_for} {tmp_out_mon} {tmp_for_mon} {tmp_for_mon}.nc {dout}/*.txt')
    
    return 0


if __name__ == '__main__':
    main(sys.argv[1:])
