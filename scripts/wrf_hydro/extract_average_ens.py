''' Extract monthly basin averaged quantities from WRF-Hydro ensemble simulations and forcings for B-120 basins for LSTM

Usage:
    python extract_average_ens.py [domain] [fcst_start] [fcst_end] [fcst_update] [ens1] [ens2] [fcst_type]
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

    t1 = datetime.strptime(argv[1], '%Y%m%d')
    t2 = datetime.strptime(argv[2], '%Y%m%d')
    tupdate = datetime.strptime(argv[3], '%Y%m%d')
    ens1 = int(argv[4])
    ens2 = int(argv[5])
    
    workdir = f'{config["base_dir"]}/wrf_hydro/{domain}/fcst/{argv[6]}/output/init{t1:%Y%m%d}_update{tupdate:%Y%m%d}'
    os.chdir(workdir)
    nens = len(glob('??'))
    forcdir = f'{config["base_dir"]}/wrf_hydro/{domain}/fcst/{argv[6]}/forcing/{t1:%Y}-{t2:%Y}'
    
    os.chdir(workdir)

    cdocmd = 'cdo -s -w -outputtab,date,name,value -fldmean'
    
    site_list = pd.read_csv(f'{config["base_dir"]}/wrf_hydro/{domain}/b-120/site_list_25.csv')
    site_names = site_list['name'].tolist()
    site_names.remove('TRF1'); site_names.remove('TRF2')
    site_names.append('TRF')

    for ens in range(ens1, ens2+1)[rank::size]:
        
        ens_out     = f'{ens:02d}/{t1:%Y%m%d}-{t2:%Y%m%d}.LDASOUT_DOMAIN1.daily'
        ens_for_mon1= f'{ens:02d}/{t1:%Y%m}.LDASIN_DOMAIN1.monthly'
        ens_for_mon = f'{ens:02d}/{t1:%Y%m}-{t2:%Y%m}.LDASIN_DOMAIN1.monthly'

        dout = f'basins/averaged/{ens:02d}'
        if not os.path.isdir(dout):
            os.system(f'mkdir -p {dout}')
            
        # daily output
        t = t1
        fnins = f'{config["base_dir"]}/wrf_hydro/{domain}/nrt/output/1km_daily/{t:%Y%m0100}.LDASOUT_DOMAIN1'
        t += relativedelta(months=1)
        while t<=t2:
            fnins += f' {ens:02d}/{t:%Y%m0100}.LDASOUT_DOMAIN1'
            t += relativedelta(months=1)
        cmd = f'cdo -O --sortname -f nc4 -z zip mergetime -apply,-expr,"SNEQV=SNEQV;SMTOT=sellevel(SOIL_M,1)*0.05+sellevel(SOIL_M,2)*0.15+sellevel(SOIL_M,3)*0.3+sellevel(SOIL_M,4)*0.5" [ {fnins} ] {ens_out}'
        print(cmd); os.system(cmd)
    
        # monthly forcing
        # calculate the first month
        fnins = ''
        t = t1
        t2m = t1 + relativedelta(months=1)
        while t<t2m:
            flink = f'{forcdir}/{ens:02d}/{t:%Y/%Y%m%d}.LDASIN_DOMAIN1'
            tlink = datetime.strptime(os.path.basename(os.readlink(flink)).split('.')[0], '%Y%m%d')
            ydiff = t.year - tlink.year
            if ydiff!=0:
                fnins += f' -shifttime,{ydiff:d}year {flink}'
            else:
                fnins += f' {flink}'
            t += timedelta(days=1)
        cmd = f'cdo -O --sortname -f nc4 -z zip monmean -setrtomiss,1e10,1e30 -mergetime [ {fnins} ] {ens_for_mon1}'
        print(cmd); os.system(cmd)
        
        # assemble the rest of months from retro data by tracing the links
        fnins = ''
        t = t1 + relativedelta(months=1)
        while t<=t2:
            flink = f'{forcdir}/{ens:02d}/{t:%Y/%Y%m%d}.LDASIN_DOMAIN1'
            if 'retro' in os.readlink(flink):
                tlink = datetime.strptime(os.path.basename(os.readlink(flink)).split('.')[0], '%Y%m%d')
            else:
                t10 = t + timedelta(days=10)
                flink10 = f'{forcdir}/{ens:02d}/{t10:%Y/%Y%m%d}.LDASIN_DOMAIN1'
                tlink = datetime.strptime(os.path.basename(os.readlink(flink10)).split('.')[0], '%Y%m%d') - timedelta(days=10)
            ydiff = t.year - tlink.year
            fnins += f' {config["base_dir"]}/wrf_hydro/{domain}/retro/forcing/1km_monthly/{tlink:%Y%m}.LDASIN_DOMAIN1.monthly'
            t += relativedelta(months=1)
        cmd = f'cdo -O --sortname -f nc4 -z zip mergetime -apply,-expr,"T2D=T2D-273.15;RAINRATE=RAINRATE*86400;SWDOWN=SWDOWN;LWDOWN=LWDOWN;Q2D=Q2D*1000;WIND=sqrt(U2D*U2D+V2D*V2D)" [ {ens_for_mon1} -apply,-shifttime,{ydiff:d}year [ {fnins} ] ] {ens_for_mon}.nc'
        print(cmd); os.system(cmd)
        cmd = f'cdo -O --sortname -f nc4 -z zip setrtomiss,-10e20,-1000 -setrtomiss,10e6,10e20 -merge -selname,T2D,SWDOWN,LWDOWN,Q2D,WIND {ens_for_mon}.nc -muldpm -selname,RAINRATE {ens_for_mon}.nc {ens_for_mon}'
        print(cmd); os.system(cmd)

        # read streamflow data
        fnin = f'{ens:02d}/{t1:%Y%m%d}-{t2:%Y%m%d}.CHRTOUT_DOMAIN1.monthly'
        fin  = nc.Dataset(fnin, 'r')
        nsites = site_list.shape[0]
        ntimes = fin['time'].size
        tstamps = [nc.num2date(fin['time'][i], fin['time'].units).strftime('%Y-%m-01') for i in range(ntimes)]
        data = np.zeros((nsites, ntimes))        
        for i,row in zip(site_list.index, site_list['row']):
            data[i, :] = fin['streamflow'][:, row]
        fin.close()
        kafperday = 86400/1233.48/1000
        data *= kafperday
        for m in range(ntimes):
            month = int(tstamps[m].split('-')[1])
            year  = int(tstamps[m].split('-')[0])
            md = monthrange(year, month)[1] # number of days in the month
            data[:, m] *= md
        df_q = pd.DataFrame({'Date': pd.to_datetime(tstamps,format='%Y-%m-%d')})
        df_q.set_index('Date', inplace=True, drop=True)
        for i,name in zip(site_list.index, site_list['name']):
            if name=='TRF2':
                continue
            if name=='TRF1':
                df_q['TRF'] = np.squeeze(data[i, :]) - np.squeeze(data[i+1, :])
            else:
                df_q[name] = np.squeeze(data[i, :])

        for name in site_names:

            # daily output on 1st day of the month
            fnout = f'{dout}/{name}_out.txt'
            cmd = f'{cdocmd} -ifthen ../../../../domain/masks/{name}.nc {ens_out} | awk \'BEGIN {{print "Date,SWE,SMTOT"}} {{if (NR>2&&$1!=lastdate) print lastdate","a["SNEQV"]","a["SMTOT"]; a[$2]=$3; lastdate=$1}} END {{print lastdate","a["SNEQV"]","a["SMTOT"]}}\' > {fnout}'
            os.system(cmd)
            df_out_mon = pd.read_csv(fnout, index_col='Date', parse_dates=True)
        
            # monthly forcing
            fnout = f'{dout}/{name}_for.txt'
            cmd = f'{cdocmd} -ifthen ../../../../domain/masks/{name}.nc {ens_for_mon} | awk \'BEGIN {{print "Date,T2D,PREC,SWDOWN,LWDOWN,Q2D,WIND"}} {{if (NR>1) sub(/..$/, "01", $1); if (NR>2&&$1!=lastdate) print lastdate","a["T2D"]","a["RAINRATE"]","a["SWDOWN"]","a["LWDOWN"]","a["Q2D"]","a["WIND"]; a[$2]=$3; lastdate=$1}} END {{print lastdate","a["T2D"]","a["RAINRATE"]","a["SWDOWN"]","a["LWDOWN"]","a["Q2D"]","a["WIND"]}}\' > {fnout}'
            os.system(cmd)
            df_for_mon = pd.read_csv(fnout, index_col='Date', parse_dates=True)

            # read streamflow data
            #df_q = pd.read_csv(f'basins/simulated/{name}_{t1:%Y%m%d}-{t2:%Y%m%d}.csv', index_col='Date', parse_dates=True)

            # read NRT data
            df_nrt_mon = pd.read_csv(f'{config["base_dir"]}/wrf_hydro/{domain}/nrt/output/basins/averaged/{name}_monthly.csv', index_col='Date', parse_dates=True)
        
            fnout = f'{dout}/{name}_monthly.csv'
            for v in ['T2D', 'PREC', 'SWDOWN', 'LWDOWN', 'Q2D', 'WIND']:
                df_out_mon[v]  = df_for_mon[v]
            #df_out_mon['Qsim'] = df_q[f'Ens{ens:02d}']
            df_out_mon['Qsim'] = df_q[name]

            if t1.month>=10:
                t0 = datetime(t1.year, 10, 1)
            else:
                t0 = datetime(t1.year-1, 10, 1)
            df_all = pd.concat([df_nrt_mon[t0:t1-relativedelta(months=1)], df_out_mon])
            df_all.to_csv(fnout, index=True, float_format='%.3f', date_format='%Y-%m-%d')
            
            os.system(f'rm -f {ens_for_mon}.nc {dout}/*.txt')
    
    comm.Barrier()
    
    return 0


if __name__ == '__main__':
    main(sys.argv[1:])
