''' Extract flows from WRF-Hydro retro simulation for streamflow gauging sites

Usage:
    python extract_gauges_retro.py [domain] [yyyymm1] [yyyymm2] [gauge_list_file] [gauge_id_field] [fnf_path]
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
from calendar import monthrange
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time
from cdf_match import sparse_cdf_match


## main function
def main(argv):

    '''main loop'''

    domain = argv[0]

    t1 = datetime.strptime(argv[1], '%Y%m')
    t2 = datetime.strptime(argv[2], '%Y%m')
    gauge_list = argv[3]
    id_field   = argv[4]
    fnf_path   = argv[5]
    
    workdir = f'{config["base_dir"]}/wrf_hydro/{domain}/retro/output'
    os.chdir(workdir)

    # Units: thousand acre feet for (TAF or KAF) monthly flow and cubic feet per second (CFS) for daily flow
    kafperday = 86400/1233.48/1000
    cmstocfs  = 35.3147
    
    site_list = pd.read_csv(f'{config["base_dir"]}/wrf_hydro/{domain}/{gauge_list}')
    nsites = site_list.shape[0]

    ntimes_daily   = 0
    ntimes_monthly = 0
    tstamps_daily = []
    tstamps_monthly = []
    
    t = t1
    while t<=t2:

        print(f'Extracting {t:%Y-%m}')
        
        # extract daily data
        fnin_daily = f'1km_daily/{t:%Y}/{t:%Y%m}.CHRTOUT_DOMAIN1'
        fin_daily  = nc.Dataset(fnin_daily, 'r')
        nt_daily   = fin_daily['time'].size
        ntimes_daily += nt_daily
        tstamps_daily.extend([nc.num2date(fin_daily['time'][i], fin_daily['time'].units).strftime('%Y-%m-%d') for i in range(nt_daily)])
        data_tmp = np.zeros((nsites, nt_daily))
        streamflow = fin_daily['streamflow'][:]
        for i,row in zip(site_list.index, site_list['row']):
            data_tmp[i, :] = streamflow[:, row]
        if t==t1:
            data_daily = data_tmp
        else:
            data_daily = np.append(data_daily, data_tmp, 1)
        fin_daily.close()
        
        # extract monthly data
        fnin_monthly = f'1km_monthly/{t:%Y%m}.CHRTOUT_DOMAIN1.monthly'
        fin_monthly  = nc.Dataset(fnin_monthly, 'r')
        ntimes_monthly += 1
        tstamps_monthly.append(t.strftime('%Y-%m-%d'))
        data_tmp = np.zeros((nsites, 1))
        streamflow = fin_monthly['streamflow'][:]
        for i,row in zip(site_list.index, site_list['row']):
            data_tmp[i, :] = streamflow[:, row]
        if t==t1:
            data_monthly = data_tmp
        else:
            data_monthly = np.append(data_monthly, data_tmp, 1)
        fin_monthly.close()
        
        t += relativedelta(months=1)

    # convert units
    data_daily   *= cmstocfs
    data_monthly *= kafperday
    
    for m in range(ntimes_monthly):
        month = int(tstamps_monthly[m].split('-')[1])
        year  = int(tstamps_monthly[m].split('-')[0])
        md = monthrange(year, month)[1] # number of days in the month
        data_monthly[:, m] *= md

    # write raw simulated streamflow, both daily and monthly
    if not os.path.isdir(f'basins/{t1:%Y%m}-{t2:%Y%m}/simulated'):
        os.system(f'mkdir -p basins/{t1:%Y%m}-{t2:%Y%m}/simulated')
    
    for i,name in zip(site_list.index, site_list[id_field]):

        df_daily   = pd.DataFrame({'Date': tstamps_daily})
        df_monthly = pd.DataFrame({'Date': tstamps_monthly})
        df_daily.set_index('Date', inplace=True, drop=True)
        df_monthly.set_index('Date', inplace=True, drop=True)
        
        df_daily['Qsim']   = np.squeeze(data_daily[i, :])
        df_monthly['Qsim'] = np.squeeze(data_monthly[i, :])
            
        fnout_daily   = f'basins/{t1:%Y%m}-{t2:%Y%m}/simulated/{name}_daily.csv'
        fnout_monthly = f'basins/{t1:%Y%m}-{t2:%Y%m}/simulated/{name}_monthly.csv'
        df_daily.to_csv(fnout_daily, index=True, float_format='%.3f', date_format='%Y-%m-%d')
        df_monthly.to_csv(fnout_monthly, index=True, float_format='%.3f', date_format='%Y-%m-%d')
        
    # calculate CDF matched streamflow and write it together with the simulated and observed values, monthly only
    if not os.path.isdir(f'basins/{t1:%Y%m}-{t2:%Y%m}/combined'):
        os.system(f'mkdir -p basins/{t1:%Y%m}-{t2:%Y%m}/combined')
    for i,name in zip(site_list.index, site_list[id_field]):

        df_monthly = pd.DataFrame({'Date': tstamps_monthly})
        df_monthly['Date'] = pd.to_datetime(df_monthly['Date'])
        df_monthly.set_index('Date', inplace=True, drop=True)
        
        df_monthly['Qsim'] = np.squeeze(data_monthly[i, :])
        
        # read FNF monthly data
        fnf_file = f'{config["base_dir"]}/obs/{fnf_path}/fnf/FNF_monthly_{name}.csv'
        fnf_data = pd.read_csv(fnf_file, index_col='Date', parse_dates=True)

        qmatch = np.zeros(len(df_monthly.index))
        fnf    = np.zeros(len(df_monthly.index))+np.nan
        cnt = 0
        for idx,row in df_monthly.iterrows():
            [matched, mavg] = sparse_cdf_match(domain, np.array([row['Qsim']]), name, idx.month, idx.year)
            #print(name, idx.month, idx.year, row['Qsim'], matched[0])
            qmatch[cnt] = matched[0]
            qobs = fnf_data[fnf_data.index==idx]
            if len(qobs)==1:
                fnf[cnt] = qobs.to_numpy()[0][0]
            cnt += 1

        df_monthly['FNF']    = fnf
        df_monthly['Qmatch'] = qmatch
        fnout_monthly = f'basins/{t1:%Y%m}-{t2:%Y%m}/combined/{name}_monthly.csv'
        df_monthly.to_csv(fnout_monthly, index=True, float_format='%.3f', date_format='%Y-%m-%d')
        
    return 0


if __name__ == '__main__':
    main(sys.argv[1:])
