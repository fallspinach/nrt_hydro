''' Extract flows from WRF-Hydro retro simulation for streamflow gauging sites

Usage:
    python extract_gauges_retro_nofnf.py [domain] [yyyymm1] [yyyymm2]
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
    gauge_list = 'domain/feature_gage_row_us_clean.csv'
    id_field   = 'gage_id'

    flag_monthly = False
    
    workdir = f'{config["base_dir"]}/wrf_hydro/{domain}/retro/output'
    os.chdir(workdir)

    # Units: thousand acre feet for (TAF or KAF) monthly flow and cubic feet per second (CFS) for daily flow
    kafperday = 86400/1233.48/1000
    cmstocfs  = 35.3147
    
    site_list = pd.read_csv(f'{config["base_dir"]}/wrf_hydro/{domain}/{gauge_list}', dtype={'gage_id': str}).sort_values(by='gage_id').reset_index(drop=True)
    nsites = site_list.shape[0]
    sites_per_file = 20

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

        if flag_monthly:
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
    if flag_monthly:
        data_monthly *= kafperday
        for m in range(ntimes_monthly):
            month = int(tstamps_monthly[m].split('-')[1])
            year  = int(tstamps_monthly[m].split('-')[0])
            md = monthrange(year, month)[1] # number of days in the month
            data_monthly[:, m] *= md

    # write raw simulated streamflow, both daily and monthly
    if not os.path.isdir(f'basins/{t1:%Y%m}-{t2:%Y%m}/combined'):
        os.system(f'mkdir -p basins/{t1:%Y%m}-{t2:%Y%m}/combined')
    
    for i,name in zip(site_list.index, site_list[id_field]):

        fileno = '%03d' % (int(i/sites_per_file))

        df_daily   = pd.DataFrame({'Date': tstamps_daily})
        df_daily.set_index('Date', inplace=True, drop=True)
        df_daily[id_field] = name
        df_daily['Qsim']   = np.squeeze(data_daily[i, :])
        
        # read USGS data
        usgs_file = f'{config["base_dir"]}/obs/usgs/streamflow/{name}.csv'
        #print(usgs_file)
        usgs_data = pd.read_csv(usgs_file, parse_dates=True, usecols=['Date', 'Qobs'])
        df_daily = pd.merge(df_daily, usgs_data, on='Date', how='left')

        if i%sites_per_file==0:
            df_daily_all   = df_daily.copy()
        else:
            df_daily_all   = pd.concat([df_daily_all, df_daily])
        if i%sites_per_file==sites_per_file-1 or i==nsites-1:
            fnout_daily   = f'basins/{t1:%Y%m}-{t2:%Y%m}/combined/{fileno}_daily.csv.gz'
            if int(i/sites_per_file)%10==0:
                print(fnout_daily)
            df_daily_all.to_csv(fnout_daily, index=False, compression='gzip', float_format='%.3f', date_format='%Y-%m-%d')
                
        if flag_monthly:
            df_monthly = pd.DataFrame({'Date': tstamps_monthly})
            df_monthly.set_index('Date', inplace=True, drop=True)
            df_monthly[id_field] = name
            df_monthly['Qsim'] = np.squeeze(data_monthly[i, :])
            if i%sites_per_file==0:
                df_monthly_all = df_monthly.copy()
            else:
                df_monthly_all = pd.concat([df_monthly_all, df_monthly])
            if i%sites_per_file==sites_per_file-1 or i==nsites-1:
                fnout_monthly = f'basins/{t1:%Y%m}-{t2:%Y%m}/combined/{fileno}_monthly.csv.gz'
                df_monthly_all.to_csv(fnout_monthly, index=False, compression='gzip', float_format='%.4f', date_format='%Y-%m-%d')
            
    return 0


if __name__ == '__main__':
    main(sys.argv[1:])
