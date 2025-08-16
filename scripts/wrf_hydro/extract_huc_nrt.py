''' Extract HUC basin averaged quantities from WRF-Hydro NRT simulation

Usage:
    python extract_huc_nrt.py [domain] [yyyymm1] [yyyymm2]
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
from scipy import ndimage
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

    # HUC level for basins
    if len(argv)>3:
        huclev = int(argv[3])
    else:
        huclev = 8

    # HUC level for file names
    lablev = 4

    fhuc = nc.Dataset(f'{config["base_dir"]}/wrf_hydro/{domain}/domain/huc{huclev}_{domain}_lcc.nc', 'r')
    huc_data = fhuc[f'huc{huclev}'][:]
    fhuc.close()
    
    workdir = f'{config["base_dir"]}/wrf_hydro/{domain}/nrt/output'
    os.chdir(workdir)

    t = t1
    while t<=t2:

        print(f'{t:%Y-%m-%d}')

        # daily output
        print('  daily output')
        fin = nc.Dataset(f'1km_daily/{t:%Y%m}.LDASOUT_DOMAIN1', 'r')
        ntimes = fin['time'].size
        if t==t1:
            tstamps = [nc.num2date(fin['time'][i], fin['time'].units).strftime('%Y-%m-%d') for i in range(ntimes)]
        else:
            tstamps.extend([nc.num2date(fin['time'][i], fin['time'].units).strftime('%Y-%m-%d') for i in range(ntimes)])
        #print(tstamps)
        for i in range(ntimes):
            data_swe = np.squeeze(fin['SNEQV'][i, :, :])
            data_sm  = np.squeeze(fin['SOIL_M'][i, :, 0, :]*0.05 + fin['SOIL_M'][i, :, 1, :]*0.15 + fin['SOIL_M'][i, :, 2, :]*0.3 + fin['SOIL_M'][i, :, 3, :]*0.5)
            if t==t1 and i==0:
                huc_data[data_swe.mask] = 0
                huc_ids = np.unique(huc_data)
                huc_ids = huc_ids[huc_ids!=0]
                huc_means_swe = ndimage.mean(data_swe, labels=huc_data, index=huc_ids)
                huc_means_sm  = ndimage.mean(data_sm,  labels=huc_data, index=huc_ids)
            else:
                huc_means_swe = np.vstack((huc_means_swe, ndimage.mean(data_swe, labels=huc_data, index=huc_ids)))
                huc_means_sm  = np.vstack((huc_means_sm,  ndimage.mean(data_sm,  labels=huc_data, index=huc_ids)))
        fin.close()

        # daily forcing
        print('  daily forcing')
        fin = nc.Dataset(f'../forcing/1km_daily/{t:%Y%m}.LDASIN_DOMAIN1.daily', 'r')
        for i in range(ntimes):
            data_p = np.squeeze(fin['RAINRATE'][i, :, :]*86400)
            data_t = np.squeeze(fin['T2D'][i, :, :]-273.15)
            if t==t1 and i==0:
                huc_means_p = ndimage.mean(data_p, labels=huc_data, index=huc_ids)
                huc_means_t = ndimage.mean(data_t, labels=huc_data, index=huc_ids)
            else:
                huc_means_p = np.vstack((huc_means_p, ndimage.mean(data_p, labels=huc_data, index=huc_ids)))
                huc_means_t = np.vstack((huc_means_t, ndimage.mean(data_t, labels=huc_data, index=huc_ids)))
        fin.close()
        
        # monthly output
        print('  monthly output/forcing')
        if t==t1:
            tstamps_mon = [t.strftime('%Y-%m-%d')]
        else:
            tstamps_mon.append(t.strftime('%Y-%m-%d'))
        fin = nc.Dataset(f'1km_monthly/{t:%Y%m}.LDASOUT_DOMAIN1.monthly', 'r')
        data_swe_mon = np.squeeze(fin['SNEQV'][0, :, :])
        data_sm_mon  = np.squeeze(fin['SOIL_M'][0, :, 0, :]*0.05 + fin['SOIL_M'][0, :, 1, :]*0.15 + fin['SOIL_M'][0, :, 2, :]*0.3 + fin['SOIL_M'][0, :, 3, :]*0.5)
        if t==t1:
            huc_means_swe_mon = ndimage.mean(data_swe_mon, labels=huc_data, index=huc_ids)
            huc_means_sm_mon  = ndimage.mean(data_sm_mon,  labels=huc_data, index=huc_ids)
        else:
            huc_means_swe_mon = np.vstack((huc_means_swe_mon, ndimage.mean(data_swe_mon, labels=huc_data, index=huc_ids)))
            huc_means_sm_mon  = np.vstack((huc_means_sm_mon,  ndimage.mean(data_sm_mon,  labels=huc_data, index=huc_ids)))
        fin.close()
        
        # monthly forcing
        fin = nc.Dataset(f'../forcing/1km_monthly/{t:%Y%m}.LDASIN_DOMAIN1.monthly', 'r')
        md = monthrange(t.year, t.month)[1]
        data_p_mon = np.squeeze(fin['RAINRATE'][0, :, :]*86400*md)
        data_t_mon = np.squeeze(fin['T2D'][0, :, :]-273.15)
        if t==t1:
            huc_means_p_mon = ndimage.mean(data_p_mon, labels=huc_data, index=huc_ids)
            huc_means_t_mon = ndimage.mean(data_t_mon, labels=huc_data, index=huc_ids)
        else:
            huc_means_p_mon = np.vstack((huc_means_p_mon, ndimage.mean(data_p_mon, labels=huc_data, index=huc_ids)))
            huc_means_t_mon = np.vstack((huc_means_t_mon, ndimage.mean(data_t_mon, labels=huc_data, index=huc_ids)))
        fin.close()
        
        t += relativedelta(months=1)

    huc_means_swe = np.atleast_2d(huc_means_swe); huc_means_swe_mon = np.atleast_2d(huc_means_swe_mon)
    huc_means_sm  = np.atleast_2d(huc_means_sm);  huc_means_sm_mon  = np.atleast_2d(huc_means_sm_mon)
    huc_means_p   = np.atleast_2d(huc_means_p);   huc_means_p_mon   = np.atleast_2d(huc_means_p_mon)
    huc_means_t   = np.atleast_2d(huc_means_t);   huc_means_t_mon   = np.atleast_2d(huc_means_t_mon)
    
    # print some results
    if False:
        print(len(tstamps), huc_ids.size, huc_means_p.shape, huc_means_t.shape, huc_means_swe.shape, huc_means_sm.shape)
        print(len(tstamps), huc_ids.size, huc_means_p_mon.shape, huc_means_t_mon.shape, huc_means_swe_mon.shape, huc_means_sm_mon.shape)
        for j in range(5):
            print(f'{huc_ids[j]:0{huclev}d}:')
            print('  daily:')
            for i in range(3 if ntimes>3 else ntimes):
                print(f'{tstamps[i]},{huc_means_p[i,j]:.4f},{huc_means_t[i,j]:.3f},{huc_means_swe[i,j]:.4f},{huc_means_sm[i,j]:.5f}')
            print('  monthly:')
            print(f'{tstamps_mon[0]},{huc_means_p_mon[0,j]:.4f},{huc_means_t_mon[0,j]:.3f},{huc_means_swe_mon[0,j]:.4f},{huc_means_sm_mon[0,j]:.5f}')
        
    # output dir
    dout = f'basins/huc{huclev}'
    if not os.path.isdir(dout):
        os.system(f'mkdir -p {dout}')
    
    # input dir for recennt retro results
    t1_retro = datetime(t1.year, 1, 1)
    t2_retro = t1 - relativedelta(months=1)
    din = f'../../retro/output/basins/by_year/{t1_retro:%Y%m}-{t2_retro:%Y%m}/huc{huclev}'

    huc2_ids = np.unique(np.round(huc_ids, lablev-huclev)/pow(10, huclev-lablev)).astype(int)
    print(f'Writing data: data HUC level={huclev} (total={huc_ids.size}), file labeled at HUC level={lablev} (total={huc2_ids.size})')

    for k in range(huc2_ids.size):
        huc2_id = huc2_ids[k]
        #print(f'  processing HUC{lablev}={huc2_id:0{lablev}d}')

        fnin = f'{din}/{huc2_id:0{lablev}d}_daily.csv.gz'
        if os.path.isfile(fnin):
            df_daily_all = pd.read_csv(fnin, dtype={f'HUC{huclev}': str})
        else:
            df_daily_all = pd.DataFrame()
        fnin = f'{din}/{huc2_id:0{lablev}d}_monthly.csv.gz'
        if os.path.isfile(fnin):
            df_monthly_all = pd.read_csv(fnin, dtype={f'HUC{huclev}': str})
        else:
            df_monthly_all = pd.DataFrame()
        
        for j in range(huc_ids.size):
            huc_id = huc_ids[j]
            
            if np.round(huc_id, lablev-huclev)/pow(10, huclev-lablev)==huc2_id:
                #print(f'    HUC{huclev}={huc_id:0{huclev}d}')
                # daily
                df_daily = pd.DataFrame({'Date': pd.to_datetime(tstamps, format='%Y-%m-%d')})
                df_daily['Date'] = pd.to_datetime(df_daily['Date']).dt.date
                df_daily[f'HUC{huclev}']  = f'{huc_id:0{huclev}d}'
                df_daily['SWE']   = huc_means_swe[:, j]
                df_daily['SMTOT'] = huc_means_sm[:, j]
                df_daily['T2D']   = huc_means_t[:, j]
                df_daily['PREC']  = huc_means_p[:, j]

                # monthly
                df_monthly = pd.DataFrame({'Date': pd.to_datetime(tstamps_mon, format='%Y-%m-%d')})
                df_monthly['Date'] = pd.to_datetime(df_monthly['Date']).dt.date
                df_monthly[f'HUC{huclev}']  = f'{huc_id:0{huclev}d}'
                df_monthly['SWE']   = huc_means_swe_mon[:, j]
                df_monthly['SMTOT'] = huc_means_sm_mon[:, j]
                df_monthly['T2D']   = huc_means_t_mon[:, j]
                df_monthly['PREC']  = huc_means_p_mon[:, j]
                
                df_daily_all   = pd.concat([df_daily_all, df_daily], ignore_index=True)
                df_monthly_all = pd.concat([df_monthly_all, df_monthly], ignore_index=True)
        
        fnout = f'{dout}/{huc2_id:0{lablev}d}_daily.csv.gz'
        df_daily_all.to_csv(fnout, compression='gzip', index=False, float_format='%.4f', date_format='%Y-%m-%d')
        fnout = f'{dout}/{huc2_id:0{lablev}d}_monthly.csv.gz'
        df_monthly_all.to_csv(fnout, compression='gzip', index=False, float_format='%.4f', date_format='%Y-%m-%d')
        
    return 0


if __name__ == '__main__':
    main(sys.argv[1:])
