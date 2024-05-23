''' Extract flows from WRF-Hydro retro simulation for all B-120 sites

Usage:
    python extract_b120_nrt.py [domain] [yyyymm1] [yyyymm2]
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
    
    workdir = f'{config["base_dir"]}/wrf_hydro/{domain}/nrt/output'
    os.chdir(workdir)
    
    kafperday = 86400/1233.48/1000
    
    site_list = pd.read_csv(f'{config["base_dir"]}/wrf_hydro/{domain}/b-120/site_list_25.csv')
    nsites = site_list.shape[0]

    ntimes = round((t2-t1).days/30.5)+1

    data = np.zeros((nsites, ntimes))
    tstamps = []
    mcnt = 0
    t = t1
    while t<=t2:
        
        fnin = f'1km_monthly/{t:%Y%m}.CHRTOUT_DOMAIN1.monthly'
        fin  = nc.Dataset(fnin, 'r')
        print(f'Month {t:%Y%m}')
        
        for i,row in zip(site_list.index, site_list['row']):
            data[i, mcnt] = fin['streamflow'][0, row]
            #if t==t1:
            #    print(f'{site_list["name"][i]} {site_list["id"][i]} {fin["feature_id"][row]}')
            
        tstamps.extend([nc.num2date(fin['time'][i], fin['time'].units).strftime('%Y-%m-16') for i in range(1)])
        fin.close()

        mcnt += 1
        t += relativedelta(months=1)
    
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
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True, drop=True)
        if name=='TRF1':
            df['Qsim'] = np.squeeze(data[i, :]) - np.squeeze(data[i+1, :])
        else:
            df[f'Qsim'] = np.squeeze(data[i, :])
        if name=='TRF1':
            name = 'TRF'

        # read CDEC monthly data
        cdec_file = f'{config["base_dir"]}/obs/cdec/fnf/FNF_monthly_{name}.csv'
        cdec_data = pd.read_csv(cdec_file, index_col='Date', parse_dates=True)

        os.system(f'mkdir -p basins/{t1:%Y}-{t2:%Y}')
        fnout = f'basins/{t1:%Y}-{t2:%Y}/{name}.csv'

        qsimbc = np.zeros(len(df.index))
        fnf    = np.zeros(len(df.index))+np.nan
        cnt = 0
        for idx,row in df.iterrows():
            [matched, mavg] = sparse_cdf_match(domain, np.array([row['Qsim']]), name, idx.month, 1979, 2023, idx.year)
            #print(name, idx.month, idx.year, row['Qsim'], matched[0])
            qsimbc[cnt] = matched[0]
            qobs = cdec_data[cdec_data.index==idx-timedelta(days=15)]
            if len(qobs)==1:
                fnf[cnt] = qobs.to_numpy()[0][0]
            cnt += 1

        df['FNF']    = fnf
        df['QsimBC'] = qsimbc

        retro_file = f'{config["base_dir"]}/wrf_hydro/{domain}/retro/output/basins/1979-2023/matched/{name}.csv'
        retro_data = pd.read_csv(retro_file, index_col='Date', parse_dates=True)

        df2 = pd.concat([retro_data, df])
        df2 = df2[~df2.index.duplicated(keep='first')]
        df.to_csv(fnout, index=True, float_format='%.3f', date_format='%Y-%m-%d')
        
    return 0

def sparse_cdf_match(domain, data, site, month, y1, y2, year):

    # load historic data, FNF and reanalysis simulated values
    hist_file = f'{config["base_dir"]}/wrf_hydro/{domain}/retro/output/basins/{y1}-{y2}/combined/{site}.csv'
    hist_data = pd.read_csv(hist_file, index_col='Date', parse_dates=True)
    # remove dates beyond WY 2023
    hist_data.drop(hist_data[hist_data.index>datetime(2023, 9, 30)].index, inplace=True)
    # remove dates within the year being corrected
    hist_data.drop(hist_data[(hist_data.index>=datetime(year, 1, 1))&(hist_data.index<=datetime(year, 12, 31))].index, inplace=True)
    # remove 0 FNF data -- not doing it since it seems some rivers have quite some zeros flows
    # hist_data.drop(hist_data[hist_data['FNF']<=0].index, inplace=True)
    # extract the target month
    if month!=0:
        hist_month = hist_data[pd.to_datetime(hist_data.index).month == month]
    else:
        hist_amjj  = hist_data[(pd.to_datetime(hist_data.index).month>=4)&(pd.to_datetime(hist_data.index).month<=7)]
        hist_month = hist_amjj.resample('1Y').sum()
    # sort and pair them
    hist_pairs = pd.DataFrame({'FNF': hist_month['FNF'].sort_values().to_numpy(), 'Qsim': hist_month['Qsim'].sort_values().to_numpy()})
    # log ratios
    hist_pairs.loc[hist_pairs['FNF']<=0, ['FNF']] = 0.0001
    hist_pairs.loc[hist_pairs['Qsim']<=0, ['Qsim']] = 0.0001
    hist_pairs['logratio'] = np.log(hist_pairs['FNF']/hist_pairs['Qsim'])

    # start to cdf match
    matched = np.zeros(data.size)
    for i,v in enumerate(data):
        # less than min or greater than max
        if v<=hist_pairs['Qsim'][0]:
            lr = hist_pairs['logratio'][0]
        elif v>=hist_pairs['Qsim'].iloc[-1]:
            lr = hist_pairs['logratio'].iloc[-1]
        else: # in the middle
            j = (hist_pairs['Qsim']<v).sum()-1
            lr1 = hist_pairs['logratio'][j]
            lr2 = hist_pairs['logratio'][j+1]
            qs1 = hist_pairs['Qsim'][j]
            qs2 = hist_pairs['Qsim'][j+1]
            #print(i, v, j, lr1, lr2, qs1, qs2)
            lr  = lr1 + (lr2-lr1) * (v-qs1)/(qs2-qs1)
        matched[i] = v*np.exp(lr)

    avg = hist_month['FNF'].mean()
    return [matched, avg]


if __name__ == '__main__':
    main(sys.argv[1:])

        
