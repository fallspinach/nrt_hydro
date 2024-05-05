''' Extract flows from WRF-Hydro retro simulation for all B-120 sites

Usage:
    python bc_b120_retro.py [domain] [yyyy1] [yyyy2]
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
        
    site_list = pd.read_csv(f'{config["base_dir"]}/wrf_hydro/{domain}/b-120/site_list_25.csv')
    for i,name in zip(site_list.index, site_list['name']):

        if name=='TRF2':
            continue
        if name=='TRF1':
            name = 'TRF'
        
        os.system(f'mkdir -p basins/{t1:%Y}-{t2:%Y}/bc')
        fnout = f'basins/{t1:%Y}-{t2:%Y}/bc/{name}.csv'

        comb_file = f'{config["base_dir"]}/wrf_hydro/{domain}/retro/output/basins/{t1:%Y}-{t2:%Y}/comb/{name}.csv'
        df = pd.read_csv(comb_file, index_col='Date', parse_dates=True)

        qsimbc = np.zeros(len(df.index))
        cnt = 0
        for idx,row in df.iterrows():
            [matched, mavg] = sparse_cdf_match(domain, np.array([row['Qsim']]), name, idx.month, t1.year, t2.year, idx.year)
            #print(name, idx.month, idx.year, row['Qsim'], matched[0])
            qsimbc[cnt] = matched[0]
            cnt += 1
        
        df['QsimBC'] = qsimbc
        df.to_csv(fnout, index=True, float_format='%.3f', date_format='%Y-%m-%d')
        
    return 0

def sparse_cdf_match(domain, data, site, month, y1, y2, year):

    # load historic data, FNF and reanalysis simulated values
    hist_file = f'{config["base_dir"]}/wrf_hydro/{domain}/retro/output/basins/{y1}-{y2}/comb/{site}.csv'
    hist_data = pd.read_csv(hist_file, index_col='Date', parse_dates=True)
    # remove dates beyond 2020
    hist_data.drop(hist_data[hist_data.index>datetime(2020, 12, 31)].index, inplace=True)
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

        
