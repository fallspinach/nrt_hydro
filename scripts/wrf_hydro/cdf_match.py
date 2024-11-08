''' Perform CDF matching on raw WRF-Hydro streamflow simulations against FNF

Usage:
    imported by other scripts only
Default values:
    must specify all
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Prototype'

import sys, os, pytz, time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config


def sparse_cdf_match(domain, data, site, month, year):

    yclim1 = config['wrf_hydro'][domain]['climrange'][0]
    yclim2 = config['wrf_hydro'][domain]['climrange'][1]
    
    # load historic data, FNF and reanalysis simulated values
    hist_file = f'{config["base_dir"]}/wrf_hydro/{domain}/retro/output/basins/{yclim1}-{yclim2}/combined/{site}_monthly.csv'
    hist_data = pd.read_csv(hist_file, index_col='Date', parse_dates=True)
    # remove dates beyond last year
    hist_data.drop(hist_data[hist_data.index>datetime(yclim2, 12, 31)].index, inplace=True)
    # remove dates within the year being corrected
    hist_data.drop(hist_data[(hist_data.index>=datetime(year, 1, 1))&(hist_data.index<=datetime(year, 12, 31))].index, inplace=True)
    # remove 0 FNF data -- not doing it since it seems some rivers have quite some zeros flows
    # hist_data.drop(hist_data[hist_data['FNF']<=0].index, inplace=True)
    
    # extract the target month
    if month!=0:
        hist_month = hist_data[pd.to_datetime(hist_data.index).month == month]
    else: # month 0 is reserved for A-J total
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

