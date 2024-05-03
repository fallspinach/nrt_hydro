''' Extract flows from WRF-Hydro ensemble forecast for all B-120 sites and perform CDF matching (with FNF obs)

Usage:
    python extract_b120_ens_a2j_with_fnf.py [domain] [fcst_start] [fcst_end] [fcst_update] [fcst_type]
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

    t1 = datetime.strptime(argv[1], '%Y%m%d')
    t2 = datetime.strptime(argv[2], '%Y%m%d')
    tupdate = datetime.strptime(argv[3], '%Y%m%d')
    t0 = t1.replace(month=4)
    
    workdir = f'{config["base_dir"]}/wrf_hydro/{domain}/fcst/{argv[4]}/output/init{t1:%Y%m%d}_update{tupdate:%Y%m%d}'
    os.chdir(workdir)
    nens = len(glob('??'))
    #nens = 2
    probs = np.array([0.1, 0.5, 0.9])
    
    kafperday = 86400/1233.48/1000
    
    site_list = pd.read_csv(f'{config["base_dir"]}/wrf_hydro/{domain}/b-120/site_list_25.csv')
    nsites = site_list.shape[0]
    fnin = f'01/{t0:%Y%m%d}-{t2:%Y%m%d}.CHRTOUT_DOMAIN1.monthly'
    fin  = nc.Dataset(fnin, 'r')
    ntimes = fin['time'].size
    tstamps = [nc.num2date(fin['time'][i], fin['time'].units).strftime('%Y-%m-16') for i in range(ntimes)]
    # give April-July sum a special time stamp - July 31
    tstamps.append( nc.num2date(fin['time'][-1], fin['time'].units).strftime('%Y') + '-07-31' )
    # find April, has to be 0 now
    i_apr = 0
    # number of observed months
    n_obs = ntimes-6
    fin.close()

    data = np.zeros((nsites, ntimes+1, nens))
    for ens in range(1, nens+1):
        
        fnin = f'{ens:02d}/{t0:%Y%m%d}-{t2:%Y%m%d}.CHRTOUT_DOMAIN1.monthly'
        fin  = nc.Dataset(fnin, 'r')
        print(f'Ens {ens:02d}')
        
        for i,row in zip(site_list.index, site_list['row']):
            data[i, :ntimes, ens-1] = fin['streamflow'][:, row]
            
        fin.close()
    
    data *= kafperday
    for m in range(ntimes):
        mds = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        month = int(tstamps[m].split('-')[1])
        year  = int(tstamps[m].split('-')[0])
        if year%4==0 and month==2:
            md = 29
        else:
            md = mds[month-1]
        data[:, m, :] *= md

    # April-July sum minus observed months
    data[:, ntimes, :] = data[:, i_apr+n_obs:i_apr+4, :].sum(axis=1)
    
    for i,name in zip(site_list.index, site_list['name']):

        if name=='TRF2':
            continue
        df = pd.DataFrame({'Date': tstamps})
        df.set_index('Date', inplace=True, drop=True)
        for e in range(nens):
            if name=='TRF1':
                df[f'Ens{e+1:02d}'] = np.squeeze(data[i, :, e]) - np.squeeze(data[i+1, :, e])
            else:
                df[f'Ens{e+1:02d}'] = np.squeeze(data[i, :, e])
        if name=='TRF1':
            name = 'TRF'
        
        # read CDEC monthly data
        cdec_file = f'{config["base_dir"]}/wrf_hydro/{domain}/obs/cdec/fnf/FNF_monthly_{name}.csv'
        cdec_data = pd.read_csv(cdec_file, index_col='Date', parse_dates=True)
        
        # cdf matching
        ee = np.zeros([probs.size, ntimes+1])
        pp = np.zeros([probs.size, ntimes+1])
        # AMS formula for exceedance probability with plotting position b is: p = (i-b)/(n+1-2b) and i = (n+1-2b)*p+b
        # using plotting position b=0.4 from Cunnane (1978) for GEV p = (i-0.4)/(n+0.2) and i = (n+0.2)*p+0.4
        ii    = (nens+0.2)*(1-probs)+0.4-1            # real index value in decimals
        ii_lo = np.floor((nens+0.2)*(1-probs)+0.4).astype(int)-1  # lower bound integer
        ii_up = np.ceil((nens+0.2)*(1-probs)+0.4).astype(int)-1   # upper bound integer
        ii_lo[ii_lo<0] = 0                        # cap all index values, i.e., no extrapolation
        ii_up[ii_up>nens-1] = nens-1
        wt_lo = np.ones(probs.shape)*0.5          # weight for lower bound integer, initialize to 0.5
        wt_up = np.ones(probs.shape)*0.5          # weight for upper bound integer, initialize to 0.5
        interp = (ii_lo!=ii_up)                   # interpolation needed only if the real index is not an integer
        wt_lo[interp] = ii_up[interp] - ii
        wt_up[interp] = ii - ii_lo[interp]
        
        if i==0:
            print(f'probs: {probs}'); print(f'index: {ii}'); print(f'index lower: {ii_lo}'); print(f'index upper: {ii_up}')
            print(f'weight lower: {wt_lo}'); print(f'weight upper: {wt_up}')
        
        avg = np.zeros(ntimes+1)
        
        for m in range(ntimes+1):
            
            if m<ntimes:
                month = int(df.index[m].split('-')[1])
            else:
                # give April-July sum a special month number 0
                month = 0
            
            [matched, mavg] = sparse_cdf_match(domain, df.iloc[m].to_numpy(), name, month, n_obs)
            
            # swap in observed values if existed
            if m<n_obs:
                tt = t0+relativedelta(months=m) #+ timedelta(days=15) # CDEC monthly timestamps on 1st day of the month instead of 16th
                qobs = cdec_data[pd.to_datetime(cdec_data.index)==tt].to_numpy()[0][0] #; print(qobs, type(qobs))
                print(name, ': ', tt.strftime('%Y-%m-%d '), qobs, ', ', data[i, m, :2], matched[:2])
                if not np.isnan(qobs):
                    matched[:] = qobs
            
            df.iloc[m] = matched
            matched.sort()
            
            ee[:,m] = matched[ii_lo]*wt_lo + matched[ii_up]*wt_up
            pp[:,m] = ee[:,m]/mavg*100
            
            avg[m] = mavg

        # add observed months back to April-July sums
        ee[:, ntimes] += ee[:, :n_obs].sum(axis=1)
        avg[ntimes] += avg[:n_obs].sum()
        pp[:, ntimes] = ee[:,ntimes]/avg[ntimes]*100

        for ip,prob in enumerate(probs):
            pct = int(prob*100)
            df[f'Exc{pct:02d}'] = ee[ip,:]
            df[f'Pav{pct:02d}'] = pp[ip,:]
        df['Avg'] = avg
        
        os.system('mkdir -p basins')
        fnout = f'basins/{name}_{t1:%Y%m%d}-{t2:%Y%m%d}.csv'
        df.to_csv(fnout, index=True, float_format='%.3f', date_format='%Y-%m-%d')
        
    return 0

def sparse_cdf_match(domain, data, site, month, n_obs):
        
    # load historic data, FNF and reanalysis simulated values
    hist_file = f'{config["base_dir"]}/wrf_hydro/{domain}/retro/output/basins/csv/{site}.csv'
    hist_data = pd.read_csv(hist_file, index_col='Date', parse_dates=True)
    # remove dates beyond 2020
    hist_data.drop(hist_data[hist_data.index>datetime(2020, 12, 31)].index, inplace=True)
    # remove 0 FNF data -- not doing it since it seems some rivers have quite some zeros flows
    # hist_data.drop(hist_data[hist_data['FNF']<=0].index, inplace=True)
    # extract the target month
    if month!=0:
        hist_month = hist_data[pd.to_datetime(hist_data.index).month == month]
    else:
        # April-July minus observed months
        hist_a2j   = hist_data[(pd.to_datetime(hist_data.index).month>=4+n_obs)&(pd.to_datetime(hist_data.index).month<=7)]
        hist_month = hist_a2j.resample('1Y').sum()
    # sort and pair them
    hist_pairs = pd.DataFrame({'FNF': hist_month['FNF'].sort_values().to_numpy(), 'Qsim': hist_month['Qsim'].sort_values().to_numpy()})
    # log ratios
    hist_pairs.loc[hist_pairs['FNF']<=0, ['FNF']] = 0.0001
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
            lr  = lr1 + (lr2-lr1) * (v-qs1)/(qs2-qs1)
        matched[i] = v*np.exp(lr)
    
    avg = hist_month['FNF'].mean()
    return [matched, avg]
    
if __name__ == '__main__':
    main(sys.argv[1:])

        
