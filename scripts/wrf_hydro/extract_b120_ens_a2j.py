''' Extract monthly flow estimates from WRF-Hydro ensemble for B-120 sites, including Apri-Jul total,
    and calculate some exceedance levels (with and without CDF matching)

Usage:
    python extract_b120_ens_a2j.py [domain] [fcst_start] [fcst_end] [fcst_update] [fcst_type]
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

    t1 = datetime.strptime(argv[1], '%Y%m%d')
    t2 = datetime.strptime(argv[2], '%Y%m%d')
    tupdate = datetime.strptime(argv[3], '%Y%m%d')
    
    workdir = f'{config["base_dir"]}/wrf_hydro/{domain}/fcst/{argv[4]}/output/init{t1:%Y%m%d}_update{tupdate:%Y%m%d}'
    os.chdir(workdir)
    nens = len(glob('??'))
    #nens = 2
    probs = np.array([0.1, 0.5, 0.9])
    
    kafperday = 86400/1233.48/1000
    
    site_list = pd.read_csv(f'{config["base_dir"]}/wrf_hydro/{domain}/b-120/site_list_25.csv')
    nsites = site_list.shape[0]
    fnin = f'01/{t1:%Y%m%d}-{t2:%Y%m%d}.CHRTOUT_DOMAIN1.monthly'
    fin  = nc.Dataset(fnin, 'r')
    ntimes = fin['time'].size
    tstamps = [nc.num2date(fin['time'][i], fin['time'].units).strftime('%Y-%m-01') for i in range(ntimes)]
    # give April-July sum a special time stamp - July 31
    tstamps.append( nc.num2date(fin['time'][-1], fin['time'].units).strftime('%Y') + '-07-31' )
    # find April
    i_apr = [ts.split('-')[1] for ts in tstamps].index('04')
    fin.close()

    data = np.zeros((nsites, ntimes+1, nens))
    for ens in range(1, nens+1):
        
        fnin = f'{ens:02d}/{t1:%Y%m%d}-{t2:%Y%m%d}.CHRTOUT_DOMAIN1.monthly'
        fin  = nc.Dataset(fnin, 'r')
        print(f'Ens {ens:02d}')
        
        for i,row in zip(site_list.index, site_list['row']):
            data[i, :ntimes, ens-1] = fin['streamflow'][:, row]
            
        fin.close()
    
    data *= kafperday
    for m in range(ntimes):
        month = int(tstamps[m].split('-')[1])
        year  = int(tstamps[m].split('-')[0])
        md = monthrange(year, month)[1] # number of days in the month
        data[:, m, :] *= md

    # April-July sum
    data[:, ntimes, :] = data[:, i_apr:i_apr+4, :].sum(axis=1)
    
    # calculate exceedance levels
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
    wt_lo[interp] = ii_up[interp] - ii[interp]
    wt_up[interp] = ii[interp] - ii_lo[interp]
    
    print(f'probs: {probs}'); print(f'index: {ii}'); print(f'index lower: {ii_lo}'); print(f'index upper: {ii_up}')
    print(f'weight lower: {wt_lo}'); print(f'weight upper: {wt_up}')
    
    # write raw simulated streamflow
    os.system('mkdir -p basins/simulated')
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
        
        # calculate exceedance levels
        ee = np.zeros([probs.size, ntimes+1])        
        avg = np.zeros(ntimes+1)
        
        for m in range(ntimes+1):
            ensemble = df.iloc[m].to_numpy()
            ensemble.sort()
            ee[:,m] = ensemble[ii_lo]*wt_lo + ensemble[ii_up]*wt_up
                    
        for ip,prob in enumerate(probs):
            pct = int(prob*100)
            df[f'Exc{pct:02d}'] = ee[ip,:]
        
        fnout = f'basins/simulated/{name}_{t1:%Y%m%d}-{t2:%Y%m%d}.csv'
        df.to_csv(fnout, index=True, float_format='%.3f', date_format='%Y-%m-%d')
        
    # write CDF matched streamflow
    os.system('mkdir -p basins/cdfm')
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
        
        # calculate exceedance levels
        ee = np.zeros([probs.size, ntimes+1])
        pp = np.zeros([probs.size, ntimes+1])
        avg = np.zeros(ntimes+1)
        
        for m in range(ntimes+1):
            year  = int(df.index[m].split('-')[0])
            month = int(df.index[m].split('-')[1])
            # give April-July sum a special month number 0
            if m==ntimes:
                month = 0
            [matched, mavg] = sparse_cdf_match(domain, df.iloc[m].to_numpy(), name, month, year)
            df.iloc[m] = matched
            matched.sort()

            if mavg==0:
                print(f'Zero historical average at location {name} in month {month}')
            ee[:,m] = matched[ii_lo]*wt_lo + matched[ii_up]*wt_up
            pp[:,m] = ee[:,m]/mavg*100
            
            avg[m] = mavg
        
        for ip,prob in enumerate(probs):
            pct = int(prob*100)
            df[f'Exc{pct:02d}'] = ee[ip,:]
            df[f'Pav{pct:02d}'] = pp[ip,:]
        df['Avg'] = avg
        
        fnout = f'basins/cdfm/{name}_{t1:%Y%m%d}-{t2:%Y%m%d}.csv'
        df.to_csv(fnout, index=True, float_format='%.3f', date_format='%Y-%m-%d')
        
    return 0

if __name__ == '__main__':
    main(sys.argv[1:])

