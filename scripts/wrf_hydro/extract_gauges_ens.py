''' Extract monthly flow estimates from WRF-Hydro ensemble for streamflow gauging sites 
    and calculate some exceedance levels (with and without CDF matching)

Usage:
    python extract_gauges_ens.py [domain] [fcst_start] [fcst_end] [fcst_update] [fcst_type] [gauge_list_file] [gauge_id_field]
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
    fcst_type = argv[4]
    
    gauge_list = argv[5]
    id_field   = argv[6]
    
    workdir = f'{config["base_dir"]}/wrf_hydro/{domain}/fcst/{fcst_type}/output/init{t1:%Y%m%d}_update{tupdate:%Y%m%d}'
    os.chdir(workdir)
    nens = len(glob('??'))
    #nens = 2
    probs = np.array([0.1, 0.5, 0.9])
    
    kafperday = 86400/1233.48/1000
    
    site_list = pd.read_csv(f'{config["base_dir"]}/wrf_hydro/{domain}/{gauge_list}')
    nsites = site_list.shape[0]
    fnin = f'01/{t1:%Y%m%d}-{t2:%Y%m%d}.CHRTOUT_DOMAIN1.monthly'
    fin  = nc.Dataset(fnin, 'r')
    ntimes = fin['time'].size
    tstamps = [nc.num2date(fin['time'][i], fin['time'].units).strftime('%Y-%m-01') for i in range(ntimes)]
    fin.close()

    data = np.zeros((nsites, ntimes, nens))
    for ens in range(1, nens+1):
        
        fnin = f'{ens:02d}/{t1:%Y%m%d}-{t2:%Y%m%d}.CHRTOUT_DOMAIN1.monthly'
        fin  = nc.Dataset(fnin, 'r')
        print(f'Ens {ens:02d}')

        streamflow = fin['streamflow']
        for i,row in zip(site_list.index, site_list['row']):
            data[i, :, ens-1] = streamflow[:, row]
            
        fin.close()
    
    data *= kafperday
    for m in range(ntimes):
        month = int(tstamps[m].split('-')[1])
        year  = int(tstamps[m].split('-')[0])
        md = monthrange(year, month)[1] # number of days in the month
        data[:, m, :] *= md

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
    for i,name in zip(site_list.index, site_list[id_field]):

        df = pd.DataFrame({'Date': tstamps})
        df.set_index('Date', inplace=True, drop=True)
        for e in range(nens):
            df[f'Ens{e+1:02d}'] = np.squeeze(data[i, :, e])
        
        # calculate exceedance levels
        ee = np.zeros([probs.size, ntimes])        
        avg = np.zeros(ntimes)
        
        for m in range(ntimes):
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
    for i,name in zip(site_list.index, site_list[id_field]):

        df = pd.DataFrame({'Date': tstamps})
        df.set_index('Date', inplace=True, drop=True)
        for e in range(nens):
            df[f'Ens{e+1:02d}'] = np.squeeze(data[i, :, e])
        
        # calculate exceedance levels
        ee = np.zeros([probs.size, ntimes])
        pp = np.zeros([probs.size, ntimes])
        avg = np.zeros(ntimes)
        
        for m in range(ntimes):
            year  = int(df.index[m].split('-')[0])
            month = int(df.index[m].split('-')[1])
            [matched, mavg] = sparse_cdf_match(domain, df.iloc[m].to_numpy(), name, month, year)
            df.iloc[m] = matched
            matched.sort()
            
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

