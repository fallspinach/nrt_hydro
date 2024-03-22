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
import sqlite3

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

## main function
def main(argv):

    '''main loop'''

    if len(argv)>=1:
        domain = argv[0]
    else:
        domain = 'cnrfc'
    
    y1 = 1979
    y2 = 2023
    freq = 'daily'
    
    dout = f'{config["base_dir"]}/wrf_hydro/{domain}/retro/output/rivers'
    if not os.path.isdir(dout):
        os.system(f'mkdir -p {dout}')
    
    kafperday = 86400/1233.48/1000
    
    site_list = pd.read_csv(f'{config["base_dir"]}/wrf_hydro/{domain}/domain/rivers/line_numbers_{domain}_order4plus.csv')
    nsites = site_list.shape[0]
    
    if freq=='daily':
        fnin = f'{config["base_dir"]}/wrf_hydro/{domain}/retro/output/1km_{freq}/stat/{y1}-{y2}.STREAMFLOW.ydrunpctl.00-99'
    else:
        fnin = f'{config["base_dir"]}/wrf_hydro/{domain}/retro/output/1km_{freq}/stat/{y1}-{y2}.STREAMFLOW.{freq}.ydrunpctl.00-99'
    
    for s in range(100)[rank::size]:
        
        print('Extracting percentile %d' % s)
        
        fin  = nc.Dataset(fnin, 'r')
        nt = fin['time'].size

        data_all = np.squeeze(fin['streamflow'][:, s, :round(site_list['row'].max())+1])
        data = data_all[:, site_list['row'].to_numpy().astype(np.int32)].transpose()
            
        data_all = None
            
        if freq=='daily':
            tstamps = [nc.num2date(fin['time'][0]+1440*i, fin['time'].units).strftime('%Y-%m-%d') for i in range(nt)]
        else:
            tstamps = [nc.num2date(fin['time'][i], fin['time'].units).strftime('%Y-%m-%d') for i in range(nt)]
            
        fin.close()
                
        #data *= kafperday

        df = pd.DataFrame({'Date': tstamps})
        df.set_index('Date', inplace=True, drop=True)
        
        for i,name in zip(site_list.index, site_list['feature_id']):
            df[name] = np.squeeze(data[i, :])

        fnout = f'{dout}/CHRTOUT_{y1}-{y2}.{freq}.pctl{s:02d}.csv.gz'
        df.to_csv(fnout, index=True, float_format='%.3f', date_format='%Y-%m-%d', compression='gzip')
        
        data = None
        del df
        
    return 0

    
if __name__ == '__main__':
    main(sys.argv[1:])

        
