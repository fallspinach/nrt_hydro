import sys, os, pytz, time
import netCDF4 as nc
from glob import glob
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from configparser import ConfigParser, ExtendedInterpolation
from utilities import find_last_time
from mpi4py import MPI
import sqlite3

config = ConfigParser(interpolation=ExtendedInterpolation())
config.read('config.ini')

## some setups
#workdir   = config['default']['forecast_dir'] + '/esp/output'

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

## main function
def main(argv):

    '''main loop'''

    y1 = 1979
    y2 = 2020
    freq = 'daily'
    
    dout = '%s/output/rivers' % (config['default']['reanalysis_dir'])
    if not os.path.isdir(dout):
        os.system('mkdir -p %s' % dout)
    
    kafperday = 86400/1233.48/1000
    
    site_list = pd.read_csv(config['default']['base_dir']+'/domain/rivers/line_numbers_cnrfc_order4plus.csv')
    nsites = site_list.shape[0]
    
    if freq=='daily':
        fnin = '%s/output/1km_%s/stat/%d-%d.STREAMFLOW.ydrunpctl.00-99' % (config['default']['reanalysis_dir'], freq, y1, y2)
    else:
        fnin = '%s/output/1km_%s/stat/%d-%d.STREAMFLOW.%s.ydrunpctl.00-99' % (config['default']['reanalysis_dir'], freq, y1, y2, freq)
    
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

        fnout = '%s/CHRTOUT_%d-%d.%s.pctl%02d.csv.gz' % (dout, y1, y2, freq, s)
        df.to_csv(fnout, index=True, float_format='%.3f', date_format='%Y-%m-%d', compression='gzip')
        
        data = None
        del df
        
    return 0

    
if __name__ == '__main__':
    main(sys.argv[1:])

        
