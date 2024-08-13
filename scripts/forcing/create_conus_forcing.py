''' Run GrADS scripts to create 0.01 deg forcing data

Usage:
    python create_conus_forcing.py [yyyymmddhh1] [yyyymmddhh2] [product_type]
Default values:
    must specify all
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, math, pytz, yaml, subprocess
from glob import glob
import netCDF4 as nc
from datetime import datetime, timedelta
from mpi4py import MPI
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time


## some setups
workdir   = f'{config["base_dir"]}/forcing/nwm'
stg4_path = f'{config["base_dir"]}/forcing/stage4/archive' # path to Stage IV files
nld2_path = f'{config["base_dir"]}/forcing/nldas2/NLDAS_FORA0125_H.2.0' # path to NLDAS-2 archive folder
prsm_path = f'{config["base_dir"]}/forcing/prism/recent/nc'             # path to PRISM files

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

## main function
def main(argv):
    
    '''main loop'''
    
    os.chdir(workdir)

    time1 = datetime.strptime(argv[0], '%Y%m%d%H')
    time2 = datetime.strptime(argv[1], '%Y%m%d%H')
    time1 = time1.replace(tzinfo=pytz.utc)
    time2 = time2.replace(tzinfo=pytz.utc)
    prodtype = argv[2]
    
    step  = timedelta(hours=1)

    alltimes = []
    t = time1
    while t <= time2:
        alltimes.append(t)
        t += step

    ntimes = len(alltimes)
    chunk  = math.ceil(ntimes/size)
    #print('Chunk size = %d' % chunk)

    #for rank in range(size):
    t1 = chunk*rank
    t2 = chunk*(rank+1)-1
    t2 = ntimes-1 if t2>ntimes-1 else t2

    if t1<ntimes:
        tg1 = alltimes[t1].strftime('%Hz%d%b%Y')
        tg2 = alltimes[t2].strftime('%Hz%d%b%Y')
        #print('Rank = %d, t1 = %d, t2 = %d, time1 = %s, time2 = %s' % (rank, t1, t2, tg1, tg2))

        if prodtype == 'nrt':
            
            last_stg4 = find_last_time(stg4_path+'/20??/ST4.20??????', 'ST4.%Y%m%d')
            last_nld2 = find_last_time(nld2_path+'/202?/???/*.nc', 'NLDAS_FORA0125_H.A%Y%m%d.%H00.020.nc')
        
            arg3 = 'realtime' if alltimes[t2]>last_stg4 else 'archive'
            arg4 = 'hrrr'     if alltimes[t2]>last_nld2 else 'nldas2'

            cmd = f'opengrads -lbc "../../scripts/forcing/comb_nwm_0.01deg_{prodtype}.gs {tg1} {tg2} {arg3} {arg4}"'
            
        else:
            
            # find the last PRISM recent file
            ncfiles = glob(f'{prsm_path}/PRISM_tmean_stable_4kmD2_*.nc')
            ncfiles.sort()
            f = nc.Dataset(ncfiles[-1], 'r')
            last_prsm = datetime.strptime(str(nc.num2date(f['time'][-1], f['time'].units)), '%Y-%m-%d %H:%M:%S')
            last_prsm = last_prsm.replace(tzinfo=pytz.utc)
            f.close()

            if alltimes[t1].year<1981:
                arg3 = 'historical'
            elif alltimes[t1]<last_prsm-timedelta(days=1):
                arg3 = 'recent'
            else:
                arg3 = 'provisional'
                
            cmd = f'opengrads -lbc "../../scripts/forcing/comb_nwm_0.01deg_{prodtype}.gs {tg1} {tg2} {arg3}"'
            
        print(cmd); os.system(cmd)

    comm.Barrier()

    return 0


if __name__ == '__main__':
    main(sys.argv[1:])

