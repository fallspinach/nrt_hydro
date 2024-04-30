''' Merge per-hour lat/lon forcing data into per-day, reproject to NWM grid, and subset it for domains of interest

Usage:
    python mergetime_subset.py [yyyymmdd1] [yyyymmdd2]
Default values:
    must specify all
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, math, pytz, yaml
from datetime import datetime, timedelta
from mpi4py import MPI
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

## some setups
workdir   = f'{config["base_dir"]}/forcing/nwm'

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

## main function
def main(argv):
    
    '''main loop'''

    for key,value in os.environ.items():
        if 'SLURM' in key:
            print(key, '=', value)
    
    os.chdir(workdir)

    time1 = datetime.strptime(argv[0], '%Y%m%d')
    time2 = datetime.strptime(argv[1], '%Y%m%d')
    time1 = time1.replace(tzinfo=pytz.utc)
    time2 = time2.replace(tzinfo=pytz.utc)
    prodtype = argv[2]
    
    step  = timedelta(days=1)

    alltimes = []
    t = time1
    while t <= time2:
        alltimes.append(t)
        t += step

    for t in alltimes[rank::size]:

        fsrc = f'0.01deg/{t:%Y/%Y%m/%Y%m%d}??.LDASIN_DOMAIN1'
        fout = f'0.01deg/{t:%Y/%Y%m/%Y%m%d}.LDASIN_DOMAIN1'
        if prodtype == 'nrt':
            cmd = f'cdo -O -f nc4 -z zip mergetime {fsrc} {fout}'
        else:
            cmd = f'cdo -O -f nc4 -z zip mergetime {fsrc} {fout}; /bin/rm -f {fsrc}'
        print(cmd); os.system(cmd)

        fsrc = fout
        cdocmd = 'cdo -f nc4 -z zip remap,domain/scrip_conus_bilinear.nc,domain/cdo_weights_conus.nc'
        fout = f'1km/conus/{prodtype}/{t:%Y/%Y%m%d}.LDASIN_DOMAIN1'
        dout = os.path.dirname(fout)
        if not os.path.isdir(dout):
            os.system(f'mkdir -p {dout}')
        if prodtype == 'nrt':
            cmd = f'{cdocmd} {fsrc} {fout}' 
        else:
            cmd = f'{cdocmd} {fsrc} {fout}; /bin/rm -f {fsrc}'
            #cmd = f'{cdocmd} {fsrc} {fout}' 
        print(cmd); os.system(cmd)

        fconus = fout
        for domain in config['forcing']['domains']:

            if domain=='basins24':
                fsrc = f'1km/cnrfc/{prodtype}/{t:%Y/%Y%m%d}.LDASIN_DOMAIN1'
            else:
                fsrc = fconus
            
            with open(f'domain/cdo_indexbox_{domain}.txt', 'r') as f:
                indexbox = f.read().rstrip()
            cdocmd = f'cdo -f nc4 -z zip add -selindexbox,{indexbox}'
            
            fout = f'1km/{domain}/{prodtype}/{t:%Y/%Y%m%d}.LDASIN_DOMAIN1'
            dout = os.path.dirname(fout)
            if not os.path.isdir(dout):
                os.system(f'mkdir -p {dout}')
                
            cmd = f'{cdocmd} {fsrc} domain/xmask0_{domain}.nc {fout}'
            print(cmd); os.system(cmd)

    comm.Barrier()

    return 0


if __name__ == '__main__':
    main(sys.argv[1:])

