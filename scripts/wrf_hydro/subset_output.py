''' Subset WRF-Hydro results over a smaller subdomain from a larger domain

Usage:
    mpirun -np [# of procs] python subset_output.py [subdomain] [domain] [yyyymm1] [yyyymm2] [retro|nrt]
Default values:
    must specify all
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, pytz
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from mpi4py import MPI

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

## main function
def main(argv):
    
    '''main loop'''

    subdomain = argv[0]
    domain    = argv[1]
    
    time1 = datetime.strptime(argv[2], '%Y%m')
    time2 = datetime.strptime(argv[3], '%Y%m')

    ptype = argv[4]
    
    time1 = time1.replace(tzinfo=pytz.utc)
    time2 = time2.replace(tzinfo=pytz.utc)
    step  = relativedelta(months=1)

    workdir = f'{config["base_dir"]}/wrf_hydro'
    os.chdir(workdir)
   
    if len(argv)>3: 
        monthly_flag = True
    else:
        monthly_flag = False
    
    alltimes = []
    t = time1
    while t <= time2:
        alltimes.append(t)
        t += step

    for t in alltimes[rank::size]:
        
        if ptype=='nrt':
            fdin  =    f'{domain}/{ptype}/output/1km_daily/{t:%Y%m}.LDASOUT_DOMAIN1'
            fdout = f'{subdomain}/{ptype}/output/1km_daily/{t:%Y%m}.LDASOUT_DOMAIN1'
        else:
            fdin  =    f'{domain}/{ptype}/output/1km_daily/{t:%Y/%Y%m}.LDASOUT_DOMAIN1'
            fdout = f'{subdomain}/{ptype}/output/1km_daily/{t:%Y/%Y%m}.LDASOUT_DOMAIN1'
        
        fmin  =    f'{domain}/{ptype}/output/1km_monthly/{t:%Y%m}.LDASOUT_DOMAIN1.monthly'
        fmout = f'{subdomain}/{ptype}/output/1km_monthly/{t:%Y%m}.LDASOUT_DOMAIN1.monthly'

        for f in [fdout, fmout]:
            dout = os.path.dirname(f)
            if not os.path.isdir(dout):
                os.system(f'mkdir -p {dout}')

        with open(f'{config["base_dir"]}/forcing/nwm/domain/cdo_indexbox_{subdomain}.txt', 'r') as f:
            indexbox = f.read().rstrip()
        fmask = f'{config["base_dir"]}/forcing/nwm/domain/xmask0_{subdomain}.nc'
        
        cdocmd = f'cdo -f nc4 -z zip add -selindexbox,{indexbox}'

        cmd = f'{cdocmd} {fdin} {fmask} {fdout}'
        print(cmd); os.system(cmd)
        
        cmd = f'{cdocmd} {fmin} {fmask} {fmout}'
        print(cmd); os.system(cmd)
        
    comm.Barrier()
    return 0


if __name__ == '__main__':
    main(sys.argv[1:])

