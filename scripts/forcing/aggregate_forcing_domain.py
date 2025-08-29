''' Aggregate hourly forcing to daily and monthly for a domain only

Usage:
    mpirun -np [# of procs] python aggregate_forcing_domain.py [domain] [yyyymm1] [yyyymm2] [retro|nrt]
Default values:
    must specify all
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Prototype'

import sys, os, pytz, time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from calendar import monthrange
from mpi4py import MPI
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/wrf_hydro')
from utilities import config, find_last_time
import add_pctl_rank_monthly

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

## main function
def main(argv):

    '''main loop'''

    domain = argv[0]
    t1 = datetime.strptime(argv[1], '%Y%m')
    t2 = datetime.strptime(argv[2], '%Y%m')
    ptype = argv[3]
    
    workdir = f'{config["base_dir"]}/forcing/nwm'
    os.chdir(workdir)

    step  = relativedelta(months=1)
    alltimes = []
    t = t1
    while t <= t2:
        alltimes.append(t)
        t += step
    
    for t in alltimes[rank::size]:
        
        md = monthrange(t.year, t.month)[1]
        tn = t + step
        if os.path.isfile(f'1km_hourly/{domain}/{ptype}/{tn:%Y/%Y%m}01.LDASIN_DOMAIN1'):
            cmd = f'cdo -O -f nc4 -z zip delete,timestep=1,{md+2} -daymean -shifttime,-1hour [ -mergetime 1km_hourly/{domain}/{ptype}/{t:%Y/%Y%m}??.LDASIN_DOMAIN1 1km_hourly/{domain}/{ptype}/{tn:%Y/%Y%m}01.LDASIN_DOMAIN1 ] 1km_daily/{domain}/{ptype}/{t:%Y/%Y%m}.LDASIN_DOMAIN1.daily'
        else:
            cmd = f'cdo -O -f nc4 -z zip delete,timestep=1,{md+2} -daymean -shifttime,-1hour [ -mergetime 1km_hourly/{domain}/{ptype}/{t:%Y/%Y%m}??.LDASIN_DOMAIN1 ] 1km_daily/{domain}/{ptype}/{t:%Y/%Y%m}.LDASIN_DOMAIN1.daily'
        print(cmd); os.system(cmd)
        cmd = f'cdo -O -f nc4 -z zip monmean 1km_daily/{domain}/{ptype}/{t:%Y/%Y%m}.LDASIN_DOMAIN1.daily 1km_monthly/{domain}/{ptype}/{t:%Y/%Y%m}.LDASIN_DOMAIN1.monthly'
        print(cmd); os.system(cmd)
        if ptype=='nrt':
            add_pctl_rank_monthly.main([domain, f'1km_monthly/{domain}/{ptype}/{t:%Y/%Y%m}.LDASIN_DOMAIN1.monthly'])
        
        if domain=='cnrfc':
            subdomains = ['basins24']
        elif domain=='cbrfc':
            subdomains = ['yampa']
            
        for subdomain in subdomains:
            findex = f'domain/cdo_indexbox_{subdomain}.txt'
            fmask  = f'domain/xmask0_{subdomain}.nc'
            with open(findex, 'r') as f:
                indexbox = f.read().rstrip()
            cdocmd = f'cdo -f nc4 -z zip add -selindexbox,{indexbox}'
            for freq in ['daily', 'monthly']:
                fsrc = f'1km_{freq}/{domain}/{ptype}/{t:%Y/%Y%m}.LDASIN_DOMAIN1.{freq}'
                fout = f'1km_{freq}/{subdomain}/{ptype}/{t:%Y/%Y%m}.LDASIN_DOMAIN1.{freq}'
                cmd = f'{cdocmd} {fsrc} {fmask} {fout}'
                print(cmd); os.system(cmd)

    comm.Barrier()        

    return 0


if __name__ == '__main__':
    main(sys.argv[1:])
