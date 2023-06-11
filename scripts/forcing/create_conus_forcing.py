import sys, os, math, pytz, yaml, subprocess
from glob import glob
from datetime import datetime, timedelta
from mpi4py import MPI
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time


## some setups
workdir   = config['base_dir'] + '/forcing/nwm'
stg4_path = config['base_dir'] + '/forcing/stage4/archive' # path to Stage IV files
nld2_path = config['base_dir'] + '/forcing/nldas2/NLDAS_FORA0125_H.002' # path to NLDAS-2 archive folder

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
    #step  = timedelta(hours=int(argv[2]))
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

        last_stg4 = find_last_time(stg4_path+'/20????/ST4.20??????', 'ST4.%Y%m%d')
        last_nld2 = find_last_time(nld2_path+'/202?/???/*.nc', 'NLDAS_FORA0125_H.A%Y%m%d.%H00.002.nc')
        
        arg3 = 'realtime' if alltimes[t2]>last_stg4 else 'archive'
        arg4 = 'hrrr'     if alltimes[t2]>last_nld2 else 'nldas2'

        cmd = 'opengrads -lbc "../../scripts/forcing/comb_nwm_0.01deg_nrt.gs %s %s %s %s"' % (tg1, tg2, arg3, arg4)
        print(cmd); os.system(cmd)

    comm.Barrier()

    return 0


if __name__ == '__main__':
    main(sys.argv[1:])

