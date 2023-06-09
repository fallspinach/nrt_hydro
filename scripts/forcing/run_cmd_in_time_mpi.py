import sys, os, pytz
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from mpi4py import MPI

## some setups
workdir   = '.'

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

## main function
def main(argv):

    '''main loop'''

    if argv[0] == 'hourly':
        step = timedelta(hours=1)
        time1 = datetime.strptime(argv[1], '%Y%m%d%H')
        time2 = datetime.strptime(argv[2], '%Y%m%d%H')
    elif argv[0] == 'daily':
        step = timedelta(days=1)
        time1 = datetime.strptime(argv[1], '%Y%m%d')
        time2 = datetime.strptime(argv[2], '%Y%m%d')
    elif argv[0] == 'monthly':
        step = relativedelta(months=1)
        time1 = datetime.strptime(argv[1], '%Y%m')
        time2 = datetime.strptime(argv[2], '%Y%m')
    elif argv[0] == 'yearly' or argv[0] == 'annual' or argv[0] == 'annually':
        step = relativedelta(years=1)
        time1 = datetime.strptime(argv[1], '%Y')
        time2 = datetime.strptime(argv[2], '%Y')
    else:
        print('Usage: %s [hourly|daily|monthly|yearly] [time_start] [time_end] [base_cmd] [cmd_args] ...' % sys.argv[0])
        sys.exit(1)
        
    basecmd = argv[3]
    cmdargs = argv[4]

    alltimes = []
    t = time1
    while t <= time2:
        alltimes.append(t)
        t += step

    for t in alltimes[rank::size]:
        targs = t.strftime(cmdargs)
        cmd = '%s %s' % (basecmd, targs)
        print(cmd)
        if len(argv)==5:
            os.system(cmd)

    comm.Barrier()

    return 0


if __name__ == '__main__':
    main(sys.argv[1:])
