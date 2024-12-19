''' Run a templated shell command for many time steps with MPI parallel processing

Usage:
    mpirun -np [num_of_procs] python run_cmd_in_ensemble_mpi.py [ens1] [ens2] ["cmd_template"]
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

## some setups
workdir   = '.'

## main function
def main(argv):

    '''main loop'''

    # MPI setup
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if len(argv)<3:
        print(f'Usage: mpirun -np [num_of_procs] python {os.path.basename(sys.argv[0])} [ens1] [ens2] ["cmd_template"]')
        sys.exit(0)
    else:
        ens1 = int(argv[0])
        ens2 = int(argv[1])
        cmd_tpls = argv[2:]

    for e in range(ens1, ens2+1)[rank::size]:
        for cmd_tpl in cmd_tpls:
            n = cmd_tpl.count('{')
            if n==0:
                cmd = cmd_tpl
            elif n==1:
                cmd = cmd_tpl.format(e)
            elif n==2:
                cmd = cmd_tpl.format(e, e)
            elif n==3:
                cmd = cmd_tpl.format(e, e, e)
            elif n==4:
                cmd = cmd_tpl.format(e, e, e, e)
            elif n==5:
                cmd = cmd_tpl.format(e, e, e, e, e)
            elif n==6:
                cmd = cmd_tpl.format(e, e, e, e, e, e)
            elif n==7:
                cmd = cmd_tpl.format(e, e, e, e, e, e, e)
            elif n==8:
                cmd = cmd_tpl.format(e, e, e, e, e, e, e, e)
            elif n==9:
                cmd = cmd_tpl.format(e, e, e, e, e, e, e, e, e)
            elif n==10:
                cmd = cmd_tpl.format(e, e, e, e, e, e, e, e, e, e)
            else:
                print('Too many occurrences of the ensemble number.')
                sys.exit(0)
            if cmd != '--dry-run':
                print(cmd)
            if argv[-1] != '--dry-run':
                os.system(cmd)

    comm.Barrier()

    return 0


if __name__ == '__main__':
    main(sys.argv[1:])
