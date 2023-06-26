import sys, os, math, pytz, yaml
from datetime import datetime, timedelta
from mpi4py import MPI
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

## some setups
workdir   = config['base_dir'] + '/forcing/nwm'

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

## main function
def main(argv):
    
    '''main loop'''
    
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

        fsrc = t.strftime('0.01deg/%Y/%Y%m/%Y%m%d??.LDASIN_DOMAIN1')
        fout = t.strftime('0.01deg/%Y/%Y%m/%Y%m%d.LDASIN_DOMAIN1')
        if prodtype == 'nrt':
            cmd = 'cdo -O -f nc4 -z zip mergetime %s %s' % (fsrc, fout)
        else:
            cmd = 'cdo -O -f nc4 -z zip mergetime %s %s; /bin/rm -f %s' % (fsrc, fout, fsrc)
        print(cmd); os.system(cmd)

        fsrc = fout
        cdocmd = 'cdo -f nc4 -z zip remap,domain/scrip_conus_bilinear.nc,domain/cdo_weights_conus.nc'
        fout = '1km/conus/%s/%s' % (prodtype, t.strftime('%Y/%Y%m%d.LDASIN_DOMAIN1'))
        dout = os.path.dirname(fout)
        if not os.path.isdir(dout):
            os.system('mkdir -p '+dout)
        if prodtype == 'nrt':
            cmd = '%s %s %s' % (cdocmd, fsrc, fout)
        else:
            cmd = '%s %s %s; /bin/rm -f %s' % (cdocmd, fsrc, fout, fsrc)
        print(cmd); os.system(cmd)

        fsrc = fout
        for region in config['forcing']['regions']:
            
            with open('domain/cdo_indexbox_%s.txt' % region, 'r') as f:
                indexbox = f.read().rstrip()
            cdocmd = 'cdo -f nc4 -z zip add -selindexbox,%s' % indexbox
            
            fout = '1km/%s/%s/%s' % (region, prodtype, t.strftime('%Y/%Y%m%d.LDASIN_DOMAIN1'))
            dout = os.path.dirname(fout)
            if not os.path.isdir(dout):
                os.system('mkdir -p '+dout)
                
            cmd = '%s %s domain/xmask0_%s.nc %s' % (cdocmd, fsrc, region, fout)
            print(cmd); os.system(cmd)

    comm.Barrier()

    return 0


if __name__ == '__main__':
    main(sys.argv[1:])

