import sys, os, pytz, time
import netCDF4 as nc
from glob import glob
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

from mpi4py import MPI
#import add_pctl_rank_daily, add_pctl_rank_monthly

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

flag_deldaily = True

## main function
def main(argv):

    '''main loop'''

    domain = argv[0]
    t1 = datetime.strptime(argv[1], '%Y%m')
    t2 = datetime.strptime(argv[2], '%Y%m')
    step  = timedelta(days=1)

    workdir   = f'{config["base_dir"]}/wrf_hydro/{domain}/retro/output/1km_daily'
    os.chdir(workdir)

    alltimes = []
    t = t1
    while t <= t2:
        alltimes.append(t)
        t += relativedelta(months=1)

    for m in alltimes[rank::size]:

        fout = f'{m:%Y/%Y%m}.LDASOUT_DOMAIN1'
        fin  = []
        for d in range(1, 32):
            dd = m+timedelta(days=d)
            fin.append(f'{m:%Y}/{dd:%Y%m%d%H}.LDASOUT_DOMAIN1')
            if dd==m+relativedelta(months=1)-timedelta(days=m.day-1):
                break

        cmd = f'cdo -O -f nc4 -z zip shifttime,-12hour -mergetime {" ".join(fin)} {fout}'
        print(cmd); os.system(cmd)
        cmd = f'ncks -4 -L 5 {fout} {fout}.nc4; /bin/mv {fout}.nc4 {fout}'
        print(cmd); os.system(cmd)
        if flag_deldaily:
            os.system(f'rm -f {" ".join(fin)}')
        #add_pctl_rank_daily.main([fout])
        
        fmout = f'../1km_monthly/{m:%Y%m}.LDASOUT_DOMAIN1.monthly'
        cmd = f'cdo -O -f nc4 -z zip monmean {fout} {fmout}'
        print(cmd); os.system(cmd)
        cmd = f'ncks -4 -L 5 {fmout} {fmout}.nc4; /bin/mv {fmout}.nc4 {fmout}'
        print(cmd); os.system(cmd)
        #add_pctl_rank_monthly.main([fmout])

        for rout in ['CHRTOUT_DOMAIN1', 'LAKEOUT_DOMAIN1']:

            tofix = ['streamflow', 'q_lateral', 'velocity', 'qSfcLatRunoff', 'qBucket', 'qBtmVertRunoff',
                     'reservoir_assimilated_value', 'water_sfc_elev', 'inflow', 'outflow']

            fndst = f'{m:%Y/%Y%m}.{rout}'
            dst = nc.Dataset(fndst, 'w')
            fnsrc = f'{(m+timedelta(days=1)):%Y/%Y%m%d%H}.{rout}'
            src = nc.Dataset(fnsrc, 'r')

            # copy global attributes all at once via dictionary
            dst.setncatts(src.__dict__)
            # copy dimensions
            for name, dimension in src.dimensions.items():
                dst.createDimension(
                    name, (len(dimension) if not dimension.isunlimited() else None))
            # copy all file data except for the excluded
            for name, variable in src.variables.items():
                if name not in tofix:
                    x = dst.createVariable(name, variable.datatype, variable.dimensions, zlib=True)
                    dst[name].setncatts(src[name].__dict__)
                    dst[name][:] = src[name][:]
                else:
                    x = dst.createVariable(name, variable.datatype, ('time', 'feature_id',), zlib=True)
                    dst[name].setncatts(src[name].__dict__)
                    dst[name][0,:] = src[name][:]
            dst['time'][0] = src['time'][0] - 720
            src.close()
            if flag_deldaily:
                os.system(f'rm -f {fnsrc}')

            for d in range(2, 32):
                dd = m+timedelta(days=d)
                fnsrc = f'{m:%Y}/{dd:%Y%m%d%H}.{rout}'
                src = nc.Dataset(fnsrc, 'r')
                dst['time'][d-1] = src['time'][0] - 720
                for name, variable in src.variables.items():
                    if name in tofix:
                        dst[name][d-1,:] = src[name][:]
                src.close()
                if flag_deldaily:
                    os.system(f'rm -f {fnsrc}')
                if dd==m+relativedelta(months=1)-timedelta(days=m.day-1):
                    break

            dst.close()
            #add_pctl_rank_daily.main([fndst])

            # caculate monthly
            fout  = f'{m:%Y/%Y%m}.{rout}'
            fmout = f'../1km_monthly/{m:%Y%m}.{rout}.monthly'
            cmd = f'cdo -f nc4 -z zip monmean {fout} {fmout}'
            print(cmd); os.system(cmd)
            cmd = f'ncks -4 -L 5 {fmout} {fmout}.nc4; /bin/mv {fmout}.nc4 {fmout}'
            print(cmd); os.system(cmd)
            #add_pctl_rank_monthly.main([fmout])

    return 0

if __name__ == '__main__':
    main(sys.argv[1:])

        
