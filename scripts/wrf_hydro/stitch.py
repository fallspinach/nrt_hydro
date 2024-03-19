import netCDF4 as nc
import os, sys
from mpi4py import MPI
#import add_pctl_rank_daily, add_pctl_rank_monthly

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

retro = '1979-2023'

def main(argv):
    v = argv[0]
    p1 = int(argv[1])
    p2 = int(argv[2])

    if 'STREAMFLOW' in argv[0]:
        ns = 100
    else:
        ns = 95

    for p in range(p1, p2+1)[rank::size]:
        fins = [f'split/{retro}.{v}.ydrunpctl.{p:02d}.s{s:02d}' for s in range(ns)]
        fout = [f'split/{retro}.{v}.ydrunpctl.{p:02d}']
        stitch(fins+fout)
        
    comm.Barrier()
    return 0

def stitch(argv):

    if 'STREAMFLOW' in argv[0]:
        dimname = 'feature_id'
        vs      = ['streamflow']
    else:
        dimname = 'y'
        vs      = ['SOIL_M', 'SNEQV']

    fout = nc.Dataset(argv[-1], 'w')
    fsin = []

    sumlen = 0
    for i in range(len(argv)-1):
        fin = nc.Dataset(argv[i], 'r')
        fsin.append(fin)
        sumlen += len(fin.dimensions[dimname])
    
    print('Total dim length is %d.' % sumlen)

    pos = 0
    fin = fsin[0]

    # copy global attributes all at once via dictionary
    fout.setncatts(fin.__dict__)
    # copy dimensions
    for name, dimension in fin.dimensions.items():
        if name != dimname:
            fout.createDimension(name, (len(dimension) if not dimension.isunlimited() else None))
        else:
            fout.createDimension(name, sumlen)
    # copy all file data except for the excluded
    for name, variable in fin.variables.items():
        if name not in vs and name != dimname:
            x = fout.createVariable(name, variable.datatype, variable.dimensions)
            fout[name].setncatts(fin[name].__dict__)
            fout[name][:] = fin[name][:]
        else:
            if name == dimname:
                x = fout.createVariable(name, variable.datatype, variable.dimensions)
                fout[name].setncatts(fin[name].__dict__)
                n = fin[name].shape[0]
                fout[name][pos:pos+n] = fin[name][:]
            else:
                x = fout.createVariable(name, variable.datatype, variable.dimensions, zlib=True)
                fout[name].setncatts(fin[name].__dict__)
                n = fin[name].shape[1]
                fout[name][:,pos:pos+n] = fin[name][:]
    pos += n
    fin.close()
    
    for fin in fsin[1:]:
        print(fin.history.split('\n')[1].split(' ')[-1])
        n = fin[dimname].shape[0]
        fout[dimname][pos:pos+n] = fin[dimname][:]
        for name in vs:
            fout[name][:,pos:pos+n] = fin[name][:]
        pos += n
        fin.close()

    fout.close()

if __name__ == '__main__':
    main(sys.argv[1:])

