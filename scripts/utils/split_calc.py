import os, sys, math
from mpi4py import MPI

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

v = sys.argv[1]
p = int(sys.argv[2])

#v='STREAMFLOW'
#v = 'SMTOT_SWE'
retro = '1979-2023'

if v == 'STREAMFLOW':
    dim = 'feature_id'
    nf = 2776738
elif v == 'SMTOT_SWE':
    dim = 'y'
    nf = 1320

ns = 100

step = math.ceil(nf/ns)
alls = list(range(0, nf, step))

for s,i1 in enumerate(alls):
    if s%size==rank:
        i2 = i1 + step - 1
        if i2>=nf:
            i2 = nf-1
        print(rank, s, i1, i2)

        if p==-1:
            cmd = f'ncks -d {dim},{i1},{i2} {retro}.{v} split/{retro}.{v}.s{s:02d}'
            print(cmd); os.system(cmd)
            for o in ['ydrunmin', 'ydrunmax']:
                cmd = f'cdo -O {o},31 split/{retro}.{v}.s{s:02d} split/{retro}.{v}.{o}.s{s:02d}' 
                print(cmd); os.system(cmd)
        else:
            cmd = f'cdo -O ydrunpctl,{p+0.5:.1f},31 split/{retro}.{v}.s{s:02d} split/{retro}.{v}.ydrunmin.s{s:02d} split/{retro}.{v}.ydrunmax.s{s:02d} split/{retro}.{v}.ydrunpctl.{p:02d}.s{s:02d}' 
        print(cmd)
        os.system(cmd)

comm.Barrier()
