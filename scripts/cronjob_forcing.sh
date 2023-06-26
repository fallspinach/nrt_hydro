#!/bin/bash

cd /cw3e/mead/projects/cnt107/nrt_hydro/

# update NRT forcing
sbatch -t  2:00:00 -n 1 -p shared -J forcnrt -o forcing/log/update_nrt_`date -u +\%Y\%m\%d_\%Hz`.txt --wrap="python scripts/forcing/update_conus_forcing_nrt.py"

# update WWRF deterministic (ECMWF) forecast forcing
sbatch -t 00:40:00 -n 12 -p shared -J wwrfdet -o forcing/log/update_wwrf_`date -u +\%Y\%m\%d_\%Hz`.txt --wrap="mpirun -np 12 python scripts/forcing/process_wwrf.py"

# update WWRF ensemble forecast forcing between September and April
if [ `date -u +%m` -le "04" ] || [ `date -u +%m` -ge "09" ]; then 
    sbatch -t 04:00:00 -n 12 -p shared -J wwrfens -o forcing/log/update_wwrfens_`date -u +\%Y\%m\%d_\%Hz`.txt --wrap="mpirun -np 12 python scripts/forcing/process_wwrf_ens.py"
fi

