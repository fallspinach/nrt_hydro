#!/bin/bash
# Shell script to mimic the function of "cron" to initiate scheduled tasks as the cron tool is unavailable on a cluster
# Usage: ./fake_cron.sh
#
# author: Ming Pan
# email: m3pan@ucsd.edu
# status: Development

part_shared=shared-128
hinterval=4
hoffset=0

modelid=nwm_v3
#nrt_domains="cnrfc basins24 conus yampa"
nrt_domains="cnrfc"
#fcst_domains="cnrfc"
fcst_domains="cnrfc"

cd $(realpath $(dirname $0))/../

#sleep 60m

while true; do
    currhour=$(date -u +%k)
    
    if [ $currhour == 2 ]; then
    
        # update NRT forcing
        flog=forcing/log/update_nrt_`date -u +\%Y\%m\%d_\%H`z.txt
        sbatch -t 2:00:00 --nodes=1 --ntasks-per-node=1 -p $part_shared -J forcnrt -o $flog --wrap="python scripts/forcing/update_conus_forcing_nrt.py"
        
        # collect MODIS SCA
        #flog=obs/log/log_modis_$(date -u +%Y%m%d_%H)z.txt
        #sbatch -t 2:00:00 --nodes=1 --ntasks-per-node=1 --mem=20G -p $part_shared -J modissca -o $flog --wrap="python scripts/obs/process_modis_sca.py"
        
        # collect FNF data from CDEC
        python scripts/obs/process_cdec_fnf.py
        flog=obs/log/log_snow_$(date -u +%Y%m%d_%H)z.txt
        sbatch -t 2:00:00 --nodes=1 --ntasks-per-node=1 -p $part_shared -J snow -o $flog --wrap="python scripts/obs/process_cdec_snow.py"
        flog=obs/log/log_usgs_$(date -u +%Y%m%d_%H)z.txt
        sbatch -t 6:00:00 --nodes=1 --ntasks-per-node=1 -p $part_shared -J usgs -o $flog --wrap="python scripts/obs/process_usgs_streamflow.py"
        
        sleep 60m
    
    elif  [ $currhour == 3 ]; then
    
        # run WRF-Hydro NRT
        for domain in $nrt_domains; do
            flog=$modelid/$domain/nrt/run/log/log_nrt_$(date -u +%Y%m%d_%H)z.txt
            sbatch -t 00:10:00 --nodes=1 --ntasks-per-node=1 -p $part_shared -J nrt$domain -o $flog --wrap="python scripts/$modelid/run_nrt.py $domain"
        done
        sleep 40m

        # update system status
        flog=$modelid/$domain/nrt/run/log/log_status_$(date -u +%Y%m%d_%H)z.txt
        sbatch -t 00:30:00 --nodes=1 --ntasks-per-node=1 -p $part_shared -J chkstatus -o $flog --wrap="python scripts/$modelid/check_status.py update_gcloud"
        sleep 22m

    elif [ $currhour == 5 ]; then

        # update GFS deterministic forecast forcing
        # run WRF-Hydro + GFS deterministic forecast
        wfcst="gfs"
        
        flog=forcing/log/update_${wfcst}_`date -u +\%Y\%m\%d_\%H`z.txt
        sbatch -t 00:40:00 --nodes=1 --ntasks-per-node=1 -p $part_shared -J ${wfcst}forc -o $flog --wrap="python scripts/forcing/process_${wfcst}.py"
        sleep 40m

        for domain in $fcst_domains; do
            flog=$modelid/$domain/fcst/$wfcst/run/log/log_${wfcst}_$(date -u +%Y%m%d_%H)z.txt
            sbatch -t 00:10:00 --nodes=1 --ntasks-per-node=1 -p $part_shared -J $wfcst$domain -o $flog --wrap="python scripts/$modelid/run_fcst_det.py $domain $wfcst"
        done
        sleep 30m
        
        # update system status
        flog=$modelid/$domain/nrt/run/log/log_status_$(date -u +%Y%m%d_%H)z.txt
        sbatch -t 00:30:00 --nodes=1 --ntasks-per-node=1 -p $part_shared -J chkstatus -o $flog --wrap="python scripts/$modelid/check_status.py update_gcloud"
        
    elif  [ $currhour == 13 ]; then
    
        # update WWRF deterministic (ECMWF) forecast forcing
        # run WRF-Hydro + WWRF deterministic forecast
        wfcst="wwrf"
        
        flog=forcing/log/update_${wfcst}_`date -u +\%Y\%m\%d_\%H`z.txt
        sbatch -t 00:10:00 --nodes=1 --ntasks-per-node=1 -p $part_shared -J ${wfcst}forc -o $flog --wrap="python scripts/forcing/process_${wfcst}.py"
        sleep 30m
        
        for domain in $fcst_domains; do
            flog=$modelid/$domain/fcst/$wfcst/run/log/log_${wfcst}_$(date -u +%Y%m%d_%H)z.txt
            sbatch -t 00:10:00 --nodes=1 --ntasks-per-node=1 -p $part_shared -J $wfcst$domain -o $flog --wrap="python scripts/$modelid/run_fcst_det.py $domain $wfcst"
        done
        sleep 40m
        
        # update WWRF ensemble forecast forcing between September and April
        if [ `date -u +%m` -le "04" ] || [ `date -u +%m` -ge "09" ]; then
            flog=forcing/log/update_wwrfens_`date -u +\%Y\%m\%d_\%Hz`.txt
            #sbatch -t 04:00:00 --nodes=1 --ntasks-per-node=12 -p $part_shared -J wwrfens -o $flog --wrap="mpirun -np 12 python scripts/forcing/process_wwrf_ens.py"
        fi
        
        # update system status
        flog=$modelid/$domain/nrt/run/log/log_status_$(date -u +%Y%m%d_%H)z.txt
        sbatch -t 00:30:00 --nodes=1 --ntasks-per-node=1 -p $part_shared -J chkstatus -o $flog --wrap="python scripts/$modelid/check_status.py update_gcloud"

    else
        sleep 10m
    fi
done



