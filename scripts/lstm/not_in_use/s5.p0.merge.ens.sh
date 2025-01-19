#!/bin/bash

namls="./inputs.lstm/stn.names.24.txt"
cat $namls

hisd="./inputs.lstm/hist.dyn.input"
#fcsd="/expanse/nfs/cw3e/cwp101/nrt_hydro/wrf_hydro/basins24/fcst/esp_wwrf/output/init20241201_update20241222/basins/averaged/"
#fcsd="/expanse/nfs/cw3e/cwp101/nrt_hydro/wrf_hydro/basins24/fcst/esp_wwrf/output/init20250101_update20250101/basins/averaged/"
fcsd="./averaged/"
outd="./inputs.lstm/csv_inputs/"



#for ens in {01..45}
for ens in $(seq -f '%02g' 1 45)
do
	echo $ens
   while read line
   do
       id=`echo $line|awk -F, '{print $2}'`
       num=`echo $line|awk -F, '{print $1+1-1}'`
       cp   $hisd/$id.197910-202409.dyn.csv  $outd/$ens/$num.csv 
       awk '{if(NR>1) print $0",9999.0"}' $fcsd/$ens/${id}_monthly.csv  >>  $outd/$ens/$num.csv
   done < $namls
done

