#!/bin/bash

tmpdir=/scratch/$USER/job_$SLURM_JOBID
cdo="cdo -s -O -f nc4 -z zip"

for y in `seq $1 $1`; do
  for m in `seq -w 1 12`; do
    tmp=$tmpdir/tmp$y$m.nc
    $cdo selname,SOIL_M,SNEQV ../$y/$y$m.LDASOUT_DOMAIN1 $tmp
    ncap2 -O -s 'thickness[soil_layers_stag]={0.1,0.3,0.6,1}' $tmp $tmp
    ncwa -a soil_layers_stag -w thickness $tmp $tmpdir/tmp$y$m.SMTOT_SWE
  done
  $cdo mergetime $tmpdir/tmp$y??.SMTOT_SWE extract/$y.SMTOT_SWE
  $cdo selname,streamflow -mergetime ../$y/$y??.CHRTOUT_DOMAIN1 extract/$y.STREAMFLOW
  rm -f $tmpdir/tmp$y*
done

