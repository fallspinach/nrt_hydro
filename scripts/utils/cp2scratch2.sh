#!/bin/bash
#SBATCH --job-name=transfer_data
#SBATCH --partition=cw3e-shared
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --time=12:00:00
#SBATCH --account=cwp101
# Define source and destination
SRC="/expanse/nfs/cw3e/cwp101/nrt_hydro/wrf_hydro/conus/"
DEST="/expanse/lustre/scratch/mpan/temp_project/wrf_hydro/conus/"
# Use rsync with SSH to copy everything from SRC to DEST
find $SRC -mindepth 1 -type d -printf "%P\n" | xargs -I% mkdir -p $DEST/%;
find $SRC ! -type d -printf "%P\n"| xargs -P32 -I% rsync -avih "$SRC/%" "$DEST/%";
rsync -avih $SRC $DEST
