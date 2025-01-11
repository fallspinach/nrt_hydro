#!/bin/bash

###### merge esp fcst to LSTM inputs
echo "step 1...."
./s5.p0.merge.ens.sh


###### generate LSTM flow results
echo "step 2...."
    #for ens in {01..45}
    for ens in $(seq -f '%02g' 1 45)
    do
        echo $ens
        python s5.p1.predict.py ./inputs.lstm/configs.81-20/general/config.$ens
    done

###### merge all flow output into final csv files
echo "step 3...."
    python s5.p2.merge.py.no.init.flow 2025 0101 01

