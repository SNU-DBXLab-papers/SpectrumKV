#!/bin/bash

set -euo pipefail

#echo "[INIT] Preparing devices..."
#rm -rf /mnt/pmem1/ckpt_log /mnt/pmem1/pmem*

echo "[START] SpectrumKV workloads ..."
workloads=("d")
distributions=("zipf" "unif")
threads=("16" "32" "8" "4" "1" )

#mkdir -p results/raw
#mkdir -p results/processed

#echo "[CLEAN] Removing old results..."
#rm -rf results/raw/*
#rm -rf results/processed/*

	#outfile="results/processed/load_zipf.txt"
	#> "$outfile"
	#for th in "${threads[@]}"; do
    #    echo "[RUN] LOAD, Dist=zipf, Threads=$th"

#        #save output
#    	fname="result_load_zipf_${th}.dat"
#        rm -rf /mnt/pmem1/ckpt_log /mnt/pmem1/pmem*
#    	numactl --cpunodebind=1 --membind=1 ./project a zipf $th --insert-only > results/raw/$fname

#	    #parse throughput from output
#	    tp=$(grep "YCSB_INSERT throughput" "results/raw/$fname" | tail -n 1 | awk '{print $3}' | cut -d. -f1)
#    	echo "$th    $tp" >> $outfile
#    done

for dist in "${distributions[@]}"; do
    #echo "[INIT] Preparing "${dist}" workload..."
 #cp ./workloads/100M_"${dist}"/* ./workloads/

	#outfile="results/processed/load_${dist}.txt"
	#> "$outfile"
	#for th in "${threads[@]}"; do
    #    echo "[RUN] LOAD, Dist=$dist, Threads=$th"

        #save output
    #	fname="result_load_${dist}_${th}.dat"
    #    rm -rf /mnt/pmem0/ckpt_log /mnt/pmem0/pmem*
   # 	numactl --cpunodebind=0 --membind=0 ./project a $dist $th --insert-only > results/raw/$fname

	#    #parse throughput from output
	 #   tp=$(grep "YCSB_INSERT throughput" "results/raw/$fname" | tail -n 1 | awk '{print $3}' | cut -d. -f1)
    #	echo "$th    $tp" >> $outfile
   # done

    for workload in "${workloads[@]}"; do
#		rm -rf /mnt/pmem0/ckpt_log /mnt/pmem0/pmem*
#		outfile="results/processed/${workload}_${dist}.txt"
#		> "$outfile"
		for th in "${threads[@]}"; do
			#rm -rf /mnt/pmem0/ckpt_log /mnt/pmem0/pmem*
			echo "[RUN] workload=$workload, Dist=$dist, Threads=$th"

			#save output
			fname="result_${workload}_${dist}_${th}.dat"
            #rm -rf /mnt/pmem0/ckpt_log /mnt/pmem0/pmem*
			numactl --cpunodebind=0 --membind=0 ./project $workload $dist $th > results/raw/$fname

			#parse throughput from output
#			workload_upper=$(echo "$workload" | tr '[:lower:]' '[:upper:]')
#			tp=$(grep "YCSB_${workload_upper} throughput" "results/raw/$fname" | tail -n 1 | awk '{print $3}' | cut -d. -f1)
#			echo "$th    $tp" >> $outfile
		done
	done
done

# TODO
# gnuplot and generate pdf

echo "[✓] COMPLETED - SpectrumKV results in results/raw and results/processed"
