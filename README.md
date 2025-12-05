# SpectrumKV: Workload-Adaptive Indexing for Key-Value Stores on Heterogeneous Memory Systems
**SpectrumKV** is a write-efficient, recovery aware indexing framework designed for key-value (KV) stores running on heterogeneous memory systems, particuliarly those incorporating non-volatile memory(NVM).
Traditional systems often treat index and data updates equally, resulting in frequent small writes on the critical path that degrade performacne. **SpectrumKV** addresses this by decoupling indexing from data persistence through two core ideas:
1. Adaptive structure modification operations (SMOs):
    - Deferred SMOs delaying rebalancing and persistence of structural changes to reduce write amplification and interference with foreground operations.
    - Speculative SMOs proactively allocates routing capacity under skewed insertions to avoid hotspots.
3. Dual-layer, asymmetric indexing:
   - A fast, DRAM-resident main index handles query processing.
   - A crash-consistent shadow index in NVM uaranteed safe recovery without requiring synchronous writes.

Across diverse workloads, **SpectrumKV** significantly improves write throughput, reduces persistence overhead, and enables instant recovery.

## Repository Structure
```
.
├── include/        # Public headers for SpectrumKV data structures and APIs
├── src/            # Core implementation of the SpectrumKV indexing framework
├── test/           # Unit tests, microbenchmarks, and validation workloads
├── Makefile        # Build rules and compilation targets
└── run_spect.sh    # Benchmark running and cleanup
```

## Building SpectrumKV
### Requirements and dependencies
- C++17 or newer  
- GCC or Clang  
- PMDK (libpmemobj)

SpectrumKV is configures for persistent memory. A configured PM path is necessary; skip if you have a configured path.
```
# set Optane DCPMM to AppDirect mode
sudo ipmctl create -f -goal persistentmemorytype=appdirect
# configure PM device to fsdax mode
sudo ndctl create-namespace -m fsdax
# create and mount a file system with DAX
sudo mkfs.ext4 -f /dev/pmem0
sudo mount -o dax /dev/pmem0 /mnt/pmem0
```

  
### Build Instructions
```
git clone https://github.com/SNU-DBXLab-papers/SpectrumKV.git
cd SpectrumKV
make -j$(nproc)
```
a benchmarking binary will be generated `./project`

## Running YCSB
```
./run_spect.sh [workload] [distribution] [thread_num] # --insert-only for exit after LOAD BULK
```
Parameters:
```
-workload
  a #(50% Read, 50% Update)
  b #(95% Read, 5% Update)
  c #(100% Read)
  d #(95% Read, 5% Lastest Update)
  e #(95% Scan, 5% Update)
  g #(99% Read,1% Insert) custom for recovery testing (index convergence)
-distribution
  unif
  zipf
-thread_num
```
## For comparison with baselines
Please see 
