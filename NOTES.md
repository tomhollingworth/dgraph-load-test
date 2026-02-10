# Notes

## Observations

A couple of key points to note when trying to re-create the issue:
 - It does takes time to present itself in the order of 20-30 minutes - running for a short period (even with a higher load doesn't present the issue)
    - I run a scenario writing 1000msg/s up to 200k and it usually passes fine on just laptop hardware writing about 985 msg/s.
    - I've run it as slow as 30msg/s up to 1 million (9hr run) in server hardware and it will present (1 event in a million missing)
 - Has been re-produced on 4 different hardware configurations (1 server, 3 laptops/consumer desktops)
 - Better hardware seems to delay when it first occurs and the volume of missing events
 - Although the data appears missing in the aggregate counts, using an uid from another server to query the data, it does actually resolve

## Hardware Configurations

Different hardware configurations tried:
- Laptop (Windows)
    - 40GB DDR3 Memory (3200 MT/s) Dual Channel
    - NVME Samsung SSD 970 EVO Plus
    - AMD Ryzen 9 5900HS (16vCPU)
- Server (Linux/Ubuntu 24.02)
    - 32GB DDR4 ECC Memory (2667 MT/s) Dual Channel
    - INTEL SSDSC2KG96 (EXT4) 
    - INTEL SSDSC2KG96 (ZFS) 
    - Intel(R) Xeon(R) E-2286G CPU @ 4.00GHz (12vCPU)