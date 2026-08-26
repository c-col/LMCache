## 1. Sweep Matrix

### RAM, 4 shards

CSV,op,get_batch_mode,num_workers,num_keys,value_bytes,queue_ms,dispatch_ms,transfer_ms,handoff_ms,total_ms,gbps_mean,gbps_p50,inflight,agg_gbps

== op=get workers=64 keys=125 value=16.80MB batch=2.100GB mode=single min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.03ms | dispatch    30.80ms | transfer   125.36ms | handoff    0.17ms | total   156.36ms |  13.43 GB/s
  batch 1: queue     0.03ms | dispatch    23.95ms | transfer   140.34ms | handoff    0.16ms | total   164.48ms |  12.77 GB/s
  batch 2: queue     0.06ms | dispatch    44.73ms | transfer   106.59ms | handoff    0.05ms | total   151.44ms |  13.87 GB/s
  batch 3: queue     0.06ms | dispatch    36.28ms | transfer   128.10ms | handoff    0.15ms | total   164.60ms |  12.76 GB/s
  batch 4: queue     0.05ms | dispatch    25.86ms | transfer   131.09ms | handoff    0.20ms | total   157.19ms |  13.36 GB/s
  batch 5: queue     0.03ms | dispatch    25.65ms | transfer   133.99ms | handoff    0.27ms | total   159.95ms |  13.13 GB/s
  batch 6: queue     0.04ms | dispatch    22.75ms | transfer   135.44ms | handoff    0.16ms | total   158.39ms |  13.26 GB/s
  batch 7: queue     0.06ms | dispatch    47.31ms | transfer   110.20ms | handoff    0.17ms | total   157.74ms |  13.31 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.04       0.05       0.06
  dispatch      32.17      30.80      47.31
  transfer     126.39     131.09     140.34
  handoff        0.17       0.17       0.27
  total        158.77     158.39     164.60
  aggregate: 8 batches x 2.100GB in 1.27s = 13.22 GB/s wall-clock
CSV,get,single,64,125,16800000,0.044,32.167,126.389,0.166,158.767,13.236,13.313,1,13.22

== op=get workers=64 keys=250 value=8.40MB batch=2.100GB mode=single min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.04ms | dispatch    12.26ms | transfer   140.30ms | handoff    0.20ms | total   152.80ms |  13.74 GB/s
  batch 1: queue     0.04ms | dispatch    20.67ms | transfer   138.50ms | handoff    0.10ms | total   159.31ms |  13.18 GB/s
  batch 2: queue     0.06ms | dispatch    13.45ms | transfer   139.06ms | handoff    0.16ms | total   152.72ms |  13.75 GB/s
  batch 3: queue     0.03ms | dispatch    13.83ms | transfer   139.27ms | handoff    0.23ms | total   153.35ms |  13.69 GB/s
  batch 4: queue     0.08ms | dispatch    11.79ms | transfer   117.62ms | handoff    0.16ms | total   129.64ms |  16.20 GB/s
  batch 5: queue     0.02ms | dispatch    12.85ms | transfer   105.32ms | handoff    0.16ms | total   118.35ms |  17.74 GB/s
  batch 6: queue     0.04ms | dispatch    13.67ms | transfer   146.30ms | handoff    0.15ms | total   160.16ms |  13.11 GB/s
  batch 7: queue     0.05ms | dispatch    19.59ms | transfer   136.27ms | handoff    0.17ms | total   156.09ms |  13.45 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.04       0.04       0.08
  dispatch      14.76      13.67      20.67
  transfer     132.83     139.06     146.30
  handoff        0.17       0.16       0.23
  total        147.80     153.35     160.16
  aggregate: 8 batches x 2.100GB in 1.18s = 14.20 GB/s wall-clock
CSV,get,single,64,250,8400000,0.044,14.761,132.83,0.166,147.802,14.36,13.743,1,14.197

== op=get workers=64 keys=62 value=33.60MB batch=2.083GB mode=single min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.03ms | dispatch    48.90ms | transfer    80.89ms | handoff    0.16ms | total   129.98ms |  16.03 GB/s
  batch 1: queue     0.03ms | dispatch    64.79ms | transfer    71.80ms | handoff    0.15ms | total   136.77ms |  15.23 GB/s
  batch 2: queue     0.04ms | dispatch    53.98ms | transfer    87.59ms | handoff    0.24ms | total   141.85ms |  14.69 GB/s
  batch 3: queue     0.03ms | dispatch    53.17ms | transfer    64.53ms | handoff    0.07ms | total   117.80ms |  17.68 GB/s
  batch 4: queue     0.03ms | dispatch    65.84ms | transfer    82.77ms | handoff    0.14ms | total   148.78ms |  14.00 GB/s
  batch 5: queue     0.04ms | dispatch    55.42ms | transfer   109.57ms | handoff    0.16ms | total   165.18ms |  12.61 GB/s
  batch 6: queue     0.04ms | dispatch    47.45ms | transfer   100.82ms | handoff    0.06ms | total   148.37ms |  14.04 GB/s
  batch 7: queue     0.03ms | dispatch    40.54ms | transfer   115.15ms | handoff    0.06ms | total   155.78ms |  13.37 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.03       0.03       0.04
  dispatch      53.76      53.98      65.84
  transfer      89.14      87.59     115.15
  handoff        0.13       0.15       0.24
  total        143.06     148.37     165.18
  aggregate: 8 batches x 2.083GB in 1.14s = 14.56 GB/s wall-clock
CSV,get,single,64,62,33600000,0.033,53.761,89.14,0.128,143.063,14.707,14.686,1,14.556

== op=get workers=64 keys=500 value=4.20MB batch=2.100GB mode=single min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.05ms | dispatch     8.29ms | transfer   133.72ms | handoff    0.25ms | total   142.31ms |  14.76 GB/s
  batch 1: queue     0.03ms | dispatch     5.57ms | transfer   135.47ms | handoff    0.19ms | total   141.26ms |  14.87 GB/s
  batch 2: queue     0.04ms | dispatch     5.72ms | transfer   138.21ms | handoff    0.19ms | total   144.16ms |  14.57 GB/s
  batch 3: queue     0.03ms | dispatch    10.13ms | transfer   144.77ms | handoff    0.16ms | total   155.10ms |  13.54 GB/s
  batch 4: queue     0.03ms | dispatch     5.48ms | transfer   120.94ms | handoff    0.15ms | total   126.59ms |  16.59 GB/s
  batch 5: queue     0.03ms | dispatch     5.92ms | transfer   122.40ms | handoff    0.15ms | total   128.50ms |  16.34 GB/s
  batch 6: queue     0.03ms | dispatch     7.91ms | transfer   133.18ms | handoff    0.15ms | total   141.27ms |  14.87 GB/s
  batch 7: queue     0.05ms | dispatch     6.89ms | transfer   150.04ms | handoff    0.21ms | total   157.19ms |  13.36 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.04       0.03       0.05
  dispatch       6.99       6.89      10.13
  transfer     134.84     135.47     150.04
  handoff        0.18       0.19       0.25
  total        142.05     142.31     157.19
  aggregate: 8 batches x 2.100GB in 1.14s = 14.75 GB/s wall-clock
CSV,get,single,64,500,4200000,0.036,6.99,134.84,0.182,142.049,14.861,14.865,1,14.754

== op=get workers=64 keys=125 value=16.80MB batch=2.100GB mode=pipeline min_keys_per_tile=8 target_tile_mb=32 inflight=1
  batch 0: queue     0.03ms | dispatch     0.33ms | transfer   283.26ms | handoff    0.16ms | total   283.78ms |   7.40 GB/s
  batch 1: queue     0.17ms | dispatch     0.44ms | transfer   316.38ms | handoff    0.17ms | total   317.16ms |   6.62 GB/s
  batch 2: queue     0.03ms | dispatch     0.54ms | transfer   370.73ms | handoff    0.13ms | total   371.43ms |   5.65 GB/s
  batch 3: queue     0.03ms | dispatch     0.49ms | transfer   326.03ms | handoff    0.16ms | total   326.70ms |   6.43 GB/s
  batch 4: queue     0.04ms | dispatch     0.40ms | transfer   279.49ms | handoff    0.17ms | total   280.10ms |   7.50 GB/s
  batch 5: queue     0.12ms | dispatch     0.38ms | transfer   280.85ms | handoff    0.15ms | total   281.50ms |   7.46 GB/s
  batch 6: queue     0.03ms | dispatch     0.34ms | transfer   318.08ms | handoff    0.15ms | total   318.60ms |   6.59 GB/s
  batch 7: queue     0.04ms | dispatch     0.53ms | transfer   340.88ms | handoff    0.15ms | total   341.60ms |   6.15 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.06       0.04       0.17
  dispatch       0.43       0.44       0.54
  transfer     314.46     318.08     370.73
  handoff        0.15       0.16       0.17
  total        315.11     318.60     371.43
  aggregate: 8 batches x 2.100GB in 2.52s = 6.66 GB/s wall-clock
CSV,get,pipeline,64,125,16800000,0.061,0.431,314.463,0.155,315.11,6.725,6.621,1,6.663

== op=get workers=64 keys=250 value=8.40MB batch=2.100GB mode=pipeline min_keys_per_tile=8 target_tile_mb=32 inflight=1
  batch 0: queue     0.02ms | dispatch     0.50ms | transfer   219.89ms | handoff    0.20ms | total   220.62ms |   9.52 GB/s
  batch 1: queue     0.12ms | dispatch     0.79ms | transfer   223.35ms | handoff    0.15ms | total   224.40ms |   9.36 GB/s
  batch 2: queue     0.16ms | dispatch     0.36ms | transfer   216.87ms | handoff    0.07ms | total   217.46ms |   9.66 GB/s
  batch 3: queue     0.05ms | dispatch     0.41ms | transfer   226.01ms | handoff    0.14ms | total   226.61ms |   9.27 GB/s
  batch 4: queue     0.04ms | dispatch     0.57ms | transfer   213.23ms | handoff    0.16ms | total   214.00ms |   9.81 GB/s
  batch 5: queue     0.09ms | dispatch     0.48ms | transfer   216.04ms | handoff    0.15ms | total   216.76ms |   9.69 GB/s
  batch 6: queue     0.06ms | dispatch     0.39ms | transfer   226.57ms | handoff    0.15ms | total   227.16ms |   9.24 GB/s
  batch 7: queue     0.03ms | dispatch     0.27ms | transfer   228.87ms | handoff    0.19ms | total   229.36ms |   9.16 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.07       0.06       0.16
  dispatch       0.47       0.48       0.79
  transfer     221.35     223.35     228.87
  handoff        0.15       0.15       0.20
  total        222.05     224.40     229.36
  aggregate: 8 batches x 2.100GB in 1.78s = 9.45 GB/s wall-clock
CSV,get,pipeline,64,250,8400000,0.071,0.472,221.354,0.149,222.046,9.463,9.519,1,9.453

== op=get workers=64 keys=62 value=33.60MB batch=2.083GB mode=pipeline min_keys_per_tile=8 target_tile_mb=32 inflight=1
  batch 0: queue     0.09ms | dispatch     0.56ms | transfer   453.04ms | handoff    0.14ms | total   453.83ms |   4.59 GB/s
  batch 1: queue     0.05ms | dispatch     0.44ms | transfer   348.49ms | handoff    0.20ms | total   349.17ms |   5.97 GB/s
  batch 2: queue     0.11ms | dispatch     0.54ms | transfer   360.40ms | handoff    0.08ms | total   361.13ms |   5.77 GB/s
  batch 3: queue     0.03ms | dispatch     0.37ms | transfer   484.82ms | handoff    0.15ms | total   485.38ms |   4.29 GB/s
  batch 4: queue     0.04ms | dispatch     0.34ms | transfer   328.62ms | handoff    0.16ms | total   329.17ms |   6.33 GB/s
  batch 5: queue     0.11ms | dispatch     0.36ms | transfer   351.38ms | handoff    0.20ms | total   352.05ms |   5.92 GB/s
  batch 6: queue     0.12ms | dispatch     0.38ms | transfer   474.93ms | handoff    0.19ms | total   475.63ms |   4.38 GB/s
  batch 7: queue     0.15ms | dispatch     0.47ms | transfer   388.11ms | handoff    0.15ms | total   388.88ms |   5.36 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.09       0.11       0.15
  dispatch       0.43       0.44       0.56
  transfer     398.72     388.11     484.82
  handoff        0.16       0.16       0.20
  total        399.41     388.88     485.38
  aggregate: 8 batches x 2.083GB in 3.20s = 5.22 GB/s wall-clock
CSV,get,pipeline,64,62,33600000,0.088,0.433,398.724,0.159,399.405,5.325,5.769,1,5.215

== op=get workers=64 keys=500 value=4.20MB batch=2.100GB mode=pipeline min_keys_per_tile=8 target_tile_mb=32 inflight=1
  batch 0: queue     0.05ms | dispatch     0.38ms | transfer   178.64ms | handoff    0.20ms | total   179.27ms |  11.71 GB/s
  batch 1: queue     0.06ms | dispatch     0.48ms | transfer   324.40ms | handoff    0.12ms | total   325.06ms |   6.46 GB/s
  batch 2: queue     0.04ms | dispatch     0.34ms | transfer   162.15ms | handoff    0.16ms | total   162.71ms |  12.91 GB/s
  batch 3: queue     0.05ms | dispatch     0.35ms | transfer   169.04ms | handoff    0.22ms | total   169.66ms |  12.38 GB/s
  batch 4: queue     0.04ms | dispatch     0.30ms | transfer   168.37ms | handoff    0.16ms | total   168.87ms |  12.44 GB/s
  batch 5: queue     0.05ms | dispatch     0.32ms | transfer   165.80ms | handoff    0.07ms | total   166.25ms |  12.63 GB/s
  batch 6: queue     0.05ms | dispatch     0.28ms | transfer   176.03ms | handoff    0.16ms | total   176.52ms |  11.90 GB/s
  batch 7: queue     0.08ms | dispatch     0.31ms | transfer   161.19ms | handoff    0.15ms | total   161.72ms |  12.99 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.05       0.05       0.08
  dispatch       0.34       0.34       0.48
  transfer     188.20     169.04     324.40
  handoff        0.15       0.16       0.22
  total        188.76     169.66     325.06
  aggregate: 8 batches x 2.100GB in 1.51s = 11.11 GB/s wall-clock
CSV,get,pipeline,64,500,4200000,0.054,0.345,188.205,0.154,188.757,11.676,12.436,1,11.11

== op=get workers=64 keys=125 value=16.80MB batch=2.100GB mode=pipeline min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.02ms | dispatch     0.30ms | transfer   158.60ms | handoff    0.07ms | total   158.99ms |  13.21 GB/s
  batch 1: queue     0.05ms | dispatch     0.34ms | transfer   156.74ms | handoff    0.07ms | total   157.20ms |  13.36 GB/s
  batch 2: queue     0.04ms | dispatch     0.25ms | transfer   134.13ms | handoff    0.07ms | total   134.49ms |  15.61 GB/s
  batch 3: queue     0.03ms | dispatch     0.24ms | transfer   139.43ms | handoff    0.17ms | total   139.86ms |  15.01 GB/s
  batch 4: queue     0.05ms | dispatch     0.26ms | transfer   140.23ms | handoff    0.22ms | total   140.76ms |  14.92 GB/s
  batch 5: queue     0.04ms | dispatch     0.31ms | transfer   150.99ms | handoff    0.14ms | total   151.48ms |  13.86 GB/s
  batch 6: queue     0.05ms | dispatch     0.27ms | transfer   158.24ms | handoff    0.15ms | total   158.71ms |  13.23 GB/s
  batch 7: queue     0.10ms | dispatch     0.24ms | transfer   167.85ms | handoff    0.15ms | total   168.34ms |  12.47 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.04       0.05       0.10
  dispatch       0.28       0.27       0.34
  transfer     150.78     156.74     167.85
  handoff        0.13       0.15       0.22
  total        151.23     157.20     168.34
  aggregate: 8 batches x 2.100GB in 1.21s = 13.88 GB/s wall-clock
CSV,get,pipeline,64,125,16800000,0.045,0.277,150.777,0.131,151.228,13.961,13.863,1,13.878

== op=get workers=64 keys=250 value=8.40MB batch=2.100GB mode=pipeline min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.11ms | dispatch     0.37ms | transfer   168.42ms | handoff    0.16ms | total   169.05ms |  12.42 GB/s
  batch 1: queue     0.04ms | dispatch     0.35ms | transfer   183.50ms | handoff    0.21ms | total   184.10ms |  11.41 GB/s
  batch 2: queue     0.05ms | dispatch     0.32ms | transfer   343.66ms | handoff    0.14ms | total   344.17ms |   6.10 GB/s
  batch 3: queue     0.04ms | dispatch     0.35ms | transfer   176.52ms | handoff    0.07ms | total   176.98ms |  11.87 GB/s
  batch 4: queue     0.04ms | dispatch     0.33ms | transfer   174.39ms | handoff    0.15ms | total   174.91ms |  12.01 GB/s
  batch 5: queue     0.04ms | dispatch     0.27ms | transfer   188.51ms | handoff    0.26ms | total   189.08ms |  11.11 GB/s
  batch 6: queue     0.05ms | dispatch     0.31ms | transfer   183.72ms | handoff    0.15ms | total   184.23ms |  11.40 GB/s
  batch 7: queue     0.06ms | dispatch     0.30ms | transfer   180.47ms | handoff    0.13ms | total   180.97ms |  11.60 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.05       0.05       0.11
  dispatch       0.33       0.33       0.37
  transfer     199.90     183.50     343.66
  handoff        0.16       0.15       0.26
  total        200.44     184.10     344.17
  aggregate: 8 batches x 2.100GB in 1.60s = 10.47 GB/s wall-clock
CSV,get,pipeline,64,250,8400000,0.052,0.326,199.899,0.159,200.436,10.989,11.604,1,10.471

== op=get workers=64 keys=62 value=33.60MB batch=2.083GB mode=pipeline min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.05ms | dispatch     0.30ms | transfer   117.29ms | handoff    0.15ms | total   117.79ms |  17.69 GB/s
  batch 1: queue     0.02ms | dispatch     0.23ms | transfer   145.30ms | handoff    0.14ms | total   145.70ms |  14.30 GB/s
  batch 2: queue     0.04ms | dispatch     0.22ms | transfer   151.99ms | handoff    0.20ms | total   152.46ms |  13.66 GB/s
  batch 3: queue     0.09ms | dispatch     0.24ms | transfer   140.30ms | handoff    0.31ms | total   140.94ms |  14.78 GB/s
  batch 4: queue     0.04ms | dispatch     0.24ms | transfer   118.81ms | handoff    0.16ms | total   119.23ms |  17.47 GB/s
  batch 5: queue     0.02ms | dispatch     0.24ms | transfer   162.01ms | handoff    0.21ms | total   162.47ms |  12.82 GB/s
  batch 6: queue     0.04ms | dispatch     0.29ms | transfer   158.32ms | handoff    0.06ms | total   158.71ms |  13.13 GB/s
  batch 7: queue     0.05ms | dispatch     0.24ms | transfer   126.25ms | handoff    0.14ms | total   126.68ms |  16.44 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.04       0.04       0.09
  dispatch       0.25       0.24       0.30
  transfer     140.03     145.30     162.01
  handoff        0.17       0.16       0.31
  total        140.50     145.70     162.47
  aggregate: 8 batches x 2.083GB in 1.12s = 14.82 GB/s wall-clock
CSV,get,pipeline,64,62,33600000,0.043,0.251,140.034,0.171,140.499,15.036,14.781,1,14.821

== op=get workers=64 keys=500 value=4.20MB batch=2.100GB mode=pipeline min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.08ms | dispatch     0.31ms | transfer   188.77ms | handoff    0.13ms | total   189.29ms |  11.09 GB/s
  batch 1: queue     0.04ms | dispatch     0.36ms | transfer   333.54ms | handoff    0.17ms | total   334.10ms |   6.29 GB/s
  batch 2: queue     0.05ms | dispatch     0.34ms | transfer   165.49ms | handoff    0.18ms | total   166.07ms |  12.65 GB/s
  batch 3: queue     0.11ms | dispatch     0.42ms | transfer   187.08ms | handoff    0.16ms | total   187.76ms |  11.18 GB/s
  batch 4: queue     0.07ms | dispatch     0.26ms | transfer   177.53ms | handoff    0.15ms | total   178.00ms |  11.80 GB/s
  batch 5: queue     0.04ms | dispatch     0.35ms | transfer   182.63ms | handoff    0.08ms | total   183.09ms |  11.47 GB/s
  batch 6: queue     0.05ms | dispatch     0.36ms | transfer   162.09ms | handoff    0.05ms | total   162.55ms |  12.92 GB/s
  batch 7: queue     0.08ms | dispatch     0.32ms | transfer   188.19ms | handoff    0.17ms | total   188.76ms |  11.13 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.06       0.07       0.11
  dispatch       0.34       0.35       0.42
  transfer     198.16     187.08     333.54
  handoff        0.14       0.16       0.18
  total        198.70     187.76     334.10
  aggregate: 8 batches x 2.100GB in 1.59s = 10.55 GB/s wall-clock
CSV,get,pipeline,64,500,4200000,0.064,0.339,198.164,0.136,198.704,11.065,11.47,1,10.554

### RAM, 8 shards

CSV,op,get_batch_mode,num_workers,num_keys,value_bytes,queue_ms,dispatch_ms,transfer_ms,handoff_ms,total_ms,gbps_mean,gbps_p50,inflight,agg_gbps

== op=get workers=64 keys=125 value=16.80MB batch=2.100GB mode=single min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.02ms | dispatch    19.99ms | transfer    92.96ms | handoff    0.28ms | total   113.25ms |  18.54 GB/s
  batch 1: queue     0.03ms | dispatch    25.84ms | transfer    83.81ms | handoff    0.05ms | total   109.73ms |  19.14 GB/s
  batch 2: queue     0.04ms | dispatch    20.35ms | transfer   102.18ms | handoff    0.15ms | total   122.72ms |  17.11 GB/s
  batch 3: queue     0.06ms | dispatch    17.71ms | transfer   260.15ms | handoff    0.14ms | total   278.06ms |   7.55 GB/s
  batch 4: queue     0.04ms | dispatch    19.00ms | transfer    90.77ms | handoff    0.14ms | total   109.95ms |  19.10 GB/s
  batch 5: queue     0.06ms | dispatch    21.95ms | transfer    86.59ms | handoff    0.17ms | total   108.77ms |  19.31 GB/s
  batch 6: queue     0.09ms | dispatch    25.42ms | transfer    96.47ms | handoff    0.21ms | total   122.18ms |  17.19 GB/s
  batch 7: queue     0.06ms | dispatch    22.50ms | transfer   103.98ms | handoff    0.14ms | total   126.68ms |  16.58 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.05       0.06       0.09
  dispatch      21.59      21.95      25.84
  transfer     114.61      96.47     260.15
  handoff        0.16       0.15       0.28
  total        136.42     122.18     278.06
  aggregate: 8 batches x 2.100GB in 1.09s = 15.39 GB/s wall-clock
CSV,get,single,64,125,16800000,0.051,21.593,114.613,0.159,136.417,16.815,18.544,1,15.385

== op=get workers=64 keys=250 value=8.40MB batch=2.100GB mode=single min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.04ms | dispatch     8.73ms | transfer   139.38ms | handoff    0.18ms | total   148.33ms |  14.16 GB/s
  batch 1: queue     0.03ms | dispatch    10.08ms | transfer   162.08ms | handoff    0.16ms | total   172.35ms |  12.18 GB/s
  batch 2: queue     0.04ms | dispatch     8.51ms | transfer   131.41ms | handoff    0.19ms | total   140.14ms |  14.99 GB/s
  batch 3: queue     0.12ms | dispatch     9.23ms | transfer    86.12ms | handoff    0.20ms | total    95.67ms |  21.95 GB/s
  batch 4: queue     0.02ms | dispatch     8.96ms | transfer   122.80ms | handoff    0.27ms | total   132.05ms |  15.90 GB/s
  batch 5: queue     0.03ms | dispatch     6.87ms | transfer   116.53ms | handoff    0.22ms | total   123.65ms |  16.98 GB/s
  batch 6: queue     0.07ms | dispatch     6.78ms | transfer   138.37ms | handoff    0.21ms | total   145.42ms |  14.44 GB/s
  batch 7: queue     0.02ms | dispatch     7.56ms | transfer    98.11ms | handoff    0.19ms | total   105.88ms |  19.83 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.04       0.04       0.12
  dispatch       8.34       8.73      10.08
  transfer     124.35     131.41     162.08
  handoff        0.20       0.20       0.27
  total        132.94     140.14     172.35
  aggregate: 8 batches x 2.100GB in 1.06s = 15.78 GB/s wall-clock
CSV,get,single,64,250,8400000,0.045,8.339,124.351,0.202,132.936,16.305,15.903,1,15.783

== op=get workers=64 keys=62 value=33.60MB batch=2.083GB mode=single min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.02ms | dispatch    40.29ms | transfer    58.05ms | handoff    0.14ms | total    98.51ms |  21.15 GB/s
  batch 1: queue     0.09ms | dispatch    44.90ms | transfer    51.60ms | handoff    0.14ms | total    96.72ms |  21.54 GB/s
  batch 2: queue     0.03ms | dispatch    38.56ms | transfer    67.61ms | handoff    0.18ms | total   106.39ms |  19.58 GB/s
  batch 3: queue     0.05ms | dispatch    35.60ms | transfer    70.04ms | handoff    0.08ms | total   105.78ms |  19.69 GB/s
  batch 4: queue     0.04ms | dispatch    36.21ms | transfer    72.38ms | handoff    0.06ms | total   108.69ms |  19.17 GB/s
  batch 5: queue     0.04ms | dispatch    44.70ms | transfer    49.83ms | handoff    0.14ms | total    94.71ms |  22.00 GB/s
  batch 6: queue     0.05ms | dispatch    38.36ms | transfer    68.69ms | handoff    0.15ms | total   107.25ms |  19.42 GB/s
  batch 7: queue     0.09ms | dispatch    42.48ms | transfer    59.62ms | handoff    0.14ms | total   102.34ms |  20.36 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.05       0.05       0.09
  dispatch      40.14      40.29      44.90
  transfer      62.23      67.61      72.38
  handoff        0.13       0.14       0.18
  total        102.55     105.78     108.69
  aggregate: 8 batches x 2.083GB in 0.82s = 20.30 GB/s wall-clock
CSV,get,single,64,62,33600000,0.052,40.137,62.228,0.13,102.546,20.363,20.357,1,20.303

== op=get workers=64 keys=500 value=4.20MB batch=2.100GB mode=single min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.04ms | dispatch     5.52ms | transfer    94.36ms | handoff    0.16ms | total   100.08ms |  20.98 GB/s
  batch 1: queue     0.04ms | dispatch     5.19ms | transfer    91.87ms | handoff    0.20ms | total    97.30ms |  21.58 GB/s
  batch 2: queue     0.05ms | dispatch     4.65ms | transfer   291.75ms | handoff    0.15ms | total   296.60ms |   7.08 GB/s
  batch 3: queue     0.04ms | dispatch     4.88ms | transfer    94.88ms | handoff    0.14ms | total    99.95ms |  21.01 GB/s
  batch 4: queue     0.08ms | dispatch     4.45ms | transfer    93.58ms | handoff    0.07ms | total    98.18ms |  21.39 GB/s
  batch 5: queue     0.04ms | dispatch     4.78ms | transfer    97.12ms | handoff    0.16ms | total   102.09ms |  20.57 GB/s
  batch 6: queue     0.13ms | dispatch     5.12ms | transfer    89.44ms | handoff    0.15ms | total    94.84ms |  22.14 GB/s
  batch 7: queue     0.08ms | dispatch     4.45ms | transfer    94.83ms | handoff    0.14ms | total    99.50ms |  21.10 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.06       0.05       0.13
  dispatch       4.88       4.88       5.52
  transfer     118.48      94.83     291.75
  handoff        0.15       0.15       0.20
  total        123.57      99.95     296.60
  aggregate: 8 batches x 2.100GB in 0.99s = 16.96 GB/s wall-clock
CSV,get,single,64,500,4200000,0.062,4.88,118.479,0.147,123.567,19.483,21.105,1,16.959

== op=get workers=64 keys=125 value=16.80MB batch=2.100GB mode=pipeline min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.07ms | dispatch     0.25ms | transfer   111.54ms | handoff    0.19ms | total   112.04ms |  18.74 GB/s
  batch 1: queue     0.03ms | dispatch     0.32ms | transfer   133.95ms | handoff    0.07ms | total   134.37ms |  15.63 GB/s
  batch 2: queue     0.04ms | dispatch     0.38ms | transfer   114.20ms | handoff    0.17ms | total   114.78ms |  18.30 GB/s
  batch 3: queue     0.05ms | dispatch     0.28ms | transfer   134.61ms | handoff    0.16ms | total   135.09ms |  15.54 GB/s
  batch 4: queue     0.05ms | dispatch     0.30ms | transfer   117.53ms | handoff    0.15ms | total   118.03ms |  17.79 GB/s
  batch 5: queue     0.04ms | dispatch     0.31ms | transfer   128.71ms | handoff    0.17ms | total   129.22ms |  16.25 GB/s
  batch 6: queue     0.10ms | dispatch     0.26ms | transfer   121.90ms | handoff    0.14ms | total   122.41ms |  17.16 GB/s
  batch 7: queue     0.08ms | dispatch     0.31ms | transfer   122.23ms | handoff    0.22ms | total   122.84ms |  17.10 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.06       0.05       0.10
  dispatch       0.30       0.31       0.38
  transfer     123.08     122.23     134.61
  handoff        0.16       0.17       0.22
  total        123.60     122.84     135.09
  aggregate: 8 batches x 2.100GB in 0.99s = 16.98 GB/s wall-clock
CSV,get,pipeline,64,125,16800000,0.057,0.301,123.082,0.157,123.598,17.063,17.156,1,16.979

== op=get workers=64 keys=250 value=8.40MB batch=2.100GB mode=pipeline min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.04ms | dispatch     0.49ms | transfer   163.63ms | handoff    0.16ms | total   164.32ms |  12.78 GB/s
  batch 1: queue     0.04ms | dispatch     0.38ms | transfer   148.13ms | handoff    0.05ms | total   148.61ms |  14.13 GB/s
  batch 2: queue     0.03ms | dispatch     0.34ms | transfer   141.82ms | handoff    0.13ms | total   142.33ms |  14.75 GB/s
  batch 3: queue     0.04ms | dispatch     0.33ms | transfer   143.18ms | handoff    0.21ms | total   143.77ms |  14.61 GB/s
  batch 4: queue     0.05ms | dispatch     0.28ms | transfer   126.83ms | handoff    0.15ms | total   127.31ms |  16.50 GB/s
  batch 5: queue     0.09ms | dispatch     0.45ms | transfer   148.59ms | handoff    0.07ms | total   149.20ms |  14.08 GB/s
  batch 6: queue     0.04ms | dispatch     0.29ms | transfer   152.13ms | handoff    0.17ms | total   152.62ms |  13.76 GB/s
  batch 7: queue     0.04ms | dispatch     0.29ms | transfer   148.75ms | handoff    0.15ms | total   149.23ms |  14.07 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.05       0.04       0.09
  dispatch       0.36       0.34       0.49
  transfer     146.63     148.59     163.63
  handoff        0.14       0.15       0.21
  total        147.17     149.20     164.32
  aggregate: 8 batches x 2.100GB in 1.18s = 14.26 GB/s wall-clock
CSV,get,pipeline,64,250,8400000,0.046,0.357,146.633,0.135,147.172,14.334,14.131,1,14.258

== op=get workers=64 keys=62 value=33.60MB batch=2.083GB mode=pipeline min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.05ms | dispatch     0.23ms | transfer   102.93ms | handoff    0.07ms | total   103.27ms |  20.17 GB/s
  batch 1: queue     0.05ms | dispatch     0.30ms | transfer    93.15ms | handoff    0.17ms | total    93.67ms |  22.24 GB/s
  batch 2: queue     0.02ms | dispatch     0.22ms | transfer    97.07ms | handoff    0.10ms | total    97.41ms |  21.38 GB/s
  batch 3: queue     0.04ms | dispatch     0.30ms | transfer   100.58ms | handoff    0.17ms | total   101.08ms |  20.61 GB/s
  batch 4: queue     0.03ms | dispatch     0.24ms | transfer   102.69ms | handoff    0.27ms | total   103.23ms |  20.18 GB/s
  batch 5: queue     0.04ms | dispatch     0.25ms | transfer   100.40ms | handoff    0.07ms | total   100.75ms |  20.68 GB/s
  batch 6: queue     0.03ms | dispatch     0.25ms | transfer   103.14ms | handoff    0.16ms | total   103.58ms |  20.11 GB/s
  batch 7: queue     0.04ms | dispatch     0.27ms | transfer   112.71ms | handoff    0.16ms | total   113.17ms |  18.41 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.04       0.04       0.05
  dispatch       0.26       0.25       0.30
  transfer     101.58     102.69     112.71
  handoff        0.15       0.16       0.27
  total        102.02     103.23     113.17
  aggregate: 8 batches x 2.083GB in 0.82s = 20.41 GB/s wall-clock
CSV,get,pipeline,64,62,33600000,0.036,0.257,101.584,0.145,102.022,20.473,20.609,1,20.407

== op=get workers=64 keys=500 value=4.20MB batch=2.100GB mode=pipeline min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.06ms | dispatch     0.36ms | transfer   145.20ms | handoff    0.21ms | total   145.84ms |  14.40 GB/s
  batch 1: queue     0.11ms | dispatch     0.41ms | transfer   129.16ms | handoff    0.14ms | total   129.82ms |  16.18 GB/s
  batch 2: queue     0.04ms | dispatch     0.31ms | transfer   146.46ms | handoff    0.14ms | total   146.96ms |  14.29 GB/s
  batch 3: queue     0.04ms | dispatch     0.37ms | transfer   148.89ms | handoff    0.17ms | total   149.47ms |  14.05 GB/s
  batch 4: queue     0.04ms | dispatch     0.41ms | transfer   130.18ms | handoff    0.06ms | total   130.70ms |  16.07 GB/s
  batch 5: queue     0.04ms | dispatch     0.26ms | transfer   142.40ms | handoff    0.14ms | total   142.84ms |  14.70 GB/s
  batch 6: queue     0.04ms | dispatch     0.34ms | transfer   141.05ms | handoff    0.15ms | total   141.58ms |  14.83 GB/s
  batch 7: queue     0.04ms | dispatch     0.29ms | transfer   134.90ms | handoff    0.19ms | total   135.42ms |  15.51 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.05       0.04       0.11
  dispatch       0.35       0.36       0.41
  transfer     139.78     142.40     148.89
  handoff        0.15       0.15       0.21
  total        140.33     142.84     149.47
  aggregate: 8 batches x 2.100GB in 1.12s = 14.94 GB/s wall-clock
CSV,get,pipeline,64,500,4200000,0.052,0.345,139.782,0.15,140.328,15.003,14.832,1,14.94

### Flex, 4 shards

CSV,op,get_batch_mode,num_workers,num_keys,value_bytes,queue_ms,dispatch_ms,transfer_ms,handoff_ms,total_ms,gbps_mean,gbps_p50,inflight,agg_gbps

== op=get workers=64 keys=125 value=16.80MB batch=2.100GB mode=single min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.04ms | dispatch    26.21ms | transfer   134.80ms | handoff    0.22ms | total   161.27ms |  13.02 GB/s
  batch 1: queue     0.03ms | dispatch    21.55ms | transfer   134.97ms | handoff    0.17ms | total   156.71ms |  13.40 GB/s
  batch 2: queue     0.05ms | dispatch    27.86ms | transfer   131.32ms | handoff    0.15ms | total   159.39ms |  13.18 GB/s
  batch 3: queue     0.05ms | dispatch    19.02ms | transfer   138.15ms | handoff    0.18ms | total   157.40ms |  13.34 GB/s
  batch 4: queue     0.04ms | dispatch    33.72ms | transfer   123.16ms | handoff    0.15ms | total   157.07ms |  13.37 GB/s
  batch 5: queue     0.03ms | dispatch    34.94ms | transfer   125.37ms | handoff    0.16ms | total   160.50ms |  13.08 GB/s
  batch 6: queue     0.06ms | dispatch    27.30ms | transfer   134.85ms | handoff    0.34ms | total   162.55ms |  12.92 GB/s
  batch 7: queue     0.03ms | dispatch    29.01ms | transfer   125.40ms | handoff    0.16ms | total   154.59ms |  13.58 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.04       0.04       0.06
  dispatch      27.45      27.86      34.94
  transfer     131.00     134.80     138.15
  handoff        0.19       0.17       0.34
  total        158.68     159.39     162.55
  aggregate: 8 batches x 2.100GB in 1.27s = 13.23 GB/s wall-clock
CSV,get,single,64,125,16800000,0.04,27.451,131.002,0.191,158.685,13.237,13.341,1,13.226

== op=get workers=64 keys=250 value=8.40MB batch=2.100GB mode=single min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.03ms | dispatch    12.21ms | transfer   110.58ms | handoff    0.19ms | total   123.01ms |  17.07 GB/s
  batch 1: queue     0.04ms | dispatch    16.46ms | transfer   129.08ms | handoff    0.15ms | total   145.73ms |  14.41 GB/s
  batch 2: queue     0.04ms | dispatch    16.74ms | transfer   122.35ms | handoff    0.21ms | total   139.34ms |  15.07 GB/s
  batch 3: queue     0.03ms | dispatch    10.20ms | transfer   137.11ms | handoff    0.15ms | total   147.49ms |  14.24 GB/s
  batch 4: queue     0.04ms | dispatch    14.18ms | transfer   132.39ms | handoff    0.16ms | total   146.77ms |  14.31 GB/s
  batch 5: queue     0.05ms | dispatch    17.83ms | transfer   125.17ms | handoff    0.14ms | total   143.19ms |  14.67 GB/s
  batch 6: queue     0.03ms | dispatch    14.45ms | transfer   137.83ms | handoff    0.21ms | total   152.51ms |  13.77 GB/s
  batch 7: queue     0.04ms | dispatch    15.84ms | transfer   133.44ms | handoff    0.16ms | total   149.49ms |  14.05 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.04       0.04       0.05
  dispatch      14.74      15.84      17.83
  transfer     128.49     132.39     137.83
  handoff        0.17       0.16       0.21
  total        143.44     146.77     152.51
  aggregate: 8 batches x 2.100GB in 1.15s = 14.63 GB/s wall-clock
CSV,get,single,64,250,8400000,0.037,14.74,128.492,0.171,143.44,14.698,14.41,1,14.629

== op=get workers=64 keys=62 value=33.60MB batch=2.083GB mode=single min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.08ms | dispatch    55.77ms | transfer    88.72ms | handoff    0.16ms | total   144.73ms |  14.39 GB/s
  batch 1: queue     0.06ms | dispatch    57.17ms | transfer    90.55ms | handoff    0.18ms | total   147.97ms |  14.08 GB/s
  batch 2: queue     0.04ms | dispatch    61.59ms | transfer   224.40ms | handoff    0.16ms | total   286.19ms |   7.28 GB/s
  batch 3: queue     0.05ms | dispatch    59.40ms | transfer    96.53ms | handoff    0.17ms | total   156.15ms |  13.34 GB/s
  batch 4: queue     0.03ms | dispatch    77.79ms | transfer   100.95ms | handoff    0.17ms | total   178.94ms |  11.64 GB/s
  batch 5: queue     0.04ms | dispatch    53.56ms | transfer   135.85ms | handoff    0.16ms | total   189.60ms |  10.99 GB/s
  batch 6: queue     0.04ms | dispatch    48.53ms | transfer   101.26ms | handoff    0.14ms | total   149.97ms |  13.89 GB/s
  batch 7: queue     0.08ms | dispatch    48.05ms | transfer    90.03ms | handoff    0.28ms | total   138.44ms |  15.05 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.05       0.05       0.08
  dispatch      57.73      57.17      77.79
  transfer     116.04     100.95     224.40
  handoff        0.18       0.17       0.28
  total        174.00     156.15     286.19
  aggregate: 8 batches x 2.083GB in 1.39s = 11.97 GB/s wall-clock
CSV,get,single,64,62,33600000,0.052,57.733,116.036,0.176,173.997,12.583,13.891,1,11.968

== op=get workers=64 keys=500 value=4.20MB batch=2.100GB mode=single min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.05ms | dispatch     4.15ms | transfer   314.80ms | handoff    0.22ms | total   319.21ms |   6.58 GB/s
  batch 1: queue     0.04ms | dispatch     7.22ms | transfer   167.87ms | handoff    0.08ms | total   175.21ms |  11.99 GB/s
  batch 2: queue     0.04ms | dispatch     3.29ms | transfer   325.12ms | handoff    0.15ms | total   328.60ms |   6.39 GB/s
  batch 3: queue     0.04ms | dispatch     8.32ms | transfer   193.80ms | handoff    0.19ms | total   202.36ms |  10.38 GB/s
  batch 4: queue     0.03ms | dispatch     3.23ms | transfer   154.11ms | handoff    0.16ms | total   157.53ms |  13.33 GB/s
  batch 5: queue     0.09ms | dispatch     4.66ms | transfer   270.99ms | handoff    0.15ms | total   275.88ms |   7.61 GB/s
  batch 6: queue     0.08ms | dispatch     3.66ms | transfer   186.46ms | handoff    0.15ms | total   190.35ms |  11.03 GB/s
  batch 7: queue     0.04ms | dispatch     5.69ms | transfer   206.54ms | handoff    0.20ms | total   212.47ms |   9.88 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.05       0.04       0.09
  dispatch       5.03       4.66       8.32
  transfer     227.46     206.54     325.12
  handoff        0.16       0.16       0.22
  total        232.70     212.47     328.60
  aggregate: 8 batches x 2.100GB in 1.86s = 9.01 GB/s wall-clock
CSV,get,single,64,500,4200000,0.05,5.027,227.462,0.162,232.701,9.649,10.378,1,9.014


== op=get workers=64 keys=125 value=16.80MB batch=2.100GB mode=pipeline min_keys_per_tile=8 target_tile_mb=32 inflight=1
  batch 0: queue     0.04ms | dispatch     0.39ms | transfer   378.70ms | handoff    0.07ms | total   379.20ms |   5.54 GB/s
  batch 1: queue     0.04ms | dispatch     0.28ms | transfer   293.96ms | handoff    0.17ms | total   294.45ms |   7.13 GB/s
  batch 2: queue     0.13ms | dispatch     0.49ms | transfer   297.59ms | handoff    0.15ms | total   298.35ms |   7.04 GB/s
  batch 3: queue     0.06ms | dispatch     0.36ms | transfer   268.16ms | handoff    0.16ms | total   268.74ms |   7.81 GB/s
  batch 4: queue     0.12ms | dispatch     0.32ms | transfer   310.04ms | handoff    0.07ms | total   310.55ms |   6.76 GB/s
  batch 5: queue     0.04ms | dispatch     0.40ms | transfer   303.49ms | handoff    0.15ms | total   304.08ms |   6.91 GB/s
  batch 6: queue     0.04ms | dispatch     0.38ms | transfer   293.22ms | handoff    0.14ms | total   293.78ms |   7.15 GB/s
  batch 7: queue     0.13ms | dispatch     0.64ms | transfer   250.59ms | handoff    0.16ms | total   251.52ms |   8.35 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.07       0.06       0.13
  dispatch       0.41       0.39       0.64
  transfer     299.47     297.59     378.70
  handoff        0.13       0.15       0.17
  total        300.08     298.35     379.20
  aggregate: 8 batches x 2.100GB in 2.40s = 7.00 GB/s wall-clock
CSV,get,pipeline,64,125,16800000,0.075,0.408,299.469,0.133,300.085,7.086,7.132,1,6.996

== op=get workers=64 keys=250 value=8.40MB batch=2.100GB mode=pipeline min_keys_per_tile=8 target_tile_mb=32 inflight=1
  batch 0: queue     0.04ms | dispatch     0.40ms | transfer   214.85ms | handoff    0.21ms | total   215.49ms |   9.75 GB/s
  batch 1: queue     0.06ms | dispatch     0.30ms | transfer   213.38ms | handoff    0.06ms | total   213.81ms |   9.82 GB/s
  batch 2: queue     0.04ms | dispatch     0.28ms | transfer   218.59ms | handoff    0.17ms | total   219.08ms |   9.59 GB/s
  batch 3: queue     0.04ms | dispatch     0.35ms | transfer   207.27ms | handoff    0.21ms | total   207.87ms |  10.10 GB/s
  batch 4: queue     0.04ms | dispatch     0.31ms | transfer   188.93ms | handoff    0.17ms | total   189.46ms |  11.08 GB/s
  batch 5: queue     0.05ms | dispatch     0.40ms | transfer   196.82ms | handoff    0.27ms | total   197.54ms |  10.63 GB/s
  batch 6: queue     0.04ms | dispatch     0.42ms | transfer   209.64ms | handoff    0.16ms | total   210.26ms |   9.99 GB/s
  batch 7: queue     0.05ms | dispatch     0.54ms | transfer   195.90ms | handoff    0.15ms | total   196.65ms |  10.68 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.05       0.04       0.06
  dispatch       0.38       0.40       0.54
  transfer     205.67     209.64     218.59
  handoff        0.18       0.17       0.27
  total        206.27     210.26     219.08
  aggregate: 8 batches x 2.100GB in 1.65s = 10.18 GB/s wall-clock
CSV,get,pipeline,64,250,8400000,0.046,0.375,205.674,0.175,206.271,10.205,10.102,1,10.175

== op=get workers=64 keys=62 value=33.60MB batch=2.083GB mode=pipeline min_keys_per_tile=8 target_tile_mb=32 inflight=1
  batch 0: queue     0.11ms | dispatch     0.67ms | transfer   466.37ms | handoff    0.19ms | total   467.34ms |   4.46 GB/s
  batch 1: queue     0.13ms | dispatch     0.55ms | transfer   367.49ms | handoff    0.15ms | total   368.32ms |   5.66 GB/s
  batch 2: queue     0.12ms | dispatch     0.50ms | transfer   463.20ms | handoff    0.15ms | total   463.97ms |   4.49 GB/s
  batch 3: queue     0.13ms | dispatch     0.79ms | transfer   480.94ms | handoff    0.14ms | total   482.00ms |   4.32 GB/s
  batch 4: queue     0.04ms | dispatch     0.45ms | transfer   491.29ms | handoff    0.18ms | total   491.97ms |   4.23 GB/s
  batch 5: queue     0.04ms | dispatch     0.55ms | transfer   357.17ms | handoff    0.13ms | total   357.90ms |   5.82 GB/s
  batch 6: queue     0.11ms | dispatch     0.45ms | transfer   617.40ms | handoff    0.14ms | total   618.10ms |   3.37 GB/s
  batch 7: queue     0.04ms | dispatch     0.43ms | transfer   355.52ms | handoff    0.16ms | total   356.15ms |   5.85 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.09       0.11       0.13
  dispatch       0.55       0.55       0.79
  transfer     449.92     466.37     617.40
  handoff        0.16       0.15       0.19
  total        450.72     467.34     618.10
  aggregate: 8 batches x 2.083GB in 3.61s = 4.62 GB/s wall-clock
CSV,get,pipeline,64,62,33600000,0.09,0.549,449.922,0.157,450.718,4.775,4.49,1,4.621

== op=get workers=64 keys=500 value=4.20MB batch=2.100GB mode=pipeline min_keys_per_tile=8 target_tile_mb=32 inflight=1
  batch 0: queue     0.04ms | dispatch     2.44ms | transfer   232.33ms | handoff    0.23ms | total   235.04ms |   8.93 GB/s
  batch 1: queue     0.05ms | dispatch     0.85ms | transfer   212.04ms | handoff    0.08ms | total   213.01ms |   9.86 GB/s
  batch 2: queue     0.03ms | dispatch     0.87ms | transfer   246.93ms | handoff    0.15ms | total   247.99ms |   8.47 GB/s
  batch 3: queue     0.04ms | dispatch     0.63ms | transfer   233.90ms | handoff    0.18ms | total   234.75ms |   8.95 GB/s
  batch 4: queue     0.04ms | dispatch     2.56ms | transfer   205.51ms | handoff    0.15ms | total   208.26ms |  10.08 GB/s
  batch 5: queue     0.04ms | dispatch     0.78ms | transfer   225.42ms | handoff    0.15ms | total   226.40ms |   9.28 GB/s
  batch 6: queue     0.06ms | dispatch     3.63ms | transfer   254.78ms | handoff    0.22ms | total   258.69ms |   8.12 GB/s
  batch 7: queue     0.10ms | dispatch     0.76ms | transfer   223.24ms | handoff    0.22ms | total   224.32ms |   9.36 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.05       0.04       0.10
  dispatch       1.57       0.87       3.63
  transfer     229.27     232.33     254.78
  handoff        0.17       0.18       0.23
  total        231.06     234.75     258.69
  aggregate: 8 batches x 2.100GB in 1.85s = 9.08 GB/s wall-clock
CSV,get,pipeline,64,500,4200000,0.051,1.566,229.268,0.173,231.057,9.131,9.276,1,9.078

== op=get workers=64 keys=125 value=16.80MB batch=2.100GB mode=pipeline min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.09ms | dispatch     0.23ms | transfer   152.35ms | handoff    0.24ms | total   152.91ms |  13.73 GB/s
  batch 1: queue     0.05ms | dispatch     0.46ms | transfer   187.18ms | handoff    0.07ms | total   187.76ms |  11.18 GB/s
  batch 2: queue     0.05ms | dispatch     0.30ms | transfer   185.63ms | handoff    0.17ms | total   186.16ms |  11.28 GB/s
  batch 3: queue     0.07ms | dispatch     0.25ms | transfer   157.79ms | handoff    0.19ms | total   158.30ms |  13.27 GB/s
  batch 4: queue     0.05ms | dispatch     0.44ms | transfer   166.77ms | handoff    0.16ms | total   167.41ms |  12.54 GB/s
  batch 5: queue     0.04ms | dispatch     0.28ms | transfer   145.47ms | handoff    0.13ms | total   145.92ms |  14.39 GB/s
  batch 6: queue     0.04ms | dispatch     0.24ms | transfer   168.04ms | handoff    0.19ms | total   168.51ms |  12.46 GB/s
  batch 7: queue     0.08ms | dispatch     0.32ms | transfer   178.42ms | handoff    0.25ms | total   179.07ms |  11.73 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.06       0.05       0.09
  dispatch       0.31       0.30       0.46
  transfer     167.71     168.04     187.18
  handoff        0.17       0.19       0.25
  total        168.25     168.51     187.76
  aggregate: 8 batches x 2.100GB in 1.35s = 12.48 GB/s wall-clock
CSV,get,pipeline,64,125,16800000,0.058,0.315,167.707,0.175,168.254,12.574,12.544,1,12.475

== op=get workers=64 keys=250 value=8.40MB batch=2.100GB mode=pipeline min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.05ms | dispatch     0.37ms | transfer   292.09ms | handoff    0.16ms | total   292.66ms |   7.18 GB/s
  batch 1: queue     0.05ms | dispatch     0.33ms | transfer   155.03ms | handoff    0.05ms | total   155.47ms |  13.51 GB/s
  batch 2: queue     0.03ms | dispatch     0.34ms | transfer   152.76ms | handoff    0.20ms | total   153.34ms |  13.70 GB/s
  batch 3: queue     0.04ms | dispatch     0.31ms | transfer   152.87ms | handoff    0.14ms | total   153.36ms |  13.69 GB/s
  batch 4: queue     0.06ms | dispatch     0.29ms | transfer   168.95ms | handoff    0.15ms | total   169.46ms |  12.39 GB/s
  batch 5: queue     0.04ms | dispatch     0.39ms | transfer   164.93ms | handoff    0.16ms | total   165.53ms |  12.69 GB/s
  batch 6: queue     0.07ms | dispatch     0.27ms | transfer   150.73ms | handoff    0.15ms | total   151.22ms |  13.89 GB/s
  batch 7: queue     0.03ms | dispatch     0.35ms | transfer   158.85ms | handoff    0.15ms | total   159.39ms |  13.18 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.05       0.05       0.07
  dispatch       0.33       0.34       0.39
  transfer     174.53     158.85     292.09
  handoff        0.15       0.15       0.20
  total        175.05     159.39     292.66
  aggregate: 8 batches x 2.100GB in 1.40s = 11.99 GB/s wall-clock
CSV,get,pipeline,64,250,8400000,0.046,0.333,174.527,0.146,175.052,12.527,13.508,1,11.989

== op=get workers=64 keys=62 value=33.60MB batch=2.083GB mode=pipeline min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.04ms | dispatch     0.35ms | transfer   156.82ms | handoff    0.17ms | total   157.38ms |  13.24 GB/s
  batch 1: queue     0.09ms | dispatch     0.48ms | transfer   143.42ms | handoff    0.18ms | total   144.17ms |  14.45 GB/s
  batch 2: queue     0.06ms | dispatch     0.30ms | transfer   151.54ms | handoff    0.18ms | total   152.08ms |  13.70 GB/s
  batch 3: queue     0.04ms | dispatch     0.24ms | transfer   147.98ms | handoff    0.18ms | total   148.45ms |  14.03 GB/s
  batch 4: queue     0.06ms | dispatch     0.22ms | transfer   146.48ms | handoff    0.14ms | total   146.91ms |  14.18 GB/s
  batch 5: queue     0.06ms | dispatch     0.29ms | transfer   158.80ms | handoff    0.16ms | total   159.32ms |  13.08 GB/s
  batch 6: queue     0.04ms | dispatch     0.30ms | transfer   178.49ms | handoff    0.17ms | total   179.00ms |  11.64 GB/s
  batch 7: queue     0.05ms | dispatch     0.27ms | transfer   161.04ms | handoff    0.19ms | total   161.55ms |  12.90 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.05       0.06       0.09
  dispatch       0.31       0.30       0.48
  transfer     155.57     156.82     178.49
  handoff        0.17       0.18       0.19
  total        156.11     157.38     179.00
  aggregate: 8 batches x 2.083GB in 1.25s = 13.34 GB/s wall-clock
CSV,get,pipeline,64,62,33600000,0.055,0.307,155.571,0.173,156.106,13.401,13.698,1,13.34

== op=get workers=64 keys=500 value=4.20MB batch=2.100GB mode=pipeline min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.06ms | dispatch     0.93ms | transfer   219.97ms | handoff    0.22ms | total   221.18ms |   9.49 GB/s
  batch 1: queue     0.04ms | dispatch     0.40ms | transfer   210.19ms | handoff    0.07ms | total   210.70ms |   9.97 GB/s
  batch 2: queue     0.04ms | dispatch     2.35ms | transfer   209.61ms | handoff    0.17ms | total   212.18ms |   9.90 GB/s
  batch 3: queue     0.03ms | dispatch     1.05ms | transfer   218.31ms | handoff    0.05ms | total   219.44ms |   9.57 GB/s
  batch 4: queue     0.03ms | dispatch     2.09ms | transfer   238.43ms | handoff    0.14ms | total   240.68ms |   8.73 GB/s
  batch 5: queue     0.04ms | dispatch     1.20ms | transfer   216.46ms | handoff    0.22ms | total   217.92ms |   9.64 GB/s
  batch 6: queue     0.06ms | dispatch     0.83ms | transfer   214.67ms | handoff    0.15ms | total   215.70ms |   9.74 GB/s
  batch 7: queue     0.03ms | dispatch     0.95ms | transfer   212.54ms | handoff    0.15ms | total   213.66ms |   9.83 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.04       0.04       0.06
  dispatch       1.23       1.05       2.35
  transfer     217.52     216.46     238.43
  handoff        0.15       0.15       0.22
  total        218.93     217.92     240.68
  aggregate: 8 batches x 2.100GB in 1.75s = 9.58 GB/s wall-clock
CSV,get,pipeline,64,500,4200000,0.041,1.225,217.52,0.146,218.932,9.607,9.736,1,9.581

### Flex, 8 shards

CSV,op,get_batch_mode,num_workers,num_keys,value_bytes,queue_ms,dispatch_ms,transfer_ms,handoff_ms,total_ms,gbps_mean,gbps_p50,inflight,agg_gbps

== op=get workers=64 keys=125 value=16.80MB batch=2.100GB mode=single min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.04ms | dispatch    21.02ms | transfer    83.58ms | handoff    0.23ms | total   104.87ms |  20.02 GB/s
  batch 1: queue     0.06ms | dispatch    19.27ms | transfer    89.53ms | handoff    0.14ms | total   109.00ms |  19.27 GB/s
  batch 2: queue     0.06ms | dispatch    20.80ms | transfer    85.46ms | handoff    0.19ms | total   106.51ms |  19.72 GB/s
  batch 3: queue     0.06ms | dispatch    20.61ms | transfer    84.71ms | handoff    0.17ms | total   105.55ms |  19.90 GB/s
  batch 4: queue     0.05ms | dispatch    18.37ms | transfer    89.95ms | handoff    0.22ms | total   108.59ms |  19.34 GB/s
  batch 5: queue     0.04ms | dispatch    15.48ms | transfer    86.79ms | handoff    0.21ms | total   102.52ms |  20.48 GB/s
  batch 6: queue     0.07ms | dispatch    21.21ms | transfer    85.62ms | handoff    0.17ms | total   107.08ms |  19.61 GB/s
  batch 7: queue     0.05ms | dispatch    19.24ms | transfer    86.29ms | handoff    0.26ms | total   105.84ms |  19.84 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.05       0.06       0.07
  dispatch      19.50      20.61      21.21
  transfer      86.49      86.29      89.95
  handoff        0.20       0.21       0.26
  total        106.25     106.51     109.00
  aggregate: 8 batches x 2.100GB in 0.85s = 19.75 GB/s wall-clock
CSV,get,single,64,125,16800000,0.054,19.501,86.491,0.199,106.245,19.772,19.841,1,19.749

== op=get workers=64 keys=250 value=8.40MB batch=2.100GB mode=single min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.03ms | dispatch     7.97ms | transfer   101.15ms | handoff    0.15ms | total   109.30ms |  19.21 GB/s
  batch 1: queue     0.06ms | dispatch     8.65ms | transfer   111.74ms | handoff    0.20ms | total   120.65ms |  17.41 GB/s
  batch 2: queue     0.04ms | dispatch     7.64ms | transfer   125.77ms | handoff    0.17ms | total   133.63ms |  15.72 GB/s
  batch 3: queue     0.04ms | dispatch     7.50ms | transfer   116.57ms | handoff    0.08ms | total   124.19ms |  16.91 GB/s
  batch 4: queue     0.05ms | dispatch     8.24ms | transfer   101.76ms | handoff    0.20ms | total   110.26ms |  19.05 GB/s
  batch 5: queue     0.05ms | dispatch     9.15ms | transfer    97.31ms | handoff    0.15ms | total   106.67ms |  19.69 GB/s
  batch 6: queue     0.06ms | dispatch     7.79ms | transfer   109.48ms | handoff    0.20ms | total   117.53ms |  17.87 GB/s
  batch 7: queue     0.04ms | dispatch     9.34ms | transfer   101.97ms | handoff    0.17ms | total   111.53ms |  18.83 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.05       0.05       0.06
  dispatch       8.29       8.24       9.34
  transfer     108.22     109.48     125.77
  handoff        0.16       0.17       0.20
  total        116.72     117.53     133.63
  aggregate: 8 batches x 2.100GB in 0.93s = 17.97 GB/s wall-clock
CSV,get,single,64,250,8400000,0.048,8.287,108.219,0.165,116.719,18.084,18.829,1,17.974

== op=get workers=64 keys=62 value=33.60MB batch=2.083GB mode=single min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.03ms | dispatch    45.87ms | transfer    70.18ms | handoff    0.08ms | total   116.16ms |  17.93 GB/s
  batch 1: queue     0.04ms | dispatch    38.86ms | transfer    63.28ms | handoff    0.16ms | total   102.34ms |  20.36 GB/s
  batch 2: queue     0.04ms | dispatch    40.24ms | transfer    65.84ms | handoff    0.24ms | total   106.35ms |  19.59 GB/s
  batch 3: queue     0.05ms | dispatch    35.77ms | transfer   107.96ms | handoff    0.16ms | total   143.94ms |  14.47 GB/s
  batch 4: queue     0.04ms | dispatch    36.66ms | transfer    94.10ms | handoff    0.14ms | total   130.94ms |  15.91 GB/s
  batch 5: queue     0.04ms | dispatch    34.32ms | transfer    95.12ms | handoff    0.15ms | total   129.63ms |  16.07 GB/s
  batch 6: queue     0.06ms | dispatch    42.67ms | transfer    71.19ms | handoff    0.15ms | total   114.07ms |  18.26 GB/s
  batch 7: queue     0.05ms | dispatch    45.67ms | transfer    75.02ms | handoff    0.20ms | total   120.93ms |  17.23 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.05       0.04       0.06
  dispatch      40.01      40.24      45.87
  transfer      80.33      75.02     107.96
  handoff        0.16       0.16       0.24
  total        120.55     120.93     143.94
  aggregate: 8 batches x 2.083GB in 0.96s = 17.27 GB/s wall-clock
CSV,get,single,64,62,33600000,0.045,40.006,80.334,0.16,120.546,17.477,17.934,1,17.273

== op=get workers=64 keys=500 value=4.20MB batch=2.100GB mode=single min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.03ms | dispatch     2.93ms | transfer   306.40ms | handoff    0.21ms | total   309.58ms |   6.78 GB/s
  batch 1: queue     0.04ms | dispatch     3.25ms | transfer   109.66ms | handoff    0.16ms | total   113.11ms |  18.57 GB/s
  batch 2: queue     0.02ms | dispatch     3.96ms | transfer   113.67ms | handoff    0.20ms | total   117.84ms |  17.82 GB/s
  batch 3: queue     0.04ms | dispatch     3.15ms | transfer   156.42ms | handoff    0.18ms | total   159.79ms |  13.14 GB/s
  batch 4: queue     0.04ms | dispatch     2.95ms | transfer   114.95ms | handoff    0.16ms | total   118.10ms |  17.78 GB/s
  batch 5: queue     0.02ms | dispatch     3.11ms | transfer   103.57ms | handoff    0.17ms | total   106.88ms |  19.65 GB/s
  batch 6: queue     0.03ms | dispatch     3.81ms | transfer   106.76ms | handoff    0.14ms | total   110.75ms |  18.96 GB/s
  batch 7: queue     0.04ms | dispatch     2.96ms | transfer   111.62ms | handoff    0.21ms | total   114.83ms |  18.29 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.03       0.04       0.04
  dispatch       3.26       3.15       3.96
  transfer     140.38     113.67     306.40
  handoff        0.18       0.18       0.21
  total        143.86     117.84     309.58
  aggregate: 8 batches x 2.100GB in 1.15s = 14.57 GB/s wall-clock
CSV,get,single,64,500,4200000,0.032,3.265,140.383,0.18,143.859,16.374,18.288,1,14.57

== op=get workers=64 keys=125 value=16.80MB batch=2.100GB mode=pipeline min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.04ms | dispatch     0.33ms | transfer   134.98ms | handoff    0.15ms | total   135.50ms |  15.50 GB/s
  batch 1: queue     0.05ms | dispatch     0.26ms | transfer   295.01ms | handoff    0.19ms | total   295.51ms |   7.11 GB/s
  batch 2: queue     0.04ms | dispatch     0.27ms | transfer   131.67ms | handoff    0.14ms | total   132.12ms |  15.90 GB/s
  batch 3: queue     0.04ms | dispatch     0.33ms | transfer   135.65ms | handoff    0.17ms | total   136.18ms |  15.42 GB/s
  batch 4: queue     0.04ms | dispatch     0.35ms | transfer   133.12ms | handoff    0.05ms | total   133.56ms |  15.72 GB/s
  batch 5: queue     0.09ms | dispatch     0.31ms | transfer   137.85ms | handoff    0.14ms | total   138.38ms |  15.18 GB/s
  batch 6: queue     0.04ms | dispatch     0.36ms | transfer   133.02ms | handoff    0.13ms | total   133.55ms |  15.72 GB/s
  batch 7: queue     0.10ms | dispatch     0.30ms | transfer   127.79ms | handoff    0.15ms | total   128.34ms |  16.36 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.05       0.04       0.10
  dispatch       0.31       0.33       0.36
  transfer     153.63     134.98     295.01
  handoff        0.14       0.15       0.19
  total        154.14     135.50     295.51
  aggregate: 8 batches x 2.100GB in 1.23s = 13.62 GB/s wall-clock
CSV,get,pipeline,64,125,16800000,0.054,0.314,153.634,0.141,154.143,14.613,15.723,1,13.616

== op=get workers=64 keys=250 value=8.40MB batch=2.100GB mode=pipeline min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.02ms | dispatch     0.31ms | transfer   148.71ms | handoff    0.34ms | total   149.37ms |  14.06 GB/s
  batch 1: queue     0.04ms | dispatch     0.38ms | transfer   138.64ms | handoff    0.15ms | total   139.20ms |  15.09 GB/s
  batch 2: queue     0.10ms | dispatch     0.32ms | transfer   155.62ms | handoff    0.19ms | total   156.23ms |  13.44 GB/s
  batch 3: queue     0.04ms | dispatch     0.40ms | transfer   156.81ms | handoff    0.15ms | total   157.40ms |  13.34 GB/s
  batch 4: queue     0.04ms | dispatch     0.35ms | transfer   145.03ms | handoff    0.16ms | total   145.57ms |  14.43 GB/s
  batch 5: queue     0.03ms | dispatch     0.25ms | transfer   155.57ms | handoff    0.18ms | total   156.03ms |  13.46 GB/s
  batch 6: queue     0.04ms | dispatch     0.58ms | transfer   150.77ms | handoff    0.15ms | total   151.55ms |  13.86 GB/s
  batch 7: queue     0.05ms | dispatch     0.37ms | transfer   153.14ms | handoff    0.19ms | total   153.75ms |  13.66 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.04       0.04       0.10
  dispatch       0.37       0.37       0.58
  transfer     150.54     153.14     156.81
  handoff        0.19       0.18       0.34
  total        151.14     153.75     157.40
  aggregate: 8 batches x 2.100GB in 1.21s = 13.88 GB/s wall-clock
CSV,get,pipeline,64,250,8400000,0.044,0.371,150.536,0.187,151.138,13.916,13.857,1,13.884

== op=get workers=64 keys=62 value=33.60MB batch=2.083GB mode=pipeline min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.02ms | dispatch     0.36ms | transfer   100.26ms | handoff    0.18ms | total   100.83ms |  20.66 GB/s
  batch 1: queue     0.04ms | dispatch     0.34ms | transfer   110.53ms | handoff    0.07ms | total   110.97ms |  18.77 GB/s
  batch 2: queue     0.04ms | dispatch     0.28ms | transfer   103.92ms | handoff    0.14ms | total   104.38ms |  19.96 GB/s
  batch 3: queue     0.02ms | dispatch     0.23ms | transfer   104.64ms | handoff    0.18ms | total   105.07ms |  19.83 GB/s
  batch 4: queue     0.06ms | dispatch     0.24ms | transfer   101.64ms | handoff    0.06ms | total   102.01ms |  20.42 GB/s
  batch 5: queue     0.04ms | dispatch     0.35ms | transfer   100.94ms | handoff    0.15ms | total   101.48ms |  20.53 GB/s
  batch 6: queue     0.06ms | dispatch     0.21ms | transfer   111.28ms | handoff    0.15ms | total   111.70ms |  18.65 GB/s
  batch 7: queue     0.12ms | dispatch     0.25ms | transfer    99.13ms | handoff    0.19ms | total    99.70ms |  20.90 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.05       0.04       0.12
  dispatch       0.28       0.28       0.36
  transfer     104.04     103.92     111.28
  handoff        0.14       0.15       0.19
  total        104.52     104.38     111.70
  aggregate: 8 batches x 2.083GB in 0.84s = 19.92 GB/s wall-clock
CSV,get,pipeline,64,62,33600000,0.051,0.283,104.043,0.14,104.516,19.964,20.422,1,19.92

== op=get workers=64 keys=500 value=4.20MB batch=2.100GB mode=pipeline min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.04ms | dispatch     1.22ms | transfer   188.27ms | handoff    0.47ms | total   190.01ms |  11.05 GB/s
  batch 1: queue     0.05ms | dispatch     6.48ms | transfer   209.08ms | handoff    0.20ms | total   215.81ms |   9.73 GB/s
  batch 2: queue     0.05ms | dispatch     0.55ms | transfer   160.19ms | handoff    0.17ms | total   160.96ms |  13.05 GB/s
  batch 3: queue     0.02ms | dispatch     1.08ms | transfer   167.64ms | handoff    0.19ms | total   168.93ms |  12.43 GB/s
  batch 4: queue     0.07ms | dispatch     0.75ms | transfer   173.39ms | handoff    0.17ms | total   174.37ms |  12.04 GB/s
  batch 5: queue     0.04ms | dispatch     2.25ms | transfer   177.48ms | handoff    0.18ms | total   179.95ms |  11.67 GB/s
  batch 6: queue     0.04ms | dispatch     1.10ms | transfer   179.92ms | handoff    0.14ms | total   181.20ms |  11.59 GB/s
  batch 7: queue     0.07ms | dispatch     0.92ms | transfer   162.66ms | handoff    0.14ms | total   163.78ms |  12.82 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.05       0.05       0.07
  dispatch       1.79       1.10       6.48
  transfer     177.33     177.48     209.08
  handoff        0.21       0.18       0.47
  total        179.38     179.95     215.81
  aggregate: 8 batches x 2.100GB in 1.44s = 11.69 GB/s wall-clock
CSV,get,pipeline,64,500,4200000,0.047,1.793,177.33,0.207,179.377,11.798,12.043,1,11.69

## 2. Flex spilled-keyspace point

### Flex, 4 shards

CSV,op,get_batch_mode,num_workers,num_keys,value_bytes,queue_ms,dispatch_ms,transfer_ms,handoff_ms,total_ms,gbps_mean,gbps_p50,inflight,agg_gbps

== op=get workers=64 keys=500 value=33.60MB batch=16.800GB mode=single min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.12ms | dispatch    27.84ms | transfer  3158.24ms | handoff    0.18ms | total  3186.38ms |   5.27 GB/s
  batch 1: queue     0.04ms | dispatch    27.96ms | transfer  3528.91ms | handoff    0.18ms | total  3557.09ms |   4.72 GB/s
  batch 2: queue     0.08ms | dispatch    27.92ms | transfer  2867.67ms | handoff    0.17ms | total  2895.84ms |   5.80 GB/s
  batch 3: queue     0.03ms | dispatch    27.82ms | transfer  3139.33ms | handoff    0.24ms | total  3167.42ms |   5.30 GB/s
  batch 4: queue     0.09ms | dispatch    27.97ms | transfer  3765.53ms | handoff    0.29ms | total  3793.89ms |   4.43 GB/s
  batch 5: queue     0.04ms | dispatch   178.28ms | transfer  2682.20ms | handoff    0.19ms | total  2860.71ms |   5.87 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.07       0.08       0.12
  dispatch      52.96      27.96     178.28
  transfer    3190.31    3158.24    3765.53
  handoff        0.21       0.19       0.29
  total       3243.56    3186.38    3793.89
  aggregate: 6 batches x 16.800GB in 19.46s = 5.18 GB/s wall-clock
CSV,get,single,64,500,33600000,0.067,52.965,3190.315,0.211,3243.557,5.234,5.304,1,5.179

== op=get workers=64 keys=500 value=33.60MB batch=16.800GB mode=pipeline min_keys_per_tile=8 target_tile_mb=32 inflight=1
  batch 0: queue     0.04ms | dispatch     1.03ms | transfer  2838.88ms | handoff    0.09ms | total  2840.04ms |   5.92 GB/s
  batch 1: queue     0.05ms | dispatch     1.13ms | transfer  2341.74ms | handoff    0.10ms | total  2343.01ms |   7.17 GB/s
  batch 2: queue     0.05ms | dispatch     1.07ms | transfer  3216.69ms | handoff    0.15ms | total  3217.95ms |   5.22 GB/s
  batch 3: queue     0.03ms | dispatch     1.23ms | transfer  2992.19ms | handoff    0.36ms | total  2993.81ms |   5.61 GB/s
  batch 4: queue     0.04ms | dispatch     0.92ms | transfer  2880.01ms | handoff    0.17ms | total  2881.13ms |   5.83 GB/s
  batch 5: queue     0.05ms | dispatch     0.81ms | transfer  2933.33ms | handoff    0.18ms | total  2934.36ms |   5.73 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.04       0.05       0.05
  dispatch       1.03       1.07       1.23
  transfer    2867.14    2933.33    3216.69
  handoff        0.17       0.17       0.36
  total       2868.38    2934.36    3217.95
  aggregate: 6 batches x 16.800GB in 17.21s = 5.86 GB/s wall-clock
CSV,get,pipeline,64,500,33600000,0.044,1.029,2867.139,0.172,2868.384,5.912,5.831,1,5.856

== op=get workers=64 keys=500 value=33.60MB batch=16.800GB mode=pipeline min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.08ms | dispatch     1.46ms | transfer  2854.23ms | handoff    0.17ms | total  2855.94ms |   5.88 GB/s
  batch 1: queue     0.06ms | dispatch     1.01ms | transfer  2554.04ms | handoff    0.25ms | total  2555.35ms |   6.57 GB/s
  batch 2: queue     0.08ms | dispatch     1.38ms | transfer  2493.41ms | handoff    0.16ms | total  2495.02ms |   6.73 GB/s
  batch 3: queue     0.12ms | dispatch     0.84ms | transfer  2339.20ms | handoff    0.17ms | total  2340.33ms |   7.18 GB/s
  batch 4: queue     0.07ms | dispatch     0.74ms | transfer  2306.93ms | handoff    0.15ms | total  2307.89ms |   7.28 GB/s
  batch 5: queue     0.04ms | dispatch     0.86ms | transfer  2277.04ms | handoff    0.16ms | total  2278.11ms |   7.37 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.08       0.08       0.12
  dispatch       1.05       1.01       1.46
  transfer    2470.81    2493.41    2854.23
  handoff        0.18       0.17       0.25
  total       2472.11    2495.02    2855.94
  aggregate: 6 batches x 16.800GB in 14.83s = 6.79 GB/s wall-clock
CSV,get,pipeline,64,500,33600000,0.075,1.048,2470.809,0.177,2472.109,6.837,7.178,1,6.795
































### Flex, 8 shards

CSV,op,get_batch_mode,num_workers,num_keys,value_bytes,queue_ms,dispatch_ms,transfer_ms,handoff_ms,total_ms,gbps_mean,gbps_p50,inflight,agg_gbps

== op=get workers=64 keys=500 value=33.60MB batch=16.800GB mode=single min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.07ms | dispatch   127.03ms | transfer  3057.02ms | handoff    0.18ms | total  3184.30ms |   5.28 GB/s
  batch 1: queue     0.03ms | dispatch   124.81ms | transfer  2998.84ms | handoff    0.16ms | total  3123.84ms |   5.38 GB/s
  batch 2: queue     0.04ms | dispatch   136.43ms | transfer  2891.94ms | handoff    0.21ms | total  3028.62ms |   5.55 GB/s
  batch 3: queue     0.03ms | dispatch   149.45ms | transfer  2794.87ms | handoff    0.09ms | total  2944.44ms |   5.71 GB/s
  batch 4: queue     0.04ms | dispatch   131.22ms | transfer  2657.64ms | handoff    0.17ms | total  2789.07ms |   6.02 GB/s
  batch 5: queue     0.05ms | dispatch   146.64ms | transfer  2733.11ms | handoff    0.22ms | total  2880.03ms |   5.83 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.04       0.04       0.07
  dispatch     135.93     136.43     149.45
  transfer    2855.57    2891.94    3057.02
  handoff        0.17       0.18       0.22
  total       2991.72    3028.62    3184.30
  aggregate: 6 batches x 16.800GB in 17.95s = 5.61 GB/s wall-clock
CSV,get,single,64,500,33600000,0.042,135.933,2855.568,0.173,2991.716,5.627,5.706,1,5.615



== op=get workers=64 keys=500 value=33.60MB batch=16.800GB mode=pipeline min_keys_per_tile=8 target_tile_mb=32 inflight=1
  batch 0: queue     0.06ms | dispatch     0.98ms | transfer  2715.64ms | handoff    0.18ms | total  2716.86ms |   6.18 GB/s
  batch 1: queue     0.04ms | dispatch     0.49ms | transfer  2415.12ms | handoff    0.16ms | total  2415.81ms |   6.95 GB/s
  batch 2: queue     0.04ms | dispatch     0.88ms | transfer  2148.87ms | handoff    0.19ms | total  2149.98ms |   7.81 GB/s
  batch 3: queue     0.04ms | dispatch     0.48ms | transfer  2129.49ms | handoff    0.09ms | total  2130.10ms |   7.89 GB/s
  batch 4: queue     0.05ms | dispatch     1.00ms | transfer  2393.48ms | handoff    0.09ms | total  2394.62ms |   7.02 GB/s
  batch 5: queue     0.04ms | dispatch     0.77ms | transfer  2198.91ms | handoff    0.09ms | total  2199.82ms |   7.64 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.05       0.04       0.06
  dispatch       0.77       0.88       1.00
  transfer    2333.59    2393.48    2715.64
  handoff        0.13       0.16       0.19
  total       2334.53    2394.62    2716.86
  aggregate: 6 batches x 16.800GB in 14.01s = 7.20 GB/s wall-clock
CSV,get,pipeline,64,500,33600000,0.047,0.766,2333.588,0.13,2334.532,7.249,7.637,1,7.195

== op=get workers=64 keys=500 value=33.60MB batch=16.800GB mode=pipeline min_keys_per_tile=1 target_tile_mb=32 inflight=1
  batch 0: queue     0.04ms | dispatch     1.61ms | transfer  2461.30ms | handoff    0.17ms | total  2463.13ms |   6.82 GB/s
  batch 1: queue     0.04ms | dispatch     0.72ms | transfer  2323.40ms | handoff    0.26ms | total  2324.42ms |   7.23 GB/s
  batch 2: queue     0.04ms | dispatch     1.32ms | transfer  2609.47ms | handoff    0.21ms | total  2611.04ms |   6.43 GB/s
  batch 3: queue     0.08ms | dispatch     0.33ms | transfer  2648.39ms | handoff    0.22ms | total  2649.01ms |   6.34 GB/s
  batch 4: queue     0.04ms | dispatch     0.55ms | transfer  2261.96ms | handoff    0.16ms | total  2262.71ms |   7.42 GB/s
  batch 5: queue     0.04ms | dispatch     0.81ms | transfer  2563.30ms | handoff    0.22ms | total  2564.37ms |   6.55 GB/s
  stage       mean        p50        p99   (ms)
  queue          0.05       0.04       0.08
  dispatch       0.89       0.81       1.61
  transfer    2477.97    2563.30    2648.39
  handoff        0.20       0.22       0.26
  total       2479.11    2564.37    2649.01
  aggregate: 6 batches x 16.800GB in 14.88s = 6.78 GB/s wall-clock
CSV,get,pipeline,64,500,33600000,0.046,0.891,2477.971,0.205,2479.112,6.8,6.821,1,6.776











































