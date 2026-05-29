
---


# Kafka Topic
## datacenter.metrics
### 스키마 구조
``` json 
Key:  ecclab|work7 <- cluster|node
Value: 
{
	"ts": 1780042500000,
	"cluster": "ecclab",
	"node": "work7",
	"status": "HEALTHY",
	"metrics": {
		"cpu": {
			"util": 0.07789393166780227,
			"cores": 12.0,
			"load1": 0.45,
			"load5": 0.55,
			"load15": 0.62,
			"eff": 0.1737440758300054,
			"system": 0.014335089806762012,
			"user": 0.03915235990718268,
			"softirq": 0.006119326874036913,
			"load_norm": 0.0375,
			"iowait": 0.018287155079820646,
			"pressure": 0.006683950000054514,
			"thermal_core_throttle": 0.0,
			"thermal_package_throttle": 0.0,
			"container_throttle_ratio": 0.0
		},
		"mem": {
			"util": 0.25893741352609034,
			"stall": 0.0,
			"total_gb": 16.62281728,
			"avail_gb": 12.318547968,
			"swap_used_gb": 0.0,
			"major_page_fault_rate": 0.0,
			"pressure": 0.0,
			"cached_gb": 6.04729344,
			"dirty_gb": 0.00139264,
			"oom_cnt": 222
		},
		"net": {
			"retrans": 0.0,
			"in_mbps": 2.7246800740559896,
			"out_mbps": 6.274512227376302,
			"tcp_reset": 0.2,
			"bandwidth_util": 0.009436337066666666,
			"conntrack_util": 0.00131988525390625,
			"softnet_drop_rate": 0.0,
			"softnet_squeeze_rate": 0.23333333333333334,
			"syn_retrans_rate": 0.0,
			"nic_err_sum": 0.0,
			"nic_drop_sum": 0.0,
			"netstat_err": 0.0,
			"drop_sum": 0.0,
			"err_sum": 0.0,
			"rtt_p50_ms": 0.39255725,
			"rtt_p95_ms": 0.5149161250000001,
			"rtt_p99_ms": 0.5379218916666667
		},
		"gpu": {},
		"storage": {
			"util": 0.4446878260131386,
			"io_time_util": 0.9998333333292976,
			"inode_util": 0.06412109374999997,
			"read_mbps": 15.098046875,
			"write_mbps": 0.3685546875,
			"io_mbps": 15.4666015625,
			"read_iops": 128.86666666666667,
			"write_iops": 14.966666666666667,
			"read_latency_ms": 72.95887221935033,
			"write_latency_ms": 31.67594654772881,
			"queue_depth": 10.0,
			"io_pressure": 0.03760586666758172,
			"io_stall": 0.03558903333226529
		}
	},
	"conditions": {
		"ready": "true",
		"memory_pressure": 0,
		"disk_pressure": 0,
		"pid_pressure": 0,
		"network_unavailable": 0
	},
	"debug_ts": 1780042634586
}

```





## datacenter.node-topology
### 스키마 구조
``` json 
Key 없음
Value: 

{
	"snapshotTs": "2026-05-29T08:10:00Z",
	"cluster": "ecclab",
	"rack": "ecc",
	"node": "work7",
	"internalIp": "10.32.161.117",
	"cpuCapacityM": 12000,
	"cpuAllocatableM": 12000,
	"memCapacityBytes": 16622817280,
	"memAllocatableBytes": 16517959680,
	"podCapacity": 110,
	"taintsJson": "[]",
	"labelsJson": "{\"kubernetes_io_hostname\":\"work7\",\"rack\":\"ecc\",\"kubernetes_io_arch\":\"amd64\",\"node_group\":\"worker_node\",\"kubernetes_io_os\":\"linux\"}",
	"podCidr": "10.0.7.0/24"
}
```

## datacenter.pod-topology

### 스키마 구조

``` json
Key: 없음
Value: 

{
	"snapshotTs": "2026-05-29T08:10:00Z",
	"cluster": "ecclab",
	"rack": null,
	"node": "work2",
	"namespace": "jb-argo",
	"pod": "jb-argocd-redis-58c484cf4f-54s5z",
	"podUid": "6c1b9a0d-1d35-4289-a812-e6bde0d9ae36",
	"phase": "Running",
	"podIp": "10.0.1.118",
	"hostIp": "10.32.161.112",
	"hostNetwork": false,
	"createdByKind": "ReplicaSet",
	"createdByName": "jb-argocd-redis-58c484cf4f",
	"qosClass": "BestEffort",
	"priorityClass": null,
	"cpuRequestM": null,
	"cpuLimitM": null,
	"memRequestBytes": null,
	"memLimitBytes": null,
	"containerCount": 1,
	"restartCount": 0,
	"scheduledAt": "2026-03-19T16:20:32Z",
	"startedAt": "2026-03-19T16:20:32Z"
}

```

## datacenter.pod-metrics

### 스키마 구조

``` json

Key: 없음
Value: 

{
	"ts": 1780042920000,
	"cluster": "ecclab",
	"namespace": "observability",
	"pod": "dx-kafka-cluster-entity-operator-57bfc8d8f7-hgc55",
	"quality": "OK",
	"pod_uid": "7be1e0b9-26eb-44ec-8105-2909522ecb52",
	"window_start_ts": 1780042860000,
	"window_end_ts": 1780042920000,
	"sample_count": 68,
	"cpu_usage_rate": 0.01435170420494695,
	"cpu_load_avg_10s": 0.0,
	"mem_working_set_bytes": 5.1798016E8,
	"mem_limit_bytes": 0.0,
	"mem_rss": 5.10898176E8,
	"mem_anomaly_rate": 5.2948013184830875,
	"oom_events_delta": 0.0,
	"net_rx_rate": 300.97787470540646,
	"net_tx_rate": 373.3335678356608,
	"net_anomaly_delta": 0.0,
	"io_pressure_rate": 0.0
}
```


## datacenter.metrics.node-state.events

### 스키마 구조

``` json

{
	"kind": "snapshot",
	"scope": "node",
	"cluster": "ecclab",
	"node": "work5",
	"pod": null,
	"status": "HEALTHY",
	"reasons": [],
	"ts": 1780045294318,
	"state_since": 1780045144309,
	"previous_status": null,
	"last_seen_at": 1780045290288,
	"gap_sec": 4
}
```



## datacenter.metrics.stageab

### 스키마 구조 

``` json 
Key: 없음
Value: 

{
	"id": "cluster-rank",
	"cluster": "ecclab",
	"ts": 1780045200611,
	"ranking": [
		{
			"node": "work3",
			"rank": 1,
			"cpu_util": 0.11539773211627395
		},
		{
			"node": "work2",
			"rank": 2,
			"cpu_util": 0.10548396763625988
		},
		{
			"node": "work7",
			"rank": 3,
			"cpu_util": 0.08719720244110243
		},
		{
			"node": "work8",
			"rank": 4,
			"cpu_util": 0.06328519386879854
		},
		{
			"node": "work6",
			"rank": 5,
			"cpu_util": 0.03663519797102709
		},
		{
			"node": "work5",
			"rank": 6,
			"cpu_util": 0.024437461421917505
		},
		{
			"node": "work4",
			"rank": 7,
			"cpu_util": 0.0
		}
	]
}
```