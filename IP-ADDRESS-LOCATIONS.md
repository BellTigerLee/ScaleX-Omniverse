# IP Address Locations

Updated hardcoded cluster IP references to `10.30.0.233`.

## Current Matches For `10.30.0.233`

### ScaleX-Omniverse

- `extensions/datacenter_monitor/kafka-dummy/query-server/config.py:11`
  - `TRINO_HOST`
- `extensions/datacenter_monitor/kafka-dummy/docker-compose.yaml:231`
  - `KAFKA_BROKER`
- `extensions/datacenter_monitor/kafka-dummy/docker-compose.yaml:236`
  - `TRINO_HOST`
- `extensions/datacenter_monitor/config/AGENTS.md:14`
  - `env.cluster-poc` profile description
- `extensions/datacenter_monitor/config/env.cluster-poc:2`
  - commented `CLUSTER_HOST`
- `extensions/datacenter_monitor/config/env.cluster-poc:4`
  - commented `TOPOLOGY_URL`
- `extensions/datacenter_monitor/config/env.cluster-poc:8`
  - active `CLUSTER_HOST`
- `extensions/datacenter_monitor/config/env.cluster-poc:9`
  - commented `CLUSTER_HOST`
- `extensions/datacenter_monitor/config/env.cluster-poc:11`
  - active `TOPOLOGY_URL`
- `extensions/datacenter_monitor/config/env.cluster-poc:12`
  - commented `TOPOLOGY_URL`
- `extensions/datacenter_monitor/config/active:2`
  - same content via symlink to `env.cluster-poc`
- `extensions/datacenter_monitor/config/active:4`
  - same content via symlink to `env.cluster-poc`
- `extensions/datacenter_monitor/config/active:8`
  - same content via symlink to `env.cluster-poc`
- `extensions/datacenter_monitor/config/active:9`
  - same content via symlink to `env.cluster-poc`
- `extensions/datacenter_monitor/config/active:11`
  - same content via symlink to `env.cluster-poc`
- `extensions/datacenter_monitor/config/active:12`
  - same content via symlink to `env.cluster-poc`
- `extensions/datacenter_monitor/config/env.cluster-dev:2`
  - `CLUSTER_HOST`
- `extensions/datacenter_monitor/config/env.cluster-dev:4`
  - `TOPOLOGY_URL`
- `extensions/datacenter_monitor/config/env.example:15`
  - example `TOPOLOGY_URL`
- `extensions/datacenter_monitor/README.md:31`
  - example `CLUSTER_HOST`

### ScaleX-QueryServer

- `../ScaleX-QueryServer/query-server/config.py:12`
  - `TRINO_HOST`
- `../ScaleX-QueryServer/query-server/config.py:36`
  - `DUMMY_TOPOLOGY_URL`
- `../ScaleX-QueryServer/DUMMY-FEED-PLAN.md:17`
  - live topology URL
- `../ScaleX-QueryServer/DUMMY-FEED-PLAN.md:110`
  - `QS_DUMMY_TOPOLOGY_URL`

### ScaleX-Twin-Web

- `../ScaleX-Twin-Web/react-app/.env:1`
  - local `VITE_QUERY_SERVER`
- `../ScaleX-Twin-Web/react-app/.env:2`
  - local `VITE_TOPOLOGY_SERVER`
- `../ScaleX-Twin-Web/react-app/.env:3`
  - local `VITE_SIGNALING_SERVER`
- `../ScaleX-Twin-Web/react-app/src/OmniverseViewer.jsx:72`
  - commented `VITE_SIGNALING_SERVER`
- `../ScaleX-Twin-Web/react-app/.env.example:9`
  - commented `VITE_QUERY_SERVER`
- `../ScaleX-Twin-Web/react-app/.env.example:10`
  - commented `VITE_TOPOLOGY_SERVER`
- `../ScaleX-Twin-Web/react-app/.env.example:18`
  - commented `VITE_SIGNALING_SERVER`
