# datacenter_monitor — Omniverse Extension

Kafka-driven datacenter digital twin for Omniverse. Consumes real-time node metrics from a k8s-hosted Kafka cluster and visualizes rack/node health on a USD scene.

## Prerequisites

- An Omniverse Kit app (Create, USD Composer, or similar).
- A reachable Kafka broker exposed via k8s `NodePort`. This extension connects as an **external client** — the k8s-internal DNS will not work here.
- Python packages inside Omniverse: `kafka-python`. Install with:
  ```bash
  <omniverse_python> -m pip install kafka-python
  ```

## Configuration — Endpoint Profiles

Cluster endpoints (host/port of the Kafka NodePort) live in `config/env.<profile>` files, not in code. Switching clusters is a one-action operation.

### Layout

```
config/
├── env.example        ← template, committed
├── env.cluster-dev    ← known cluster profile, committed
├── env.<yours>        ← add new ones here
└── active             ← symlink to the currently active profile (gitignored)
```

### Each profile file declares two values

```
CLUSTER_HOST=10.38.36.10     # k8s worker node IP reachable from this machine
KAFKA_NODEPORT=31327         # NodePort of the Kafka bootstrap Service
```

### Creating a new profile

```bash
cd extensions/datacenter_monitor/config
cp env.example env.cluster-lab
$EDITOR env.cluster-lab      # fill in CLUSTER_HOST and KAFKA_NODEPORT
```

### Activating a profile — two ways

**A. Symlink (recommended, persistent)**
```bash
cd extensions/datacenter_monitor/config
ln -sfn env.cluster-lab active
```

**B. Environment variable (one-off override)**
```bash
DC_PROFILE=cluster-lab <your omniverse launch command>
```

Resolution order: `DC_PROFILE` wins → `active` symlink → `env.default` → error.

## Run

1. Activate a profile (symlink or env var, see above).
2. Launch Omniverse and load the extension from `extensions/datacenter_monitor/` through the Extension Manager (Window → Extensions → "+" → point to this folder).
3. The extension will connect to `${CLUSTER_HOST}:${KAFKA_NODEPORT}` and begin consuming `datacenter.metrics`.

## Troubleshooting

| Symptom | Cause | Fix |
|--------|-------|-----|
| `FileNotFoundError: No endpoint profile found ...` | No `DC_PROFILE`, no `active` symlink, no `env.default` | Create a profile and either `ln -sfn env.<name> active` or `export DC_PROFILE=<name>`. |
| `FileNotFoundError: DC_PROFILE='X' but .../env.X does not exist` | `DC_PROFILE` points to a nonexistent file | Check `ls config/` — the file must be named `env.<profile>`. |
| `KeyError: Profile ... missing required key: KAFKA_NODEPORT` | Profile file incomplete | Add the missing key; see `env.example`. |
| `ValueError: ... KAFKA_NODEPORT must be an integer` | Typo in port | Fix the value in the profile file. |
| Extension loads but no metrics appear | Profile correct but broker unreachable | Verify `telnet $CLUSTER_HOST $KAFKA_NODEPORT` from this machine. |

## Related docs

- Code internals: [`datacenter_monitor_python/README.md`](datacenter_monitor_python/README.md)
- Local kafka-dummy services: [`kafka-dummy/README.md`](kafka-dummy/README.md)
- Design spec: `../../docs/superpowers/specs/2026-04-15-omniverse-endpoint-profile-design.md`
