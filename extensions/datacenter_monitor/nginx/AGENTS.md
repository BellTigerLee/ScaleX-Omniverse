<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-03 | Updated: 2026-06-03 -->

# nginx

## Purpose
Reverse-proxy config that fronts the three services of the running system behind a single host (port 3001), routing by path: Omniverse Kit WebRTC signaling, the query-server REST + WebSocket API, and the React dev server (with HMR). Moving services to a different node only requires editing the `upstream` blocks at the top.

## Key Files
| File | Description |
|------|-------------|
| `scalex-twin.conf` | The site config. Upstreams: `react_app` (127.0.0.1:3002), `query_server` (10.38.36.10:30183 NodePort), `omniverse_kit` (127.0.0.1:49100). Routes: `/sign_in` + `/webrtc/` → Kit (WebSocket upgrade); `/ws/` → query-server WS; `^/(metrics\|topology\|health\|replay\|events)` → query-server REST; everything else → React. `/turn-credentials` proxy is commented out (`[수정]` — direct WebRTC, no TURN) |

## For AI Agents

### Working In This Directory
- This file is deployed by copying to `/etc/nginx/sites-available/scalex-twin`, symlinking into `sites-enabled`, then `sudo nginx -t && sudo systemctl reload nginx` (steps are in the file header).
- When the React, query-server, or Kit signaling endpoint moves, change only the `upstream` `server` lines.
- Keep the WebSocket-upgrade headers (`Upgrade`/`Connection "upgrade"`, long `proxy_read_timeout`) on the WebRTC, `/ws/`, and React (HMR) locations.
- The `query_server` upstream here uses a NodePort address (`10.38.36.10:30183`) — distinct from the demo's localhost:8000. Match it to wherever the backend actually runs.

### Testing Requirements
- No automated tests. Validate with `sudo nginx -t`, then exercise each route (React load, `/health`, a `/ws/` connection, WebRTC `/sign_in`).

### Common Patterns
- Korean comments; deployment instructions and `[수정]` markers in the header.

## Dependencies

### Internal
- Fronts `ScaleX-Twin-Web` (React), the query-server, and the `datacenter_monitor` Kit WebRTC signaling.

### External
- nginx.

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
