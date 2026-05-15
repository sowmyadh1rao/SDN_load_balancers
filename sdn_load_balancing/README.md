# SDN Load Balancer with Priority Queuing

A software-defined networking (SDN) load balancer built on the **Ryu OpenFlow controller** (OpenFlow 1.3). It distributes client traffic across three backend servers using Round Robin (RR) or Weighted Round Robin (WRR), with optional two-priority-queue QoS for real-time vs. non-real-time traffic.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Algorithms](#algorithms)
  - [Basic Round Robin](#basic-round-robin)
  - [Two Priority Queues](#two-priority-queues)
  - [RR + Priority Queues + Stats](#rr--priority-queues--stats)
- [Configuration](#configuration)
- [Requirements](#requirements)
- [Usage](#usage)


---

## Overview

This project implements a **virtual IP (VIP) load balancer** at the SDN layer. Clients connect to a single virtual IP (`10.0.0.100`), and the controller transparently distributes flows to three backend servers. The controller handles ARP replies, forward flow installation, and return path rewriting — all without clients knowing which server they are talking to.

Three variants are included:

| Variant | Algorithm | QoS | Stats Monitoring |
|---|---|---|---|
| Basic RR | Round Robin | None | No |
| Two Priority Queues | Round Robin | 2 priority queues (RT + NRT) | No |
| RR + Priority Queues + Stats | Weighted Round Robin (adaptive) | 2 priority queues (RT + NRT) | Yes (every 2s) |

---

## Architecture

```
Clients (h1, h2, h3)
        |
   [OVS Switch]  ←── Ryu Controller
        |
  ┌─────┼─────┐
 h4    h5    h6   (backend servers)
```

- **Virtual IP:** `10.0.0.100` / MAC `00:00:00:00:00:FE`
- **Client ports:** 1, 2, 3
- **Server ports:** 4 (h4), 5 (h5), 6 (h6)
- **Client subnet:** `172.168.1.0/24`
- **Server subnet:** `10.0.0.0/24`

---

## Algorithms

### Basic Round Robin

Each new flow is assigned to the next server in a fixed cycle (h4 → h5 → h6 → h4 → ...). Simple and lightweight with no load awareness.

### Two Priority Queues

Builds on Round Robin by classifying traffic into two OVS queues — real-time UDP (RTSP/RTP) goes to a high-priority queue, while standard TCP traffic (HTTP, FTP, etc.) goes to a lower-priority queue. This ensures latency-sensitive traffic is never starved by bulk transfers.

### RR + Priority Queues + Stats

Combines priority queuing with adaptive weighted scheduling. A background thread polls live flow statistics every 2 seconds and adjusts server weights dynamically — servers handling fewer active flows are preferred for new connections. This drives the distribution toward balance over time without requiring manual tuning.

---

## Configuration

Key constants at the top of each controller:

```python
VIRTUAL_IP  = '10.0.0.100'
VIRTUAL_MAC = '00:00:00:00:00:FE'

SERVERS = [
    {'ip': '10.0.0.4', 'mac': '00:00:00:00:00:04', 'port': 4, 'weight': 1},
    {'ip': '10.0.0.5', 'mac': '00:00:00:00:00:05', 'port': 5, 'weight': 1},
    {'ip': '10.0.0.6', 'mac': '00:00:00:00:00:06', 'port': 6, 'weight': 1},
]

REALTIME_PORTS     = {554, 5004, 5005}
NON_REALTIME_PORTS = {80, 443, 21, 20, 8080}
MONITOR_INTERVAL   = 2   # seconds between flow stat polls (WRR only)
```

Flow entries expire after **180 seconds of inactivity** (`idle_timeout=180`).



## Requirements

- Python 2.7 / 3.x
- [Ryu SDN Framework](https://ryu.readthedocs.io/)
- Open vSwitch (OVS) with OpenFlow 1.3 support
- Mininet (for emulation)

Install Ryu:

```bash
pip install ryu
```

---

## Usage

**Start the Ryu controller (via Docker):**

```bash
docker run --rm -it --network host \
    -v ~/workspace:/workspace \
    -e PYTHONPATH=/workspace \
    -w /workspace \
    osrg/ryu \
    ryu-manager prrlb --verbose --ofp-tcp-listen-port 6653 \
    2>&1 | tee ~/workspace/ryu.log
```

**Start the Mininet topology:**

```bash
sudo mn --controller=remote,ip=127.0.0.1,port=6653 --topo=...
```


