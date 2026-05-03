from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel
import subprocess
import time
import csv
import os
import threading
import builtins
import random

VIRTUAL_IP      = '10.0.0.100'
VIRTUAL_MAC     = '00:00:00:00:00:FE'
CONTROLLER_PORT = 6653

REALTIME_PORTS     = {554, 5004, 5005}
NON_REALTIME_PORTS = {80, 443, 21, 20}

NUM_PORTS = 6
STATS_DIR = '/tmp'

# ─────────────────────────────────────────────────────────────────────────────
# Burst time range (seconds) — simulates varying flow durations
# This is the "burst time" concept from the paper applied to network flows.
# The Ryu controller will read flow duration_sec from the switch and compute:
#   tq = (maxBT + minBT + median) / 2
# then reweight servers accordingly.
# ─────────────────────────────────────────────────────────────────────────────
MIN_BURST_TIME = 5    # shortest flow duration (seconds)
MAX_BURST_TIME = 30   # longest  flow duration (seconds)

# Target bandwidth per flow — chosen to match plain RR total bytes.
# Plain RR uses: iperf -b 10m -t 60s → 10Mbps × 60s = 75MB per flow
# Burst flows normalize: bw = (TARGET_BW_MBPS × duration) / burst_time
# So a 5s flow runs at 120Mbps, a 30s flow at 20Mbps → same ~75MB total.
TARGET_BW_MBPS = 10   # must match plain RR's -b value

# ─────────────────────────────────────────────────────────────────────────────
# User counts — change these to scale the experiment.
# N_RT_USERS  : number of real-time  flows (UDP:5004) from h1
# N_NRT_USERS : number of non-real-time flows (TCP:80) split across h2 and h3
# Total users = N_RT_USERS + N_NRT_USERS
# ─────────────────────────────────────────────────────────────────────────────
N_RT_USERS  = 50   # RT  flows from h1  (UDP:5004)
N_NRT_USERS = 50   # NRT flows total    (TCP:80) — split evenly h2/h3


# ─────────────────────────────────────────────────────────────────────────────
# Controller IP
# ─────────────────────────────────────────────────────────────────────────────

def get_controller_ip(container_name='ryu'):
    try:
        ip = subprocess.check_output(
            "docker inspect -f '{{.NetworkSettings.IPAddress}}' " + container_name,
            shell=True, stderr=subprocess.DEVNULL
        ).decode().strip()
        if ip:
            print("*** Ryu container found at %s" % ip)
            return ip
        print("*** Docker using host network — using 127.0.0.1")
        return '127.0.0.1'
    except subprocess.CalledProcessError:
        print("*** Docker container not found — using 127.0.0.1")
        return '127.0.0.1'


# ─────────────────────────────────────────────────────────────────────────────
# Queue Setup
# ─────────────────────────────────────────────────────────────────────────────

def setup_queues(num_ports=NUM_PORTS):
    print("*** Configuring single queue on all %d switch ports" % num_ports)
    for i in range(1, num_ports + 1):
        cmd = (
            "ovs-vsctl -- set Port s1-eth%d qos=@newqos%d "
            "-- --id=@newqos%d create QoS type=linux-htb "
            "   other-config:max-rate=1000000000 "
            "   queues=0=@q0_%d "
            "-- --id=@q0_%d create Queue "
            "   other-config:min-rate=1000000000 "
            "   other-config:max-rate=1000000000"
        ) % (i, i, i, i, i)
        result = subprocess.call(cmd, shell=True)
        if result != 0:
            print("    [ERROR] QoS failed on s1-eth%d" % i)
        else:
            print("    s1-eth%d -> single queue 1Gbps" % i)
    print("*** Single queue configured on all ports")


# ─────────────────────────────────────────────────────────────────────────────
# Server Startup
# ─────────────────────────────────────────────────────────────────────────────

def start_servers(servers):
    print("*** Starting iperf servers on h4, h5, h6")
    for s in servers:
        s.cmd('killall iperf 2>/dev/null')
        time.sleep(0.5)
        s.cmd('iperf -s -u -p 5004 > /tmp/%s_udp.log 2>&1 &' % s.name)
        s.cmd('iperf -s -u -p 554  > /tmp/%s_554.log 2>&1 &' % s.name)
        s.cmd('iperf -s    -p 80   > /tmp/%s_tcp.log 2>&1 &' % s.name)
        print("    %s: UDP(5004) + UDP(554) + TCP(80)" % s.name)
    time.sleep(1)


# ─────────────────────────────────────────────────────────────────────────────
# Random Burst Time Traffic Generation
# Each flow gets a RANDOM duration between MIN_BURST_TIME and MAX_BURST_TIME.
# This gives the Ryu controller varying flow durations to use as "burst times"
# for the paper's tq formula: tq = (maxBT + minBT + median) / 2
# ─────────────────────────────────────────────────────────────────────────────

def _launch_random_burst_flows(client, vip, port, proto, n_flows, max_duration, log_prefix):
    """
    Launch n_flows iperf connections each with a RANDOM duration.
    Bandwidth per flow is adjusted so total bytes per flow stays constant:
        target_bytes = TARGET_BW_MBPS * max_duration
        bw = target_bytes / burst_time
    This makes total bytes comparable to plain RR (which uses fixed -t and -b).
    Burst times logged to /tmp/<log_prefix>_bursts.txt.
    """
    burst_log = '/tmp/%s_bursts.txt' % log_prefix
    with open(burst_log, 'w') as f:
        f.write('flow_id,burst_time_sec,bw_mbps,proto,port\n')

    def _launch():
        with open(burst_log, 'a') as f:
            for i in range(n_flows):
                burst = random.randint(MIN_BURST_TIME, min(MAX_BURST_TIME, max_duration))
                # Normalize: bw × burst = TARGET_BW_MBPS × max_duration
                # So short flows run fast, long flows run slow — same total bytes
                bw_mbps = (TARGET_BW_MBPS * max_duration) / float(burst)
                bw_str  = '%.2fm' % bw_mbps
                f.write('%d,%d,%.2f,%s,%d\n' % (i, burst, bw_mbps, proto, port))
                f.flush()
                if proto == 'udp':
                    client.cmd(
                        'iperf -c %s -u -p %d -b %s -t %d '
                        '> /tmp/%s_flow%d.txt 2>&1 &'
                        % (vip, port, bw_str, burst, log_prefix, i))
                else:
                    # TCP doesn't have -b but we can limit with traffic shaping
                    # Use -t only; TCP will use available bandwidth naturally
                    client.cmd(
                        'iperf -c %s -p %d -t %d '
                        '> /tmp/%s_flow%d.txt 2>&1 &'
                        % (vip, port, burst, log_prefix, i))
                time.sleep(0.5)  # stagger flow starts

    t = threading.Thread(target=_launch)
    t.daemon = True
    t.start()
    print("    Launching %d %s flows from %s -> VIP:%d (burst %d-%ds, normalized to ~%dMbps*%ds each)" % (
        n_flows, proto.upper(), client.name, port,
        MIN_BURST_TIME, MAX_BURST_TIME,
        TARGET_BW_MBPS, max_duration))
    return burst_log


def simulate_traffic(clients, n_rt_users=50, n_nrt_users=50, duration=60):
    """
    Simulate traffic with RANDOM burst times per flow.
    Each flow independently picks a random duration so the switch
    accumulates flows with varying duration_sec values for the
    controller's tq computation.
    """
    print("*** Simulating %d RT + %d NRT users with random burst times (%d-%ds)" % (
          n_rt_users, n_nrt_users, MIN_BURST_TIME, MAX_BURST_TIME))

    n         = len(clients)
    rt_split  = [n_rt_users  // n] * n
    nrt_split = [n_nrt_users // n] * n
    rt_split[0]  += n_rt_users  % n
    nrt_split[0] += n_nrt_users % n

    for i, client in enumerate(clients):
        rt  = rt_split[i]
        nrt = nrt_split[i]

        if rt > 0:
            _launch_random_burst_flows(
                client, VIRTUAL_IP, 5004, 'udp', rt,
                duration, '%s_rt' % client.name)

        if nrt > 0:
            _launch_random_burst_flows(
                client, VIRTUAL_IP, 80, 'tcp', nrt,
                duration, '%s_nrt' % client.name)

        print("    %s: %d RT (UDP:5004)  +  %d NRT (TCP:80)" % (
              client.name, rt, nrt))

    print("*** Traffic running with random burst times")
    print("    Burst logs: /tmp/h1_rt_bursts.txt, /tmp/h2_nrt_bursts.txt, etc.")


# ─────────────────────────────────────────────────────────────────────────────
# Host Stats Collector
# ─────────────────────────────────────────────────────────────────────────────

def _collect_host_stats(net, duration, interval):
    def _poll():
        h1 = net.get('h1'); h2 = net.get('h2'); h3 = net.get('h3')
        h4 = net.get('h4'); h5 = net.get('h5'); h6 = net.get('h6')

        with open('/tmp/host_stats.txt', 'w') as f:
            f.write('elapsed_sec,host,rx_packets,tx_packets,rx_bytes,tx_bytes\n')
            start = time.time()
            while True:
                elapsed = round(time.time() - start, 1)
                if elapsed > duration:
                    break
                for h in [h1, h2, h3, h4, h5, h6]:
                    stats = h.cmd('cat /proc/net/dev | grep %s-eth0' % h.name)
                    if stats:
                        parts = stats.split()
                        try:
                            rx_bytes   = parts[1]
                            rx_packets = parts[2]
                            tx_bytes   = parts[9]
                            tx_packets = parts[10]
                            f.write('%s,%s,%s,%s,%s,%s\n' % (
                                elapsed, h.name,
                                rx_packets, tx_packets,
                                rx_bytes, tx_bytes))
                        except IndexError:
                            pass
                f.flush()
                time.sleep(interval)

    t = threading.Thread(target=_poll)
    t.daemon = True
    t.start()


# ─────────────────────────────────────────────────────────────────────────────
# Server Stats Poller
# ─────────────────────────────────────────────────────────────────────────────

def _poll_server_stats(net, duration, interval):
    def _poll():
        h4 = net.get('h4')
        h5 = net.get('h5')
        h6 = net.get('h6')

        with open('/tmp/server_debug.txt', 'w') as f:
            f.write('elapsed_sec,server,rx_pkts,tx_pkts,rx_bytes,tx_bytes\n')
            start = time.time()
            while True:
                elapsed = round(time.time() - start, 1)
                if elapsed > duration:
                    break
                for h in [h4, h5, h6]:
                    stats = h.cmd('cat /proc/net/dev | grep %s-eth0' % h.name)
                    if stats:
                        parts = stats.split()
                        try:
                            rx_bytes = int(parts[1])
                            rx_pkts  = int(parts[2])
                            tx_bytes = int(parts[9])
                            tx_pkts  = int(parts[10])
                            f.write('%s,%s,%d,%d,%d,%d\n' % (
                                elapsed, h.name,
                                rx_pkts, tx_pkts,
                                rx_bytes, tx_bytes))
                            print("  [SERVER] t=%5.1fs  %s  rx=%6d pkts  tx=%6d pkts  rx=%6dKB  tx=%6dKB" % (
                                elapsed, h.name,
                                rx_pkts, tx_pkts,
                                rx_bytes//1000, tx_bytes//1000))
                        except (IndexError, ValueError):
                            pass
                f.flush()
                time.sleep(interval)

    t = threading.Thread(target=_poll)
    t.daemon = True
    t.start()
    print("    Server stats polling started -> /tmp/server_debug.txt")


# ─────────────────────────────────────────────────────────────────────────────
# Client Stats Poller
# ─────────────────────────────────────────────────────────────────────────────

def _poll_client_stats(net, duration, interval):
    def _poll():
        h1 = net.get('h1')
        h2 = net.get('h2')
        h3 = net.get('h3')

        with open('/tmp/client_stats.txt', 'w') as f:
            f.write('elapsed_sec,client,tx_pkts,rx_pkts,tx_bytes,rx_bytes,drop\n')
            start = time.time()
            while True:
                elapsed = round(time.time() - start, 1)
                if elapsed > duration:
                    break
                for h in [h1, h2, h3]:
                    stats = h.cmd('cat /proc/net/dev | grep %s-eth0' % h.name)
                    if stats:
                        parts = stats.split()
                        try:
                            rx_bytes = int(parts[1])
                            rx_pkts  = int(parts[2])
                            rx_drop  = int(parts[4])
                            tx_bytes = int(parts[9])
                            tx_pkts  = int(parts[10])
                            tx_drop  = int(parts[12])
                            f.write('%s,%s,%d,%d,%d,%d,%d\n' % (
                                elapsed, h.name,
                                tx_pkts, rx_pkts,
                                tx_bytes, rx_bytes,
                                tx_drop + rx_drop))
                            print("  [CLIENT] t=%5.1fs  %s  tx=%6d pkts  rx=%6d pkts  tx=%6dKB  drop=%d" % (
                                elapsed, h.name,
                                tx_pkts, rx_pkts,
                                tx_bytes//1000,
                                tx_drop + rx_drop))
                        except (IndexError, ValueError):
                            pass
                f.flush()
                time.sleep(interval)

    t = threading.Thread(target=_poll)
    t.daemon = True
    t.start()
    print("    Client stats polling started -> /tmp/client_stats.txt")


# ─────────────────────────────────────────────────────────────────────────────
# Experiment
# ─────────────────────────────────────────────────────────────────────────────

def run_experiment(net, duration=60, interval=5):
    h1 = net.get('h1'); h2 = net.get('h2'); h3 = net.get('h3')
    h4 = net.get('h4'); h5 = net.get('h5'); h6 = net.get('h6')

    print("*** Cleaning up old processes")
    for h in [h1, h2, h3, h4, h5, h6]:
        h.cmd('killall iperf 2>/dev/null')
        h.cmd('killall ping  2>/dev/null')
    time.sleep(1)

    print("*** Cleared stale flows from switch")
    time.sleep(2)

    start_servers([h4, h5, h6])
    time.sleep(2)

    # Split NRT flows evenly across h2 and h3
    n_nrt_h2 = N_NRT_USERS // 2
    n_nrt_h3 = N_NRT_USERS - n_nrt_h2  # gets the extra 1 if odd
    num_users = N_RT_USERS + N_NRT_USERS

    print("*** Step 1 — Starting traffic with RANDOM burst times (%d-%ds)" % (
          MIN_BURST_TIME, MAX_BURST_TIME))
    print("    RT users : %d (h1 UDP:5004)" % N_RT_USERS)
    print("    NRT users: %d (h2=%d + h3=%d TCP:80)" % (N_NRT_USERS, n_nrt_h2, n_nrt_h3))
    print("    Total    : %d users" % num_users)

    # RT traffic: h1 → VIP:5004 UDP with random burst time per flow
    _launch_random_burst_flows(
        h1, VIRTUAL_IP, 5004, 'udp', N_RT_USERS, duration, 'h1_rt')
    print("    RT  traffic  h1 -> VIP:5004  (random burst, %d flows)" % N_RT_USERS)

    # NRT traffic: h2,h3 → VIP:80 TCP with random burst time per flow
    _launch_random_burst_flows(
        h2, VIRTUAL_IP, 80, 'tcp', n_nrt_h2, duration, 'h2_nrt')
    print("    NRT traffic  h2 -> VIP:80    (random burst, %d flows)" % n_nrt_h2)

    _launch_random_burst_flows(
        h3, VIRTUAL_IP, 80, 'tcp', n_nrt_h3, duration, 'h3_nrt')
    print("    NRT traffic  h3 -> VIP:80    (random burst, %d flows)" % n_nrt_h3)

    _poll_stats_to_file(duration, interval)
    print("    Queue+flow stats polling started")

    _poll_server_stats(net, duration, interval)
    _poll_client_stats(net, duration, interval)

    print("*** Step 2 — Waiting %ds for traffic to finish..." % duration)
    for remaining in range(duration + 5, 0, -5):
        time.sleep(5)
        print("    %ds remaining..." % max(0, remaining - 5))
    print("    Traffic finished")

    print("*** Step 3 — Parsing raw files into CSVs")
    _parse_rt_iperf  ('/tmp/h1_rt_flow0.txt')
    _parse_nrt_iperf ('/tmp/h2_nrt_flow0.txt')
    _parse_ping      ('/tmp/ping.txt')
    _parse_queue_stats('/tmp/queue_raw.txt')
    _parse_flow_stats ('/tmp/flow_raw.txt')

    # Print burst time summary
    print("\n*** Burst Time Summary (flow durations sent to switch):")
    for log in ['/tmp/h1_rt_bursts.txt', '/tmp/h2_nrt_bursts.txt', '/tmp/h3_nrt_bursts.txt']:
        try:
            with open(log) as f:
                lines = f.readlines()[1:]  # skip header
                bursts = [int(l.split(',')[1]) for l in lines if l.strip()]
                if bursts:
                    print("    %s: min=%ds  max=%ds  median=%.1fs  count=%d" % (
                        log.split('/')[-1],
                        min(bursts), max(bursts),
                        sorted(bursts)[len(bursts)//2],
                        len(bursts)))
        except IOError:
            pass

    print("\n*** Experiment complete. CSV files in %s:" % STATS_DIR)
    print("    rt_throughput.csv   — RT  Mbps over time")
    print("    nrt_throughput.csv  — NRT Mbps over time")
    print("    jitter_loss.csv     — RT  jitter and packet loss")
    print("    latency.csv         — ping latency over time")
    print("    queue_stats.csv     — queue bytes over time")
    print("    loadbalance.csv     — flows per server over time")
    print("    *_bursts.txt        — random burst times per client")
    print("\n    Plot with: python3 plot_results.py")

    # ── Final Metrics Summary ─────────────────────────────────────────────────
    print("\n" + "="*60)
    print("  FINAL METRICS SUMMARY (after %ds)" % duration)
    print("="*60)

    try:
        client_tx_bytes = {'h1': 0, 'h2': 0, 'h3': 0}
        client_tx_pkts  = {'h1': 0, 'h2': 0, 'h3': 0}
        client_rx_pkts  = {'h1': 0, 'h2': 0, 'h3': 0}
        client_drop     = {'h1': 0, 'h2': 0, 'h3': 0}
        server_rx_bytes = {'h4': 0, 'h5': 0, 'h6': 0}
        server_rx_pkts  = {'h4': 0, 'h5': 0, 'h6': 0}
        server_tx_pkts  = {'h4': 0, 'h5': 0, 'h6': 0}

        for name, h in [('h1', h1), ('h2', h2), ('h3', h3)]:
            stats = h.cmd('cat /proc/net/dev | grep %s-eth0' % name)
            if stats:
                parts = stats.split()
                try:
                    client_tx_bytes[name] = int(parts[9])
                    client_tx_pkts[name]  = int(parts[10])
                    client_rx_pkts[name]  = int(parts[2])
                    client_drop[name]     = int(parts[4]) + int(parts[12])
                except (IndexError, ValueError):
                    pass

        for name, h in [('h4', h4), ('h5', h5), ('h6', h6)]:
            stats = h.cmd('cat /proc/net/dev | grep %s-eth0' % name)
            if stats:
                parts = stats.split()
                try:
                    server_rx_bytes[name] = int(parts[1])
                    server_rx_pkts[name]  = int(parts[2])
                    server_tx_pkts[name]  = int(parts[10])
                except (IndexError, ValueError):
                    pass

        total_tx_bytes  = sum(client_tx_bytes.values())
        throughput_mbps = (total_tx_bytes * 8) / (duration * 1000000)

        total_client_tx = sum(client_tx_pkts.values())
        total_server_rx = sum(server_rx_pkts.values())
        packet_loss     = max(0, total_client_tx - total_server_rx)
        loss_pct        = (packet_loss / total_client_tx * 100) if total_client_tx > 0 else 0

        total_server_tx  = sum(server_tx_pkts.values())
        transaction_rate = total_server_tx / duration if duration > 0 else 0

        num_users     = N_RT_USERS + N_NRT_USERS
        response_time = (total_server_rx / transaction_rate) / num_users if transaction_rate > 0 else 0

        total_rx = sum(server_rx_pkts.values())
        h4_pct   = (server_rx_pkts['h4'] / total_rx * 100) if total_rx > 0 else 0
        h5_pct   = (server_rx_pkts['h5'] / total_rx * 100) if total_rx > 0 else 0
        h6_pct   = (server_rx_pkts['h6'] / total_rx * 100) if total_rx > 0 else 0

        try:
            flows       = subprocess.check_output(
                'ovs-ofctl -O OpenFlow13 dump-flows s1 2>/dev/null',
                shell=True).decode()
            h4_flows    = flows.count('10.0.0.4')
            h5_flows    = flows.count('10.0.0.5')
            h6_flows    = flows.count('10.0.0.6')
            total_flows = h4_flows + h5_flows + h6_flows
        except subprocess.CalledProcessError:
            h4_flows = h5_flows = h6_flows = total_flows = 0

        print("\n  1. THROUGHPUT")
        print("     Total bytes sent by clients : %d bytes" % total_tx_bytes)
        print("     Duration                    : %d sec" % duration)
        print("     Throughput                  : %.4f Mbps" % throughput_mbps)
        print("     (formula: total_bits / duration / 1000000)")

        print("\n  2. RESPONSE TIME")
        print("     Total server rx pkts        : %d" % total_server_rx)
        print("     Transaction rate            : %.2f resp/sec" % transaction_rate)
        print("     Number of users             : %d" % num_users)
        print("     Response Time               : %.4f sec" % response_time)
        print("     (formula: (total_server_rx / transaction_rate) / num_users)")

        print("\n  3. TRANSACTION RATE")
        print("     Total server responses      : %d packets" % total_server_tx)
        print("     Duration                    : %d sec" % duration)
        print("     Transaction Rate            : %.2f responses/sec" % transaction_rate)
        print("     (formula: total_responses / duration)")

        print("\n  4. PACKET LOSS")
        print("     Client sent                 : %d pkts" % total_client_tx)
        print("     Server received             : %d pkts" % total_server_rx)
        print("     Packet loss                 : %d pkts (%.2f%%)" % (packet_loss, loss_pct))

        print("\n  5. LOAD BALANCE DISTRIBUTION")
        print("     h4 received : %d pkts (%.1f%%)" % (server_rx_pkts['h4'], h4_pct))
        print("     h5 received : %d pkts (%.1f%%)" % (server_rx_pkts['h5'], h5_pct))
        print("     h6 received : %d pkts (%.1f%%)" % (server_rx_pkts['h6'], h6_pct))
        print("     Ideal balance would be 33.3%% each")

        print("\n  6. FLOW DISTRIBUTION")
        print("     Each connection installs 1 forward flow")
        print("     h4 flows : %d connections" % h4_flows)
        print("     h5 flows : %d connections" % h5_flows)
        print("     h6 flows : %d connections" % h6_flows)
        print("     Total    : %d connections  (ideal: %d each)" % (
            total_flows, total_flows // 3 if total_flows > 0 else 0))

        print("\n" + "="*60)

        with open('/tmp/final_metrics.txt', 'w') as mf:
            mf.write("FINAL METRICS — %ds experiment\n" % duration)
            mf.write("="*60 + "\n")
            mf.write("Throughput      : %.4f Mbps\n" % throughput_mbps)
            mf.write("Response Time   : %.4f sec\n"  % response_time)
            mf.write("Transaction Rate: %.2f resp/sec\n" % transaction_rate)
            mf.write("Packet Loss     : %d pkts (%.2f%%)\n" % (packet_loss, loss_pct))
            mf.write("Load Balance    : h4=%.1f%% h5=%.1f%% h6=%.1f%%\n" % (
                h4_pct, h5_pct, h6_pct))
            mf.write("Flow Dist       : h4=%d h5=%d h6=%d total=%d\n" % (
                h4_flows, h5_flows, h6_flows, total_flows))
        print("    Saved to /tmp/final_metrics.txt")

    except Exception as e:
        print("    [WARN] Could not compute metrics: %s" % e)


# ─────────────────────────────────────────────────────────────────────────────
# Background Stats Poller
# ─────────────────────────────────────────────────────────────────────────────

def _poll_stats_to_file(duration, interval):
    def _poll():
        q_file = open('/tmp/queue_raw.txt', 'w')
        f_file = open('/tmp/flow_raw.txt',  'w')
        p_file = open('/tmp/port_stats.txt', 'w')

        q_file.write('elapsed_sec,q0_bytes,q0_packets\n')
        f_file.write('elapsed_sec,h4_flows,h5_flows,h6_flows,total\n')
        p_file.write('elapsed_sec,port,tx_packets,tx_bytes,rx_packets,rx_bytes\n')
        q_file.flush(); f_file.flush(); p_file.flush()

        start = time.time()
        while True:
            elapsed = round(time.time() - start, 1)
            if elapsed > duration:
                break

            q0_bytes = 0; q0_pkts = 0
            for eth in ['s1-eth4', 's1-eth5', 's1-eth6']:
                try:
                    tc_out = subprocess.check_output(
                        'tc -s class show dev %s 2>/dev/null' % eth,
                        shell=True).decode()
                    current = None
                    for line in tc_out.splitlines():
                        if 'class htb' in line:
                            current = 'q0'
                        if 'Sent' in line and current:
                            parts = line.split()
                            try:
                                q0_bytes += int(parts[1])
                                q0_pkts  += int(parts[3])
                            except (IndexError, ValueError):
                                pass
                except subprocess.CalledProcessError:
                    pass

            q_file.write('%s,%d,%d\n' % (elapsed, q0_bytes, q0_pkts))
            q_file.flush()

            h4_count = 0; h5_count = 0; h6_count = 0
            try:
                flows = subprocess.check_output(
                    'ovs-ofctl -O OpenFlow13 dump-flows s1 2>/dev/null',
                    shell=True).decode()
                h4_count = flows.count('10.0.0.4')
                h5_count = flows.count('10.0.0.5')
                h6_count = flows.count('10.0.0.6')
            except subprocess.CalledProcessError:
                pass

            total = h4_count + h5_count + h6_count
            f_file.write('%s,%d,%d,%d,%d\n' % (
                elapsed, h4_count, h5_count, h6_count, total))
            f_file.flush()

            try:
                port_out = subprocess.check_output(
                    'ovs-ofctl -O OpenFlow13 dump-ports s1 2>/dev/null',
                    shell=True).decode()
                current_port = None
                tx_pkts = tx_bytes = rx_pkts = rx_bytes = 0
                for line in port_out.splitlines():
                    if 'port' in line and ':' in line:
                        current_port = line.strip().split()[1].strip(':')
                    if 'tx' in line and current_port:
                        parts = line.split(',')
                        try:
                            tx_pkts  = parts[0].split('=')[1].strip()
                            tx_bytes = parts[1].split('=')[1].strip()
                        except IndexError:
                            pass
                    if 'rx' in line and current_port:
                        parts = line.split(',')
                        try:
                            rx_pkts  = parts[0].split('=')[1].strip()
                            rx_bytes = parts[1].split('=')[1].strip()
                            p_file.write('%s,%s,%s,%s,%s,%s\n' % (
                                elapsed, current_port,
                                tx_pkts, tx_bytes,
                                rx_pkts, rx_bytes))
                        except IndexError:
                            pass
                p_file.flush()
            except subprocess.CalledProcessError:
                pass

            print("  [poll] t=%5.1fs  Q0=%6dKB  flows h4=%d h5=%d h6=%d" % (
                  elapsed, q0_bytes // 1000,
                  h4_count, h5_count, h6_count))

            time.sleep(interval)

        q_file.close()
        f_file.close()
        p_file.close()

    t = threading.Thread(target=_poll)
    t.daemon = True
    t.start()


# ─────────────────────────────────────────────────────────────────────────────
# Raw File Parsers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_rt_iperf(filepath):
    throughput_rows = [['elapsed_sec', 'rt_mbps']]
    jitter_rows     = [['elapsed_sec', 'jitter_ms', 'loss_percent']]
    try:
        with open(filepath) as f:
            for line in f:
                if 'sec' in line and '-' in line and 'Mbits/sec' in line and '[SUM]' not in line:
                    try:
                        parts = line.split()
                        t     = int(float(parts[2].replace('-', '')))
                        mbps  = float(parts[5])
                        throughput_rows.append([t, mbps])
                        if 'ms' in line and '/' in line:
                            jitter   = float(parts[8])
                            loss_str = parts[11].strip('()%')
                            jitter_rows.append([t, jitter, float(loss_str)])
                    except (IndexError, ValueError):
                        pass
    except IOError:
        print("    [WARN] %s not found" % filepath)
        return
    _save_csv('rt_throughput.csv', throughput_rows)
    _save_csv('jitter_loss.csv',   jitter_rows)


def _parse_nrt_iperf(filepath):
    rows = [['elapsed_sec', 'nrt_mbps']]
    try:
        with open(filepath) as f:
            for line in f:
                if '[SUM]' in line and 'sec' in line and '-' in line:
                    try:
                        parts = line.split()
                        t     = int(float(parts[2].replace('-', '')))
                        mbps  = float(parts[5])
                        unit  = parts[6]
                        if 'Gbits' in unit:
                            mbps = mbps * 1000
                        rows.append([t, mbps])
                    except (IndexError, ValueError):
                        pass
    except IOError:
        print("    [WARN] %s not found" % filepath)
        return
    _save_csv('nrt_throughput.csv', rows)


def _parse_ping(filepath):
    rows = [['seq', 'latency_ms']]
    try:
        with open(filepath) as f:
            for line in f:
                if 'icmp_seq' in line and 'time=' in line:
                    try:
                        seq     = line.split('icmp_seq=')[1].split()[0]
                        latency = line.split('time=')[1].split()[0]
                        rows.append([seq, latency])
                    except (IndexError, ValueError):
                        pass
    except IOError:
        print("    [WARN] %s not found" % filepath)
        return
    _save_csv('latency.csv', rows)


def _parse_queue_stats(filepath):
    _copy_raw_to_csv(filepath, 'queue_stats.csv')


def _parse_flow_stats(filepath):
    _copy_raw_to_csv(filepath, 'loadbalance.csv')


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _save_csv(filename, rows):
    path = os.path.join(STATS_DIR, filename)
    with open(path, 'w', newline='') as f:
        csv.writer(f).writerows(rows)
    print("    Saved %s  (%d data rows)" % (path, len(rows) - 1))


def _copy_raw_to_csv(src, dest_filename):
    try:
        with open(src) as f:
            data = f.read()
        path = os.path.join(STATS_DIR, dest_filename)
        with open(path, 'w') as f:
            f.write(data)
        lines = data.strip().splitlines()
        print("    Saved %s  (%d data rows)" % (path, max(0, len(lines) - 1)))
    except IOError:
        print("    [WARN] %s not found" % src)


# ─────────────────────────────────────────────────────────────────────────────
# Debug helpers
# ─────────────────────────────────────────────────────────────────────────────

def debug_udp_flows(duration=60, interval=2, logfile='/tmp/udp_debug.txt'):
    def _watch():
        with open(logfile, 'w') as f:
            f.write("="*60 + "\n")
            f.write("UDP FLOW DEBUG LOG\n")
            f.write("="*60 + "\n")
            f.flush()
            start = time.time()
            while True:
                elapsed = round(time.time() - start, 1)
                if elapsed > duration:
                    break
                f.write("\n[t=%ss]\n" % elapsed)
                try:
                    flows = subprocess.check_output(
                        'ovs-ofctl -O OpenFlow13 dump-flows s1 2>/dev/null',
                        shell=True).decode()
                    udp_flows = [line for line in flows.splitlines()
                                 if 'udp' in line.lower() or 'proto=17' in line]
                    f.write("--- UDP Flows installed (%d found) ---\n" % len(udp_flows))
                    if udp_flows:
                        for flow in udp_flows:
                            f.write("  " + flow.strip() + "\n")
                    else:
                        f.write("  [NONE] No UDP flows in switch table yet\n")
                    f.write("\n--- ALL Flows (for reference) ---\n")
                    for line in flows.splitlines():
                        f.write("  " + line.strip() + "\n")
                except subprocess.CalledProcessError as e:
                    f.write("  [ERROR] dump-flows failed: %s\n" % str(e))
                try:
                    ports = subprocess.check_output(
                        'ovs-ofctl -O OpenFlow13 dump-ports s1 2>/dev/null',
                        shell=True).decode()
                    f.write("\n--- Port Stats ---\n")
                    for line in ports.splitlines():
                        f.write("  " + line.strip() + "\n")
                except subprocess.CalledProcessError as e:
                    f.write("  [ERROR] dump-ports failed: %s\n" % str(e))
                for eth in ['s1-eth4', 's1-eth5', 's1-eth6']:
                    try:
                        tc = subprocess.check_output(
                            'tc -s class show dev %s 2>/dev/null' % eth,
                            shell=True).decode()
                        f.write("\n--- Queue Stats %s ---\n" % eth)
                        for line in tc.splitlines():
                            f.write("  " + line.strip() + "\n")
                    except subprocess.CalledProcessError:
                        f.write("  [ERROR] tc failed for %s\n" % eth)
                f.write("\n" + "-"*60 + "\n")
                f.flush()
                time.sleep(interval)
            f.write("\n[DONE] Debug log complete\n")

    t = threading.Thread(target=_watch)
    t.daemon = True
    t.start()
    print("[DEBUG] UDP flow watcher started -> logging to %s" % logfile)


def debug_ovs_trace(logfile='/tmp/ovs_trace.txt'):
    def _trace():
        with open(logfile, 'w') as f:
            f.write("OVS PACKET TRACE LOG\n")
            f.write("="*60 + "\n")
            f.flush()
            subprocess.call('ovs-appctl vlog/set dpif:dbg 2>/dev/null', shell=True)
            subprocess.call('ovs-appctl vlog/set ofproto:dbg 2>/dev/null', shell=True)
            proc = subprocess.Popen(
                'tail -f /var/log/openvswitch/ovs-vswitchd.log 2>/dev/null',
                shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            for line in proc.stdout:
                decoded = line.decode('utf-8', errors='ignore')
                if any(x in decoded.lower() for x in ['udp', 'proto=17', 'packet_in', 'flow_mod', 'drop']):
                    f.write(decoded)
                    f.flush()

    t = threading.Thread(target=_trace)
    t.daemon = True
    t.start()
    print("[DEBUG] OVS trace started -> logging to %s" % logfile)


# ─────────────────────────────────────────────────────────────────────────────
# Topology
# ─────────────────────────────────────────────────────────────────────────────

def create_topology():
    net = Mininet(controller=RemoteController, switch=OVSSwitch)
    controller_ip = get_controller_ip(container_name='ryu')
    net.addController('c0', controller=RemoteController,
                      ip=controller_ip, port=CONTROLLER_PORT)
    s1 = net.addSwitch('s1', protocols='OpenFlow13')

    h1 = net.addHost('h1', ip='172.168.1.1/24', mac='00:00:00:00:00:01')
    h2 = net.addHost('h2', ip='172.168.1.2/24', mac='00:00:00:00:00:02')
    h3 = net.addHost('h3', ip='172.168.1.3/24', mac='00:00:00:00:00:03')
    h4 = net.addHost('h4', ip='10.0.0.4/24',    mac='00:00:00:00:00:04')
    h5 = net.addHost('h5', ip='10.0.0.5/24',    mac='00:00:00:00:00:05')
    h6 = net.addHost('h6', ip='10.0.0.6/24',    mac='00:00:00:00:00:06')

    clients = [h1, h2, h3]
    servers = [h4, h5, h6]

    for h in clients + servers:
        h.cmd("sysctl -w net.ipv6.conf.all.disable_ipv6=1")
        h.cmd("sysctl -w net.ipv6.conf.default.disable_ipv6=1")
        h.cmd("sysctl -w net.ipv6.conf.lo.disable_ipv6=1")
        h.cmd("arp -s 10.0.0.100 00:00:00:00:00:FE")

    for h in clients + servers:
        net.addLink(h, s1)

    net.start()
    time.sleep(2)

    debug_udp_flows(duration=120, interval=2, logfile='/tmp/udp_debug.txt')
    debug_ovs_trace(logfile='/tmp/ovs_trace.txt')

    vip_ip  = '10.0.0.100'
    vip_mac = '00:00:00:00:00:FE'

    for client in clients:
        client.cmd('ip neigh add %s lladdr %s dev %s-eth0 nud permanent' %
                   (vip_ip, vip_mac, client.name))

    for server in servers:
        server.cmd('ip neigh add 172.168.1.1 lladdr 00:00:00:00:00:01 dev %s-eth0 nud permanent' % server.name)
        server.cmd('ip neigh add 172.168.1.2 lladdr 00:00:00:00:00:02 dev %s-eth0 nud permanent' % server.name)
        server.cmd('ip neigh add 172.168.1.3 lladdr 00:00:00:00:00:03 dev %s-eth0 nud permanent' % server.name)

    for client in clients:
        client.cmd('ip route add 10.0.0.0/24 dev %s-eth0' % client.name)

    for server in servers:
        server.cmd('ip route add default dev %s-eth0' % server.name)

    for server in servers:
        server.cmd('ip route add 172.168.1.0/24 dev %s-eth0' % server.name)

    setup_queues(num_ports=NUM_PORTS)
    time.sleep(1)

    start_servers(servers)

    builtins.run_experiment   = run_experiment
    builtins.simulate_traffic = simulate_traffic
    builtins.clients          = clients
    builtins.servers          = servers

    print("\n" + "=" * 60)
    print("  Virtual IP   : %s  (%s)" % (VIRTUAL_IP, VIRTUAL_MAC))
    print("  Controller   : %s:%d" % (controller_ip, CONTROLLER_PORT))
    print("  Clients      : h1(172.168.1.1)  h2(172.168.1.2)  h3(172.168.1.3)")
    print("  Servers      : h4(10.0.0.4)     h5(10.0.0.5)     h6(10.0.0.6)")
    print("  Burst times  : %d-%ds per flow (random)" % (MIN_BURST_TIME, MAX_BURST_TIME))
    print("  Users        : %d RT + %d NRT = %d total" % (N_RT_USERS, N_NRT_USERS, N_RT_USERS + N_NRT_USERS))
    print("  Queue        : single queue — all traffic, no priority")
    print("=" * 60)
    print("\n  CLI commands:")
    print("  py run_experiment(net)                      # default 60s")
    print("  py run_experiment(net, duration=60, interval=5)")
    print("  py simulate_traffic(clients)")
    print()

    CLI(net)
    net.stop()


if __name__ == '__main__':
    setLogLevel('info')
    create_topology()
