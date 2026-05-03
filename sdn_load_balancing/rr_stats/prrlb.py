# -*- coding: utf-8 -*-
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet, ipv4, tcp, udp, arp
from ryu.lib import hub

def _median(lst):
    """Python 2.7 compatible median — statistics module not available."""
    s = sorted(lst)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return (s[mid - 1] + s[mid]) / 2.0

VIRTUAL_IP  = '10.0.0.100'
VIRTUAL_MAC = '00:00:00:00:00:FE'

SERVERS = [
    {'ip': '10.0.0.4', 'mac': '00:00:00:00:00:04', 'port': 4, 'weight': 1},
    {'ip': '10.0.0.5', 'mac': '00:00:00:00:00:05', 'port': 5, 'weight': 1},
    {'ip': '10.0.0.6', 'mac': '00:00:00:00:00:06', 'port': 6, 'weight': 1}
]

CLIENT_PORTS = {1, 2, 3}

QUEUE_REALTIME     = 0
QUEUE_NON_REALTIME = 0

REALTIME_PORTS     = {554, 5004, 5005}
NON_REALTIME_PORTS = {80, 443, 21, 20, 8080}

ETH_TYPE_IP  = 0x0800
ETH_TYPE_ARP = 0x0806

# How often (seconds) to poll switch for flow stats and recompute weights
MONITOR_INTERVAL = 5

class LoadBalancer(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(LoadBalancer, self).__init__(*args, **kwargs)
        self.rr = 0
        self.weighted_pool = []
        for s in SERVERS:
            self.weighted_pool.extend([s] * s['weight'])
        self.conn_map  = {}   # flow_key → server
        self.arp_cache = {}
        self.return_map = {}
        self.datapath  = None

        # flow_key → duration_sec (updated by stats replies)
        self.flow_durations = {}

        # Start background monitor thread
        self.monitor_thread = hub.spawn(self._monitor)

    # ------------------------------------------------------------------ #
    #  Background monitor: polls switch stats every MONITOR_INTERVAL secs #
    # ------------------------------------------------------------------ #
    def _monitor(self):
        while True:
            if self.datapath:
                self._request_flow_stats(self.datapath)
            hub.sleep(MONITOR_INTERVAL)

    def _request_flow_stats(self, datapath):
        parser = datapath.ofproto_parser
        req = parser.OFPFlowStatsRequest(datapath)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def flow_stats_reply_handler(self, ev):
        """
        Collect duration_sec for every active flow going to a server.
        This is our 'burst time' equivalent from the paper.
        Then apply: tq = (maxBT + minBT + median) / 2
        and recompute weights so that servers with load > tq get lower weight.
        """
        durations_per_server = {s['ip']: [] for s in SERVERS}

        for stat in ev.msg.body:
            # Only consider high-priority forwarding flows (not table-miss etc.)
            if stat.priority != 10:
                continue
            duration = stat.duration_sec

            # Find which server this flow is going to by checking output action
            for instruction in stat.instructions:
                for action in instruction.actions:
                    if hasattr(action, 'field') and hasattr(action.field, 'value'):
                        pass
                # Check OFPActionOutput port
                for action in instruction.actions:
                    if action.type == 0:  # OFPAT_OUTPUT
                        for s in SERVERS:
                            if action.port == s['port']:
                                durations_per_server[s['ip']].append(duration)

        # Flatten all durations for tq computation
        all_durations = []
        for dlist in durations_per_server.values():
            all_durations.extend(dlist)

        if len(all_durations) < 2:
            # Not enough data yet, keep current weights
            return

        max_bt  = max(all_durations)
        min_bt  = min(all_durations)
        median  = _median(all_durations)

        # Paper formula: tq = (maxBT + minBT + median) / 2
        tq = (max_bt + min_bt + median) / 2

        print("\n[MONITOR] Flow durations per server:")
        for s in SERVERS:
            dlist = durations_per_server[s['ip']]
            avg = sum(dlist)/len(dlist) if dlist else 0
            print("  %s: %d flows, avg duration=%.1fs" % (s['ip'], len(dlist), avg))
        print("[MONITOR] maxBT=%.1f  minBT=%.1f  median=%.1f  tq=%.1f" % (
            max_bt, min_bt, median, tq))

        # Compute avg load per server
        avg_loads = {}
        for s in SERVERS:
            dlist = durations_per_server[s['ip']]
            avg_loads[s['ip']] = sum(dlist) / float(len(dlist)) if dlist else 0.0

        # Mean load across all servers.
        # tq tells us the expected load level from the paper formula.
        # Instead of comparing each server to tq (which always makes all
        # servers equal), we compare each server to the mean load.
        # Servers below mean → less busy → get weight=2 (more new flows)
        # Servers above mean → more busy → get weight=1 (fewer new flows)
        # tq/10 is used as a sensitivity band — if all servers are within
        # this band of each other, treat as equal and use plain RR (weight=1).
        mean_load   = sum(avg_loads.values()) / float(len(avg_loads))
        sensitivity = tq / 10.0
        all_similar = all(
            abs(avg_loads[s['ip']] - mean_load) < sensitivity
            for s in SERVERS
        )

        print("[MONITOR] maxBT=%.1f  minBT=%.1f  median=%.1f  tq=%.1f  mean_load=%.1f  sensitivity=%.1f" % (
            max_bt, min_bt, median, tq, mean_load, sensitivity))

        self.weighted_pool = []
        for s in SERVERS:
            avg_load = avg_loads[s['ip']]
            if all_similar:
                w = 1   # all servers similar → plain RR, no bias
            elif avg_load < mean_load:
                w = 2   # lighter than average → more flows
            else:
                w = 1   # heavier than average → fewer flows
            self.weighted_pool.extend([s] * w)
            print("[MONITOR] %s avg_load=%.1f mean=%.1f → weight=%d%s" % (
                s['ip'], avg_load, mean_load, w,
                " (all similar, plain RR)" if all_similar else ""))

        # Reset rr index to stay within new pool size
        self.rr = self.rr % len(self.weighted_pool)
        print("[MONITOR] New weighted_pool size: %d" % len(self.weighted_pool))

    # ---------------- Switch Setup ----------------
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        dp = ev.msg.datapath
        ofp = dp.ofproto
        parser = dp.ofproto_parser

        self.datapath = dp  # save for monitor thread
        self.rr = 0
        self.conn_map = {}
        self.return_map = {}
        self.flow_durations = {}
        self.rr_count = {'10.0.0.4': 0, '10.0.0.5': 0, '10.0.0.6': 0}

        # Table-miss → controller
        self.add_flow(dp, 0, parser.OFPMatch(),
            [parser.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)])

        # ARP → flood
        self.add_flow(dp, 1,
            parser.OFPMatch(eth_type=ETH_TYPE_ARP),
            [parser.OFPActionOutput(ofp.OFPP_FLOOD)])

        # VIP traffic → controller
        self.add_flow(dp, 1,
            parser.OFPMatch(eth_type=ETH_TYPE_IP, ipv4_dst=VIRTUAL_IP),
            [parser.OFPActionOutput(ofp.OFPP_CONTROLLER)])

        # Drop IPv6
        self.add_flow(dp, 1,
            parser.OFPMatch(eth_type=0x86dd),
            [])

    def add_flow(self, datapath, priority, match, actions, idle_timeout=180, hard_timeout=0):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(
            ofproto.OFPIT_APPLY_ACTIONS, actions
        )]

        mod = parser.OFPFlowMod(
            datapath=datapath,
            priority=priority,
            match=match,
            instructions=inst,
            idle_timeout=idle_timeout,
            hard_timeout=hard_timeout
        )
        datapath.send_msg(mod)

    # ---------------- PacketIn ----------------
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        msg = ev.msg
        dp = msg.datapath
        parser = dp.ofproto_parser

        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)

        if eth:
            if eth.ethertype == 0x86dd:
                return
        else:
            return

        # --- ARP ---
        arp_pkt = pkt.get_protocol(arp.arp)
        if arp_pkt:
            if arp_pkt.opcode == arp.ARP_REQUEST and arp_pkt.dst_ip == VIRTUAL_IP:
                self.reply_arp(dp, in_port, eth, arp_pkt)
            return

        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        tcp_pkt = pkt.get_protocol(tcp.tcp)
        udp_pkt = pkt.get_protocol(udp.udp)

        if not ip_pkt:
            return

        # --- Server → Client return traffic ---
        if ip_pkt.dst != VIRTUAL_IP:
            if in_port in {4, 5, 6}:
                if ip_pkt.proto == 1:
                    return

                client_ip_to_port = {
                    '172.168.1.1': 1,
                    '172.168.1.2': 2,
                    '172.168.1.3': 3
                }
                client_port = client_ip_to_port.get(ip_pkt.dst)
                if not client_port:
                    return

                if udp_pkt:
                    return_key = (ip_pkt.src, ip_pkt.dst, 17,
                                  udp_pkt.src_port, udp_pkt.dst_port)
                elif tcp_pkt:
                    return_key = (ip_pkt.src, ip_pkt.dst, 6,
                                  tcp_pkt.src_port, tcp_pkt.dst_port)
                else:
                    return_key = (ip_pkt.src, ip_pkt.dst, ip_pkt.proto)

                if return_key not in self.return_map:
                    self.return_map[return_key] = client_port

                actions_race = [
                    parser.OFPActionSetField(eth_src=VIRTUAL_MAC),
                    parser.OFPActionSetField(eth_dst=eth.src),
                    parser.OFPActionSetField(ipv4_src=VIRTUAL_IP),
                    parser.OFPActionOutput(client_port)
                ]
                out = parser.OFPPacketOut(
                    datapath=dp,
                    buffer_id=dp.ofproto.OFP_NO_BUFFER,
                    in_port=in_port,
                    actions=actions_race,
                    data=msg.data)
                dp.send_msg(out)
            return

        # --- VIP traffic → Dynamic WRR LB decision (per flow) ---
        if tcp_pkt:
            proto = "TCP"
            src_port = tcp_pkt.src_port
            dst_port = tcp_pkt.dst_port
            flow_key = (ip_pkt.src, ip_pkt.dst, ip_pkt.proto, src_port, dst_port)

            if flow_key in self.conn_map:
                server = self.conn_map[flow_key]
                actions = [
                    parser.OFPActionSetField(eth_dst=server['mac']),
                    parser.OFPActionSetField(ipv4_dst=server['ip']),
                    parser.OFPActionOutput(server['port'])
                ]
                out = parser.OFPPacketOut(
                    datapath=dp,
                    buffer_id=dp.ofproto.OFP_NO_BUFFER,
                    in_port=in_port,
                    actions=actions,
                    data=msg.data)
                dp.send_msg(out)
                return
            else:
                # New flow: pick next server from dynamically recomputed weighted pool
                server = self.weighted_pool[self.rr]
                self.rr = (self.rr + 1) % len(self.weighted_pool)
                self.conn_map[flow_key] = server
                self.rr_count[server['ip']] += 1
                print("[WRR FLOW] h4=%d  h5=%d  h6=%d  pool_size=%d" % (
                    self.rr_count['10.0.0.4'],
                    self.rr_count['10.0.0.5'],
                    self.rr_count['10.0.0.6'],
                    len(self.weighted_pool)))

            match = parser.OFPMatch(
                in_port=in_port,
                eth_type=ETH_TYPE_IP,
                ip_proto=6,
                ipv4_src=ip_pkt.src,
                ipv4_dst=VIRTUAL_IP,
                tcp_src=tcp_pkt.src_port,
                tcp_dst=tcp_pkt.dst_port
            )

        elif udp_pkt:
            proto = "UDP"
            src_port = udp_pkt.src_port
            dst_port = udp_pkt.dst_port
            flow_key = (ip_pkt.src, ip_pkt.dst, ip_pkt.proto, src_port, dst_port)

            if flow_key in self.conn_map:
                server = self.conn_map[flow_key]
                actions = [
                    parser.OFPActionSetField(eth_dst=server['mac']),
                    parser.OFPActionSetField(ipv4_dst=server['ip']),
                    parser.OFPActionOutput(server['port'])
                ]
                out = parser.OFPPacketOut(
                    datapath=dp,
                    buffer_id=dp.ofproto.OFP_NO_BUFFER,
                    in_port=in_port,
                    actions=actions,
                    data=msg.data)
                dp.send_msg(out)
                return
            else:
                # New flow: pick next server from dynamically recomputed weighted pool
                server = self.weighted_pool[self.rr]
                self.rr = (self.rr + 1) % len(self.weighted_pool)
                self.conn_map[flow_key] = server
                self.rr_count[server['ip']] += 1
                print("[WRR FLOW] h4=%d  h5=%d  h6=%d  pool_size=%d" % (
                    self.rr_count['10.0.0.4'],
                    self.rr_count['10.0.0.5'],
                    self.rr_count['10.0.0.6'],
                    len(self.weighted_pool)))

            match = parser.OFPMatch(
                in_port=in_port,
                eth_type=ETH_TYPE_IP,
                ip_proto=17,
                ipv4_src=ip_pkt.src,
                ipv4_dst=VIRTUAL_IP,
                udp_src=udp_pkt.src_port,
                udp_dst=udp_pkt.dst_port
            )

        else:
            return

        actions = [
            parser.OFPActionSetField(eth_dst=server['mac']),
            parser.OFPActionSetField(ipv4_dst=server['ip']),
            parser.OFPActionOutput(server['port'])
        ]

        # Install forward flow
        self.add_flow(dp, 10, match, actions, idle_timeout=180)

        # Install reverse flow
        if proto == "UDP":
            match_rev = parser.OFPMatch(
                in_port=server['port'],
                eth_type=ETH_TYPE_IP,
                ip_proto=17,
                ipv4_src=server['ip'],
                ipv4_dst=ip_pkt.src
            )
        else:
            match_rev = parser.OFPMatch(
                in_port=server['port'],
                eth_type=ETH_TYPE_IP,
                ip_proto=6,
                ipv4_src=server['ip'],
                ipv4_dst=ip_pkt.src,
                tcp_src=dst_port,
                tcp_dst=src_port
            )

        actions_rev = [
            parser.OFPActionSetField(eth_src=VIRTUAL_MAC),
            parser.OFPActionSetField(eth_dst=eth.src),
            parser.OFPActionSetField(ipv4_src=VIRTUAL_IP),
            parser.OFPActionOutput(in_port)
        ]
        self.add_flow(dp, 10, match_rev, actions_rev, idle_timeout=180)

        if msg.buffer_id != dp.ofproto.OFP_NO_BUFFER:
            out = parser.OFPPacketOut(
                datapath=dp,
                buffer_id=msg.buffer_id,
                in_port=in_port,
                actions=actions,
                data=None)
        else:
            out = parser.OFPPacketOut(
                datapath=dp,
                buffer_id=dp.ofproto.OFP_NO_BUFFER,
                in_port=in_port,
                actions=actions,
                data=msg.data)

        dp.send_msg(out)

    # ---------------- ARP Reply ----------------
    def reply_arp(self, dp, port, eth, arp_pkt):
        parser = dp.ofproto_parser

        self.arp_cache = getattr(self, "arp_cache", {})
        self.arp_cache[arp_pkt.src_ip] = arp_pkt.src_mac

        pkt = packet.Packet()
        pkt.add_protocol(ethernet.ethernet(
            ethertype=ETH_TYPE_ARP,
            dst=eth.src,
            src=VIRTUAL_MAC))
        pkt.add_protocol(arp.arp(
            opcode=arp.ARP_REPLY,
            src_mac=VIRTUAL_MAC,
            src_ip=VIRTUAL_IP,
            dst_mac=arp_pkt.src_mac,
            dst_ip=arp_pkt.src_ip))
        pkt.serialize()

        out = parser.OFPPacketOut(
            datapath=dp,
            buffer_id=dp.ofproto.OFP_NO_BUFFER,
            in_port=dp.ofproto.OFPP_CONTROLLER,
            actions=[parser.OFPActionOutput(port)],
            data=pkt.data)
        dp.send_msg(out)
