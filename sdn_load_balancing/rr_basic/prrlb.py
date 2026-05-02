# -*- coding: utf-8 -*-
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet, ipv4, tcp, udp, arp

VIRTUAL_IP  = '10.0.0.100'
VIRTUAL_MAC = '00:00:00:00:00:FE'

SERVERS = [
    {'ip': '10.0.0.4', 'mac': '00:00:00:00:00:04', 'port': 4},
    {'ip': '10.0.0.5', 'mac': '00:00:00:00:00:05', 'port': 5},
    {'ip': '10.0.0.6', 'mac': '00:00:00:00:00:06', 'port': 6}
]

CLIENT_PORTS = {1, 2, 3}

QUEUE_REALTIME     = 0
QUEUE_NON_REALTIME = 0

REALTIME_PORTS     = {554, 5004, 5005}
NON_REALTIME_PORTS = {80, 443, 21, 20, 8080}

ETH_TYPE_IP  = 0x0800
ETH_TYPE_ARP = 0x0806

class LoadBalancer(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(LoadBalancer, self).__init__(*args, **kwargs)
        self.rr = 0
        self.conn_map = {}
        self.arp_cache = {}
        self.return_map = {}

    # ---------------- Switch Setup ----------------
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        dp = ev.msg.datapath
        ofp = dp.ofproto
        parser = dp.ofproto_parser

        self.rr = 0
        self.conn_map = {}
        self.return_map = {}
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
        # print("\n" + "="*60)
        # print("[PACKET_IN RECEIVED]")
        # print("Datapath ID:", dp.id)
        # print("In port    :", in_port)
        # print("Packet len :", len(msg.data))

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)

        if eth:
            # print("Ethernet src:", eth.src)
            # print("Ethernet dst:", eth.dst)
            # print("Ether type  :", hex(eth.ethertype))
            if eth.ethertype == 0x86dd:
                # print("Packet 0x86dd returned")
                return
        else:
            # print("No Ethernet header found")
            return

        # --- ARP ---
        arp_pkt = pkt.get_protocol(arp.arp)
        if arp_pkt:
            # print("[ARP]", arp_pkt.src_ip, "->", arp_pkt.dst_ip)
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
                # print("ip_proto:", ip_pkt.proto)
                # print("udp_pkt:", udp_pkt)
                # print("tcp_pkt:", tcp_pkt)
                client_ip_to_port = {
                    '172.168.1.1': 1,
                    '172.168.1.2': 2,
                    '172.168.1.3': 3
                }
                # print("reverse : in_port", in_port)
                client_port = client_ip_to_port.get(ip_pkt.dst)
                if not client_port:
                    return

                # build return key
                if udp_pkt:
                    return_key = (ip_pkt.src, ip_pkt.dst, 17,
                                  udp_pkt.src_port, udp_pkt.dst_port)
                elif tcp_pkt:
                    return_key = (ip_pkt.src, ip_pkt.dst, 6,
                                  tcp_pkt.src_port, tcp_pkt.dst_port)
                else:
                    return_key = (ip_pkt.src, ip_pkt.dst, ip_pkt.proto)
                    # print("[WARN] Unknown proto %d from server" % ip_pkt.proto)

                if return_key in self.return_map:
                    # print("[DUP REV] forwarding only %s -> %s -> %s" % (
                    #     ip_pkt.src, ip_pkt.dst, return_key))
                    pass
                else:
                    self.return_map[return_key] = client_port
                    # print("[REV RACE] %s -> %s -> %s forwarding during race window" % (
                    #     ip_pkt.src, ip_pkt.dst, return_key))

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

        # --- VIP traffic → LB decision ---
        if tcp_pkt:
            proto = "TCP"
            src_port = tcp_pkt.src_port
            dst_port = tcp_pkt.dst_port
            # print("TCP src port:", src_port)
            # print("TCP dst port:", dst_port)

            flow_key = (ip_pkt.src, ip_pkt.dst, ip_pkt.proto, src_port, dst_port)
            if flow_key in self.conn_map:
                # print("[DUP] Duplicate PACKET_IN for existing flow %s:%s, forwarding only" % (
                #     ip_pkt.src, src_port))
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
                server = SERVERS[self.rr]
                self.rr = (self.rr + 1) % len(SERVERS)
                self.conn_map[flow_key] = server
                self.rr_count[server['ip']] += 1
                print("[RR COUNT] h4=%d  h5=%d  h6=%d" % (
                    self.rr_count['10.0.0.4'],
                    self.rr_count['10.0.0.5'],
                    self.rr_count['10.0.0.6']))

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
            # print("UDP src port:", src_port)
            # print("UDP dst port:", dst_port)

            flow_key = (ip_pkt.src, ip_pkt.dst, ip_pkt.proto, src_port, dst_port)
            if flow_key in self.conn_map:
                server = self.conn_map[flow_key]
                # print("[DUP] Duplicate PACKET_IN for existing flow %s:%s, forwarding only" % (
                #     ip_pkt.src, src_port))
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
                server = SERVERS[self.rr]
                self.rr = (self.rr + 1) % len(SERVERS)
                self.conn_map[flow_key] = server
                self.rr_count[server['ip']] += 1
                print("[RR COUNT] h4=%d  h5=%d  h6=%d" % (
                    self.rr_count['10.0.0.4'],
                    self.rr_count['10.0.0.5'],
                    self.rr_count['10.0.0.6']))

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

        # print("[LB] %s:%s (%s) -> VIP %s:%s -> %s (port %d)" % (
        #     ip_pkt.src, src_port, proto,
        #     VIRTUAL_IP, dst_port,
        #     server['ip'], server['port']
        # ))

        # Install forward flow
        self.add_flow(dp, 10, match, actions, idle_timeout=180)

        # Reverse flow — installed when client packet arrives
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
            # print("Switch is holding the packet for ", msg.buffer_id)
            out = parser.OFPPacketOut(
                datapath=dp,
                buffer_id=msg.buffer_id,
                in_port=in_port,
                actions=actions,
                data=None)
        else:
            # print("Switch is not holding the packet for ", msg.buffer_id)
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
        # print("[ARP CACHE UPDATED]", arp_pkt.src_ip, "->", arp_pkt.src_mac)

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
