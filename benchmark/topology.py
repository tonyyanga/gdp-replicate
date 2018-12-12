from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink
from mininet.log import setLogLevel
import time
import sys
import os
from gdb_log_utils import *

class GDPSimulationTopo(Topo):
    def build(self, n, loss_rate=None):
        switch = self.addSwitch('s1')
        for i in range(n):
            host = self.addHost('h' + str(i + 1), cpu=.5 / n)
            self.addLink(host, switch, cls=TCLink, bw=10, delay='20ms', loss=loss_rate)


# topos = {'simple': (lambda: GDPSimulationTopo(3, None)), 'lossy': (lambda: GDPSimulationTopo(3, 0.01))}

if len(sys.argv) != 5:
    print ('NUM_LOG_SERVER, WRITE_ITERVAL, FANOUT, ALGO')
    sys.exit(2)

NUM_LOG_SERVER = int(sys.argv[1])
WRITE_ITERVAL = float(sys.argv[2])
FANOUT = int(sys.argv[3])
ALGO = sys.argv[4]
PORT = 10262
WRITE_INTERVAL = 0.2

if __name__ == '__main__':
    setLogLevel('info')
    topo = GDPSimulationTopo(n=(NUM_LOG_SERVER + 1))
    net = Mininet(topo=topo)
    net.start()
    # net.pingAll()
    path = ','.join(sys.argv[1:])
    os.system("mkdir " + path)
    os.system('rm -f {0}/*.db'.format(path))
    os.system('rm -f {0}/*.log'.format(path))
    for i in range(NUM_LOG_SERVER):
        create_fresh_logdb("{0}/{1}.db".format(path, i))
    log_servers = net.hosts[:NUM_LOG_SERVER]
    writer = net.hosts[-1]
    for i, server in enumerate(log_servers):
        # server.cmdPrint('tcpdump port {0} -i h{1}-eth0 -w {2}/{3}.pcap &'.format(PORT, i + 1, path, i))
        peers_addr = ['{0}:{1}'.format(h.IP(), PORT) for h in log_servers
                if h != server]
        peers_addr_str = ",".join(peers_addr)
        db_file = str(i) + ".db"
        server_cmd = ['sudo ../gdp-replicate',
                        "{0}/{1}.db".format(path, i),
                        '{0}:{1}'.format(server.IP(), PORT),
                        ",".join(peers_addr),
                         FANOUT,
                        'naive',
                        '2>', "{0}/{1}.log".format(path, i),
                        '&']
        if ALGO != 'naive':
            server_cmd.remove('naive')
        server.cmdPrint(server_cmd) 
    writer.cmdPrint('sudo python3 writer.py',
                     NUM_LOG_SERVER, WRITE_INTERVAL, path,
                    '&')
    time.sleep(1000)
    net.stop()
