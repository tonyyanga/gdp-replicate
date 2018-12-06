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
            self.addLink(host, switch, cls=TCLink, bw=100, delay='5ms', loss=loss_rate)


# topos = {'simple': (lambda: GDPSimulationTopo(3, None)), 'lossy': (lambda: GDPSimulationTopo(3, 0.01))}

if len(sys.argv) != 2:
    print ('NUM_LOG_SERVER')
    sys.exit(2)

NUM_LOG_SERVER = int(sys.argv[1])
PORT = 10262
WRITE_INTERVAL = 1

if __name__ == '__main__':
    setLogLevel('info')
    topo = GDPSimulationTopo(n=(NUM_LOG_SERVER + 1), loss_rate=0.001)
    net = Mininet(topo=topo)
    net.start()
    # net.pingAll()
    os.system('rm -f *.db')
    for i in range(NUM_LOG_SERVER):
        create_fresh_logdb(str(i) + ".db")
    log_servers = net.hosts[:NUM_LOG_SERVER]
    writer = net.hosts[-1]
    for i, server in enumerate(log_servers):
        peers_addr = ['{0}:{1}'.format(h.IP(), PORT) for h in log_servers if h != server]
        peers_addr_str = ",".join(peers_addr)
        db_file = str(i) + ".db"
        server.cmdPrint('../gdp-replicate', 
                        str(i) + '.db',
                        '{0}:{1}'.format(server.IP(), PORT),
                        ",".join(peers_addr),
                         2,
                        '2>', str(i) + '.log',
                        '&')
    writer.cmdPrint('python3 writer.py',
                     NUM_LOG_SERVER, WRITE_INTERVAL,
                     #'> writer.log',
                    '&')
    time.sleep(1000)
    net.stop()
