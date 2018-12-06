import random
import time
import datetime
from gdb_log_utils import *
import sys

if len(sys.argv) != 2:
    print("NUMBER_LOG_SERVER")
    sys.exit(2)

LOGDB_NUM = int(sys.argv[1])
GLOG_DB = [str(i) + '.db' for i in range(LOGDB_NUM)]
GOSSIP_INTERVAL = 4
WRITE_INTERVAL = 1 * GOSSIP_INTERVAL

print("WRITER BEGINS")

if __name__ == '__main__':
    written_hash = [get_blob('0')]
    connections = [create_connection(file) for file in GLOG_DB]
    servers = list(range(LOGDB_NUM))
    cnt = 0
    while True:
        curr_hash = get_hash(str(random.getrandbits(256)))
        curr_data = get_hash(str(random.getrandbits(1024)))
        curr_sig = get_hash(str(random.getrandbits(100)))
        prev_hash = written_hash[-1]
        random.shuffle(servers)
        chosen = servers[:(LOGDB_NUM//2 + 1)]
        for i in chosen:
            conn = connections[i]
            c = conn.cursor()
            record = (get_blob(curr_hash), 0, 0, 0, prev_hash, get_blob(curr_data), get_blob(curr_sig))
            written_hash.append(curr_hash)
            c.execute('INSERT INTO log_entry VALUES (?, ?, ?, ?, ?, ?, ?)', record)
            conn.commit()
            print("{0}, {1} write to {2}, hash: {3}".format(datetime.datetime.now(), cnt,
                                                           chosen,
                                                           curr_hash.hex()[:4].upper()))
        cnt += 1
        time.sleep(WRITE_INTERVAL)

