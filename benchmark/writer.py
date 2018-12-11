import random
import time
import datetime
from gdb_log_utils import *
import sys

if len(sys.argv) != 4:
    print("NUMBER_LOG_SERVER, WRITE_INTERVAL, PATH")
    sys.exit(3)


PATH = sys.argv[3]
LOGDB_NUM = int(sys.argv[1])
GLOG_DB = [PATH + "/%s.db" % i for i in range(LOGDB_NUM)]
WRITE_INTERVAL = float(sys.argv[2])
MIN_DATA_SIZE = 1024
MAX_DATA_SIZE = 1025
CHURN_TIME = [200, 400]
END_TIME = 600


print("WRITER BEGINS")

if __name__ == '__main__':
    written_hash = [get_blob('0')]
    connections = [create_connection(file) for file in GLOG_DB]
    servers = list(range(LOGDB_NUM))
    cnt = 0
    log_file = open(PATH + "/writer.log", "w")
    churn_file = open(PATH + "/churn.log", "w")
    while cnt <= END_TIME:
        if cnt in CHURN_TIME:
            # randint is inclusive on both side
            server_to_die = 0 #random.randint(0, LOGDB_NUM - 1)
            wipe_all_records(PATH + "/%s.db" % server_to_die)
            log = dict(timestamp=str(datetime.datetime.now()),
                       written_cnt=cnt,
                       server_id=server_to_die)
            churn_file.write(str(log) + "\n")
            churn_file.flush()
        curr_hash = get_hash(str(random.getrandbits(256)))
        data_size = random.randint(MIN_DATA_SIZE, MAX_DATA_SIZE)
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
            log = dict(timestamp=str(datetime.datetime.now()),
                       write_cnt=cnt,
                       server_id=i,
                       record_hash=curr_hash.hex().upper())
            log_file.write(str(log) + "\n")
        log_file.flush()
        cnt += 1
        time.sleep(WRITE_INTERVAL)
    print("writer finished.")
