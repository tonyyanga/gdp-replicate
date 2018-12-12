import random
import time
import datetime
from gdb_log_utils import *
import sys

if len(sys.argv) != 6:
    print("NUMBER_LOG_SERVER, WRITE_INTERVAL, PATH, FAULTY_RATE, CHURN")
    sys.exit(3)


PATH = sys.argv[3]
LOGDB_NUM = int(sys.argv[1])
GLOG_DB = [PATH + "/%s.db" % i for i in range(LOGDB_NUM)]
WRITE_INTERVAL = float(sys.argv[2])
MIN_DATA_SIZE = 24000
MAX_DATA_SIZE = 24001
CHURN_TIME = []
if sys.argv[5] == "churn":
    CHURN_TIME = [150, 300]
END_TIME = 500
FAULTY_POSSIBILITY = float(sys.argv[4])
print("WRITER BEGINS", FAULTY_POSSIBILITY)

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
            churn_cnt = int(math.ceil(LOGDB_NUM / 3.0))
            random.shuffle(servers)
            servers_to_die = servers[:churn_cnt]
            for sever in servers_to_die:
                wipe_all_records(PATH + "/%s.db" % server)
                log = dict(timestamp=str(datetime.datetime.now()),
                           written_cnt=cnt,
                           server_id=server)
                churn_file.write(str(log) + "\n")
            churn_file.flush()
        rand1, rand2 = random.uniform(0, 1), random.uniform(0, 1)
        # making faults:
        if rand1 <= FAULTY_POSSIBILITY:
            prev_hash = get_hash(str(random.getrandbits(256)))
            log = dict(timestamp=str(datetime.datetime.now()),
                       event='hole')
            churn_file.write(str(log) + "\n")
            churn_file.flush()
        elif rand2 <= FAULTY_POSSIBILITY:
            branch_pos = random.randint(0, len(written_hash) - 2)
            prev_hash = written_hash[branch_pos]
            log = dict(timestamp=str(datetime.datetime.now()),
                       event='branch')
            churn_file.write(str(log) + "\n")
            churn_file.flush()
        else:
            prev_hash = written_hash[-1]
        curr_hash = get_hash(str(random.getrandbits(256)))
        data_size = random.randint(MIN_DATA_SIZE, MAX_DATA_SIZE)
        curr_data = get_hash(str(random.getrandbits(data_size)))
        curr_sig = get_hash(str(random.getrandbits(100)))
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
