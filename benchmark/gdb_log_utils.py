import hashlib
import sqlite3


def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        print(e)

    return None

def create_fresh_logdb(name):
    conn = create_connection(name)
    sql_create_table = """ CREATE TABLE IF NOT EXISTS log_entry (
                                        hash BLOB(32) PRIMARY KEY ON CONFLICT IGNORE,
                                        recno INTEGER, 
                                        timestamp INTEGER,
                                        accuracy FLOAT,
                                        prevhash BLOB(32),
                                        value BLOB,
                                        sig BLOB);
                                """
    try:
        c = conn.cursor()
        c.execute(sql_create_table)
    except sqlite3.Error as e:
        print(e)
    return conn


def write_graph_to_db(graph, db_name):
    conn = create_fresh_logdb(db_name)
    conn.executemany('INSERT INTO log_entry VALUES (?, ?, ?, ?, ?, ?, ?)', graph)
    conn.commit()
    conn.close()

def get_hash(name):
    val = hashlib.sha256(name.encode())
    return val.digest()

def get_blob(data):
    if type(data) == str:
        data = get_hash(data)
    return sqlite3.Binary(data)

