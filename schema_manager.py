import os
import argparse
import psycopg2
import collections
import re
from datetime import datetime

SchemaFile = collections.namedtuple('SchemaFile', ['name', 'index', 'filename'])

SchemaUpdateRow = collections.namedtuple('SchemaFile', ['name', 'index', 'filename', 'started_at', 'ended_at', 'result'])

def make_schema_update(filename):
    matcher = re.compile('^(\d+)-(.*)\.sql$')

    match = matcher.match(filename)

    index, name = match.groups()

    return SchemaFile(name, int(index), filename)

def validate_schemafiles(schemafiles):
    for i in range(0, len(schemafiles) - 1):
        if schemafiles[i].index + 1 != schemafiles[i + 1].index:
            raise Exception("Files not in sequence: %s, %s" % (schemafiles[i].filename, schemafiles[i + 1].filename))

def get_args():
    parser = argparse.ArgumentParser(description='OpenMonitors database schema manager')
    parser.add_argument('--path', help='Path of SQL schema update file to apply', required=True);
    parser.add_argument('--history', help='Do not check the schema history table');
    parser.add_argument('--novalidate', help='Do not perform schema validation', dest='novalidate', action='store_true');
    parser.add_argument('--start', help='Schema update to start off with');
    parser.add_argument('--end', help='Schema update to end with');
    parser.add_argument('--host', help='Database host', required=True);
    parser.add_argument('--dbname', help='Database name', required=True);
    parser.add_argument('--user', help='Database user', required=True);
    parser.add_argument('--password', help='Database password', required=False);

    return parser.parse_args()

def get_conn(args):
    conn_string = "dbname='%s' user='%s' host='%s'" % (args.dbname, args.user, args.host)

    if args.password:
     conn_string += " password='%s'" % args.password

    return psycopg2.connect(conn_string)

def get_schemafiles(args):
    matching_schema_files = [make_schema_update(f) for f in os.listdir(args.path)]
    if not args.novalidate:
        validate_schemafiles(matching_schema_files)
    return matching_schema_files

def get_last_update(conn):
    sql = "select name, index, file_name from schema_update_history where result = 'succeeded' order by id desc"
    conn.execute(sql)
    row = conn.fetchone()

    if row is not None:
        name, index, filename = row
        return SchemaFile(name, index, filename)
    return None
    
def apply_single_update(conn, args, schema_update):
    print("Applying update %s" % schema_update.name)
    with open(os.path.join(args.path, schema_update.filename), mode='r') as f:
        sql = f.read()
        record = SchemaUpdateRow(schema_update.name, schema_update.index, schema_update.filename, datetime.now(), None, 'started')
        if not args.novalidate:
            pass
        try:
            conn.execute(sql)
            if not args.novalidate:
                pass
            record = SchemaUpdateRow(record.name, record.index, record.filename, record.started_at, datetime.now(), 'succeeded')
            print("Successfully apllied update %s" % schema_update.name)
        except Exception as e:
            print("Error while applying update %s: %s" % (schema_update.name, e))
            if not args.novalidate:
                #conn.execute('rollback')
                pass;
            record = SchemaUpdateRow(record.name, record.index, record.filename, record.started_at, datetime.now(), 'failed')
        finally:
            record = SchemaUpdateRow(record.name, record.index, record.filename, record.started_at, datetime.now(), record.result)

        return record


def apply_updates(conn, args, schema_updates):
    if not args.novalidate:
        conn.execute('begin transaction')
    early_exit = False

    updates = []
    update = None

    for schema_update in schema_updates:
        update = apply_single_update(conn, args, schema_update)
        updates.append(update)

        if update.result != 'succeeded' and not args.novalidate:
            conn.execute('rollback')
            early_exit = True
            break

    if early_exit:
        abandoned_updates = map(lambda u: SchemaUpdateRow(u.name, u.index, u.filename, u.started_at, u.ended_at, 'abandoned'), updates[1:])
        updates = list(abandoned_updates) + [update]

    if not args.novalidate:
        conn.execute('commit')

    return updates

def insert_history(conn, updates):
    vars_list = [[update.name, update.index, update.filename, update.started_at, update.ended_at, update.result] for update in updates]

    conn.executemany('''insert into schema_update_history (name, index, file_name, started_at, ended_at, result) values (%s, %s, %s, %s, %s, %s)''',
        vars_list)

def main():
    args = get_args()
    schemafiles = get_schemafiles(args)
    conn = get_conn(args)
    if not args.novalidate:
        validate_schemafiles(schemafiles)
    cursor = conn.cursor()
    last_update = get_last_update(cursor)
    schemafiles = [s for s in schemafiles if last_update is None or s.index > last_update.index]
    updates = apply_updates(cursor, args, schemafiles)
    insert_history(cursor, updates)

if __name__ == '__main__':
    main()
