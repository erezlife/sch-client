#!/usr/bin/python

import sch_client
import json
import pyodbc
import os


__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
sch_client.init_logging(__location__, 'sync_pull')
sch_client.printme('------ Begin sync_pull ------')
config = json.load(open(os.path.join(__location__, 'config.json')))
sql = open(os.path.join(__location__, config['pull_sql'])).read()
columns = json.load(open(os.path.join(__location__, config['pull_map'])))
api = sch_client.API(config['uri'], config['key'], config['secret'])

dbms = config['dbms'] if 'dbms' in config else 'odbc'
if dbms == 'oracle':
    import cx_Oracle
    connection = cx_Oracle.connect(config['db_connection'])
else:
    connection = pyodbc.connect(config['db_connection'])


def execute_pull(api, conn, query, params, columns, batch_size=10):
    cursor = conn.cursor()

    if dbms == 'oracle':
        query, query_params = sch_client.prepare_query(query, params, ':0')
        cursor.execute(query, query_params)
    else:
        query, query_params = sch_client.prepare_query(query, params)
        cursor.execute(query, *query_params)

    def iterate():
        return cursor.fetchone()

    return sch_client.set_residents_batch(api, iterate, columns, params, batch_size)


instances = api.get_instances()
for instance in instances:
    sch_client.printme("Processing instance", ' ')
    for key in instance['key']:
        api.printme(key + "=" + instance['key'][key], ' ')
    api.printme()
    num_updated, num_skipped, missing_records, principals_updated = execute_pull(api, connection, sql, instance['key'], columns)
    sch_client.printme("Records updated: " + str(num_updated))
    sch_client.printme("Records skipped: " + str(num_skipped))
    sch_client.printme("Missing records: " + str(missing_records))
    sch_client.printme("SSO principals updated: " + str(principals_updated))

sch_client.printme('------ End sync_pull ------')
