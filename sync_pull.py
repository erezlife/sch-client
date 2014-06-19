#!/usr/bin/python

import sch_client
import json
import pyodbc
import os


def execute_pull(api, conn, query, params, columns, batch_size=10):
    cursor = conn.cursor()
    query, query_params = sch_client.prepare_query(query, params)
    cursor.execute(query, *query_params)

    def iterate():
        return cursor.fetchone()

    return sch_client.set_residents_batch(api, iterate, columns, params, batch_size)


__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
sch_client.init_logging(__location__, 'sync_pull')
sch_client.printme('------ Begin sync_pull ------')
config = json.load(open(os.path.join(__location__, 'config.json')))
sql = open(os.path.join(__location__, config['pull_sql'])).read()
columns = json.load(open(os.path.join(__location__, config['pull_map'])))
api = sch_client.API(config['uri'], config['key'], config['secret'])
if 'input_encoding' in config:
    api.input_encoding = config['input_encoding']

connection = pyodbc.connect(config['db_connection'])

instances = api.get_instances()
for instance in instances:
    sch_client.printme("Processing instance", ' ')
    for key in instance['key']:
        sch_client.printme(key + "=" + instance['key'][key], ' ')
    sch_client.printme()
    num_updated, num_skipped, missing_records = execute_pull(api, connection, sql, instance['key'], columns)
    sch_client.printme("Records updated: " + str(num_updated))
    sch_client.printme("Records skipped: " + str(num_skipped))
    sch_client.printme("Missing records: " + str(missing_records))

sch_client.printme('------ End sync_pull ------')
