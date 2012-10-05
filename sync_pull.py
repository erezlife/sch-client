#!/usr/bin/python

import sch_client
import json
import pyodbc
import os

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
sch_client.initLogging(__location__, 'sync_pull')
sch_client.printme('------ Begin sync_pull ------')
config = json.load(open(os.path.join(__location__, 'config.json')))
sql = open(os.path.join(__location__, config['pull_sql'])).read()
columns = json.load(open(os.path.join(__location__, config['pull_map'])))
api = sch_client.API(config['uri'], config['key'], config['secret'])
connection = pyodbc.connect(config['db_connection'])

instances = api.get_instances()
for instance in instances:
    sch_client.printme("Processing instance", ' ')
    for key in instance:
        sch_client.printme(key + "=" + instance[key], ' ')
    sch_client.printme()
    num_updated = sch_client.execute_pull_query(api, connection, sql, instance, columns)
    sch_client.printme("Records updated: " + str(num_updated))

sch_client.printme('------ End sync_pull ------')
