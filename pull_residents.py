#!/usr/bin/python

import sch_client
import json
import pyodbc

config = json.load(open('config.json'))
sql = open('db/select-resident.jenzabar.sql').read()
columns = json.load(open('db/select-resident.jenzabar.json'))
api = sch_client.API(config['uri'], config['key'], config['secret'])
connection = pyodbc.connect(config['db_connection'])

instances = api.get_instances()
for instance in instances:
    print "Processing instance ",
    for key in instance:
        print key + "=" + instance[key] + " ",
    print
    num_updated = sch_client.execute_pull_query(api, connection, sql, instance, columns)
    print "Records updated: " + str(num_updated)
