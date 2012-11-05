#!/usr/bin/python

import sch_client
import json
import os
import csv


__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
sch_client.initLogging(__location__, 'sync_pull')
sch_client.printme('------ Begin sync_pull ------')
config = json.load(open(os.path.join(__location__, 'config.json')))
columns = json.load(open(os.path.join(__location__, config['import_map'])))
api = sch_client.API(config['uri'], config['key'], config['secret'])

csvname = config['import_csv'] if 'import_csv' in config else 'import.csv'
has_header = config['import_csv_header'] if 'import_csv_header' in config else False
with open(csvname, 'r') as csvfile:

    reader = csv.reader(csvfile, dialect='excel')
    if has_header:
        next(reader)

    def iterate():
        try:
            return next(reader)
        except StopIteration:
            return None

    num_updated = sch_client.set_residents_batch(api, iterate, columns, {}, 50)
    sch_client.printme("Records updated: " + str(num_updated))

sch_client.printme('------ End sync_pull ------')
