#!/usr/bin/python

import sch_client
import json

config = json.load(open('config.json'))
api = sch_client.API(config['uri'], config['key'], config['secret'])

instances = api.get_instances()
for instance in instances:
    residents = api.get_residents(instance)
    print(len(residents))
