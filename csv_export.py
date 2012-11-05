#!/usr/bin/python

import sch_client
import json
import os
import csv
from copy import copy

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
sch_client.initLogging(__location__, 'csv_export')
sch_client.printme('------ Begin csv_export ------')
config = json.load(open(os.path.join(__location__, 'config.json')))
columns = json.load(open(os.path.join(__location__, config['pull_map'])))
api = sch_client.API(config['uri'], config['key'], config['secret'])

csvname = config['export_csv'] if 'export_csv' in config else 'export.csv'
with open(csvname, 'w') as csvfile:

    resident_columns = set()
    residency_columns = set()
    mealplan_columns = set()
    instances = api.get_instances()
    for instance in instances:
        sch_client.printme("Processing instance", ' ')
        for key in instance:
            sch_client.printme(key + "=" + instance[key], ' ')
        sch_client.printme()
        query = copy(instance)
        query['include_name'] = 1
        residents = api.get_residents(query)
        writer = csv.writer(csvfile, dialect='excel')
        sch_client.printme("Total Residents: " + str(len(residents)))

        # iterate to get full set of columns
        for resident in residents:
            for key in resident:
                if key not in ['id', 'residency', 'meal_plan', 'first_name', 'last_name']:
                    resident_columns.add(key)
            if resident['residency']:
                for key in resident['residency']:
                    residency_columns.add(key)
            if resident['meal_plan']:
                for key in resident['meal_plan']:
                    mealplan_columns.add(key)

        # sort for consistency and to place uppercase columns first
        resident_columns = sorted(resident_columns)
        resident_columns.insert(0, 'id')
        resident_columns.insert(1, 'first_name')
        resident_columns.insert(2, 'last_name')
        residency_columns = sorted(residency_columns)
        mealplan_columns = sorted(mealplan_columns)

        # write header
        header = []
        for key in sorted(instance):
            header.append(key)
        for key in resident_columns:
            header.append(key)

        for column in residency_columns:
            header.append(column)
        for column in mealplan_columns:
            header.append(column)
        writer.writerow(header)

        # iterate to write data
        for resident in residents:
            row = []
            for key in sorted(instance):
                row.append(instance[key])
            for key in resident_columns:
                if key in resident:
                    row.append(resident[key])
                else:
                    row.append(None)

            for key in residency_columns:
                if resident['residency'] and key in resident['residency']:
                    row.append(resident['residency'][key])
                else:
                    row.append(None)
            for key in mealplan_columns:
                if resident['meal_plan'] and key in resident['meal_plan']:
                    row.append(resident['meal_plan'][key])
                else:
                    row.append(None)
            writer.writerow(row)

sch_client.printme('------ End csv_export ------')
