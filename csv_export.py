#!/usr/bin/python

import sch_client
import json
import os
import csv

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
sch_client.initLogging(__location__, 'csv_export')
sch_client.printme('------ Begin csv_export ------')
config = json.load(open(os.path.join(__location__, 'config.json')))
columns = json.load(open(os.path.join(__location__, config['pull_map'])))
api = sch_client.API(config['uri'], config['key'], config['secret'])

csvname = config['export_csv'] if 'export_csv' in config else 'export.csv'
with open(csvname, 'wb') as csvfile:

    residency_columns = set()
    mealplan_columns = set()
    instances = api.get_instances()
    for instance in instances:
        sch_client.printme("Processing instance", ' ')
        for key in instance:
            sch_client.printme(key + "=" + instance[key], ' ')
        sch_client.printme()
        residents = api.get_residents(instance)
        writer = csv.writer(csvfile, dialect='excel')
        sch_client.printme("Total Residents: " + str(len(residents)))

        # iterate to get full set of columns
        for resident in residents:
            if resident['residency']:
                for key in resident['residency']:
                    residency_columns.add(key)
            if resident['meal_plan']:
                for key in resident['meal_plan']:
                    mealplan_columns.add(key)

        # sort for consistency and to place uppercase columns first
        residency_columns = sorted(residency_columns)
        mealplan_columns = sorted(mealplan_columns)

        # write header
        header = ['id']
        for key in sorted(instance):
            header.append(key)
        header.append('application_time')

        for column in residency_columns:
            header.append(column)
        for column in mealplan_columns:
            header.append(column)
        writer.writerow(header)

        # iterate to write data
        for resident in residents:
            row = [resident['id']]
            for key in sorted(instance):
                row.append(instance[key])
            row.append(resident['application_time'])

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
