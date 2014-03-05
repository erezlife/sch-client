#!/usr/bin/python

import sch_client
import json
import os
import csv
import sys
from copy import copy


def get_calculated_columns(resident, calculated_columns):
    outputs = []
    column_num = 0
    for column in calculated_columns:
        outputs.append(None)
        for rule in calculated_columns[column]:
            output = rule['output'] if 'output' in rule else None

            field = rule['field'] if 'field' in rule else None
            if not field:
                outputs[column_num] = output
                break

            value = rule['value'] if 'value' in rule else None

            field_value = None
            if field in resident:
                field_value = resident[field]
            elif resident['residency'] and field in resident['residency']:
                field_value = resident['residency'][field]
            elif resident['meal_plan'] and field in resident['meal_plan']:
                field_value = resident['residency'][field]

            if (value and field_value == value) or (not value and field_value):
                outputs[column_num] = output
                break
        column_num += 1
    return outputs


__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
sch_client.initLogging(__location__, 'csv_export')
sch_client.printme('------ Begin csv_export ------')

# load config file from first argument if passed
if len(sys.argv) > 1:
    configFile = sys.argv[1]
else:
    configFile = os.path.join(__location__, 'config.json')

config = json.load(open(configFile))

api = sch_client.API(config['uri'], config['key'], config['secret'])

csvname = config['export_csv'] if 'export_csv' in config else 'export.csv'
with open(csvname, 'w') as csvfile:

    resident_columns = set()
    residency_columns = set()
    mealplan_columns = set()
    calculated_columns = config['calculated_export_columns'] if 'calculated_export_columns' in config else {}
    exclude_columns = config['exclude_columns'] if 'exclude_columns' in config else []

    instances = api.get_instances()
    resident_lists = []  # a list of residents per instance
    writer = csv.writer(csvfile, dialect='excel')

    for instance in instances:
        sch_client.printme("Processing instance", ' ')
        for key in instance:
            sch_client.printme(key + "='" + instance[key], "' ")
        sch_client.printme()
        query = copy(instance)
        query['include_name'] = 1
        residents = api.get_residents(query)
        resident_lists.append(residents)
        sch_client.printme("Total Residents: " + str(len(residents)))

        # iterate to get full set of columns
        for resident in residents:
            for key in resident:
                if key not in ['id', 'residency', 'meal_plan', 'first_name', 'last_name'] + exclude_columns:
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
    for column in calculated_columns:
        header.append(column)
    writer.writerow(header)

    instance_num = 0
    for instance in instances:
        residents = resident_lists[instance_num]

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
            row += get_calculated_columns(resident, calculated_columns)
            writer.writerow(row)

        instance_num += 1

sch_client.printme('------ End csv_export ------')
