#!/usr/bin/python

import sch_client
import json
import os
import csv
import sys
from copy import copy


# function passed to get_calculated_columns to get a named value for the given resident
def get_field_value(resident, field):
    value = None
    if field in resident:
        value = resident[field]
    elif resident['residency'] and field in resident['residency']:
        value = resident['residency'][field]
    elif resident['meal_plan'] and field in resident['meal_plan']:
        value = resident['meal_plan'][field]
    return value


__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
sch_client.init_logging(__location__, 'csv_export')

# load config file from first argument if passed
if len(sys.argv) > 1:
    configFile = sys.argv[1]
else:
    configFile = os.path.join(__location__, 'config.json')

config = json.load(open(configFile))

# initialize sch api library
identifier = config['identifier'] if 'identifier' in config else None
api = sch_client.API(config['uri'], config['key'], config['secret'], identifier)

# begin export process
api.printme('------ Begin csv_export ------')

csvname = config['export_csv'] if 'export_csv' in config else 'export.csv'
with open(csvname, 'w') as csvfile:

    instance_columns = set()
    resident_columns = set()
    residency_columns = set()
    mealplan_columns = set()
    calculated_columns = config['calculated_export_columns'] if 'calculated_export_columns' in config else {}
    exclude_columns = config['exclude_columns'] if 'exclude_columns' in config else []
    exclude_default_columns = config['exclude_default_columns'] if 'exclude_default_columns' in config else False

    instances = api.get_instances()
    resident_lists = []  # a list of residents per instance
    writer = csv.writer(csvfile, dialect='excel')

    for instance in instances:
        api.printme("Processing instance", ' ')
        for key in instance:
            api.printme(key + "='" + instance[key], "' ")
        api.printme()
        query = copy(instance)
        query['include_name'] = 1
        residents = api.get_residents(query)
        resident_lists.append(residents)
        api.printme("Total Residents: " + str(len(residents)))

        # iterate to get full set of columns
        if not exclude_default_columns:
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
    if not exclude_default_columns:
        resident_columns = sorted(resident_columns)
        resident_columns.insert(0, 'id')
        resident_columns.insert(1, 'first_name')
        resident_columns.insert(2, 'last_name')
        residency_columns = sorted(residency_columns)
        mealplan_columns = sorted(mealplan_columns)
        instance_columns = sorted(instance)

    # write header
    header = []
    for key in instance_columns:
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
            for key in instance_columns:
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
            resident_dict = sch_client.FunctionDict(resident, get_field_value)
            row += sch_client.get_calculated_columns(calculated_columns.values(), resident_dict)
            writer.writerow(row)

        instance_num += 1

api.printme('------ End csv_export ------')
