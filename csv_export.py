#!/usr/bin/python

import sch_client
import json
import os
import csv
import sys
import collections
import types
import traceback
from datetime import datetime
from copy import copy


__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
sch_client.init_logging(__location__, 'csv_export')

# load config file from first argument if passed
if len(sys.argv) > 1:
    configFile = sys.argv[1]
else:
    configFile = os.path.join(__location__, 'config.json')

config = json.load(open(configFile), object_pairs_hook=collections.OrderedDict)

# initialize sch api library
identifier = config['identifier'] if 'identifier' in config else None
api = sch_client.API(config['uri'], config['key'], config['secret'], identifier)

calculated_columns = config['calculated_export_columns'] if 'calculated_export_columns' in config else {}
exclude_columns = config['exclude_columns'] if 'exclude_columns' in config else []
exclude_default_columns = config['exclude_default_columns'] if 'exclude_default_columns' in config else False

# format text for output if needed
def format_output(output):
    # adjust format if if value is datetime & format specified
    if sch_client.is_string(output) and 'datetime_format' in config:
        try:
            output = datetime.strptime(output, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return output
        return output.strftime(config['datetime_format'])
    if sch_client.is_string(output):
        if 'input_encoding' in config:
            return output.encode(encoding=config['input_encoding'])
        else:
            return output.encode(encoding='utf8')
    return output

# function passed to get_calculated_columns to get a named value for the given resident
def get_field_value(iteration=0):
    recursion_limit = 16
    state = { 'iteration': iteration }
    def get(resident, field):
        value = ''
        if field in resident:
            value = resident[field]
        elif resident['residency'] and field in resident['residency']:
            value = resident['residency'][field]
        elif resident['meal_plan'] and field in resident['meal_plan']:
            value = resident['meal_plan'][field]
        # only recursively check calculated columns if we have not done so already (resident is standard dictionary)
        elif field in calculated_columns and state['iteration'] < recursion_limit:
            state['iteration'] += 1
            value = sch_client.get_calculated_column(calculated_columns[field], sch_client.FunctionDict(resident, get_field_value(state['iteration'])))
        return format_output(value)
    return get

# begin export process
api.printme('------ Begin csv_export ------')

csvname = config['export_csv'] if 'export_csv' in config else 'export.csv'
defined_column_order = True if 'export_column_order' in config else False
export_column_order = config['export_column_order'] if defined_column_order else []

with open(csvname, 'w') as csvfile:

    # names of all available columns, grouped in sets by parent key from output data
    columns = sch_client.SetDict()

    instances = api.get_instances()
    resident_lists = []  # a list of residents per instance
    writer = csv.writer(csvfile, dialect='excel')

    for instance in instances:
        api.printme("Processing instance", ' ')
        for key in instance['key']:
            api.printme(key + "='" + instance['key'][key], "' ")

            # add instance columns to column sets
            columns['instance'].add(key)

        api.printme()
        query = copy(instance['key'])
        query['include_name'] = 1
        residents = api.get_residents(query)
        resident_lists.append(residents)
        api.printme("Total Residents: " + str(len(residents)))


        # iterate to get full set of columns
        for resident in residents:
            for key in resident:

                # Add sub-columns in the event that key specifies a nested object
                # For strings, add key as column name itself grouped under 'resident'
                if sch_client.is_iterable(resident[key]):
                    for column in resident[key]:
                        columns[key].add(column)
                elif resident[key] is not None:
                    columns['resident'].add(key)

    # define default column ordering by sorting column sets (starting with first set defined)
    # needed for consistency and to place uppercase columns first
    if not exclude_default_columns and not defined_column_order:
        group_order = ['instance', 'resident', 'residency', 'meal_plan']

        # add other unique elements from columns dictionary
        group_order = group_order + list(set(columns.keys()) - set(group_order))
        for key in group_order:

            # default id and name as first resident params
            if key == 'resident':
                export_column_order = export_column_order + ['id', 'first_name', 'last_name']

            columns[key] = sorted(columns[key])
            for column in columns[key]:
                if column not in export_column_order and column not in exclude_columns:
                    export_column_order.append(column)

    # append calculated columns provided an explicit column order wasn't defined
    if not defined_column_order:
        for column in calculated_columns:
            export_column_order.append(column)

    # write header
    writer.writerow(export_column_order)

    instance_num = 0
    for instance in instances:
        residents = resident_lists[instance_num]

        # iterate to write data
        for resident in residents:

            # add instance fields to resident
            resident.update(instance['key'])
            resident_dict = sch_client.FunctionDict(resident, get_field_value())

            row = []
            for column_name in export_column_order:
                row.append(format_output(resident_dict[column_name]))

            try:
                writer.writerow(row)
            except Exception as e:
                api.printme('error printing record: ' + resident['id'])
                api.printme(traceback.format_exception(*sys.exc_info()))

        instance_num += 1

api.printme('------ End csv_export ------')
