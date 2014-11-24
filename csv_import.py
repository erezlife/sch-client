#!/usr/bin/python

import sch_client
import json
import os
import csv
import sys
import argparse

parser = argparse.ArgumentParser(description='Get config file and import file name from command line')

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
sch_client.init_logging(__location__, 'csv_import')

# load config file from first argument if passed

parser.add_argument('-f', dest='csvname', nargs='?', default = None)
parser.add_argument('configFile', nargs='?', default=os.path.join(__location__, 'config.json'))

args = parser.parse_args()
configFile = args.configFile

config = json.load(open(configFile))
csvtemp = config['import_csv'] if 'import_csv' in config else 'import.csv'
csvname = args.csvname if args.csvname is not None else csvtemp

# initialize sch api library
identifier = config['identifier'] if 'identifier' in config else None
api = sch_client.API(config['uri'], config['key'], config['secret'], identifier)
if 'input_encoding' in config:
    api.input_encoding = config['input_encoding']

# begin import process
api.printme('------ Begin csv_import ------')

columns = json.load(open(os.path.join(__location__, config['import_map'])))
has_header = config['import_csv_header'] if 'import_csv_header' in config else False
calculated_columns = config['calculated_import_columns'] if 'calculated_import_columns' in config else []
deactivate_missing = config['deactivate_missing_residents'] if 'deactivate_missing_residents' in config else False
named_columns = {}
resident_ids = {}   # dictionary of resident id lists for each instance


with open(csvname, 'r') as csvfile:

    reader = csv.reader(csvfile, dialect='excel')

    # store named columns
    if has_header:
        header = next(reader)
        for i, val in enumerate(header):
            named_columns[val] = i

    for i, column in enumerate(columns):
        if 'name' in column:
            named_columns[column['name']] = i

    # add calculated columns to mapping
    for column in calculated_columns:
        columns.append(column['map'])

    if deactivate_missing:
        instances = api.get_instances(True, True)

    # closure passed to get_calculated_columns to get a named value for the given resident
    def get_field_value(resident, field_name):
        try:
            value = resident[named_columns[field_name]]
        except KeyError:
            raise Exception("Column named '" + str(field_name) + "' is not defined")

        # strip whitespace
        value = value.strip()

        # convert to a float if possible, otherwise use string
        try:
            value = float(value)
        except ValueError:
            pass

        return value

    # closure to get resident external id and instance id from data
    def get_resident_instance_ids(resident):
        instance_id = None
        resident_id = None
        for i, column in enumerate(columns):
            if 'assnExtLookupField' in column and 'field' in column and column['field'] == 'instance':
                for instance in instances:
                    value_match = field_match = False
                    for key, value in instance.items():
                        if key == column['assnExtLookupField']:
                            field_match = True
                            # skip to next instance if field does not match
                            if value == resident[i]:
                                value_match = True
                        if field_match and not value_match:
                            break  # move on to next instance
                    if field_match and value_match:
                        instance_id = instance['id']
            elif 'field' in column and column['field'] == 'externalId' or 'name' in column and column['name'] == 'id':
                resident_id = resident[i]
        return resident_id, instance_id

    # define iterator for batch resident function
    def iterate():
        try:
            resident = next(reader)
        except StopIteration:
            return None

        resident_dict = sch_client.FunctionDict(resident, get_field_value)
        resident += sch_client.get_calculated_columns(calculated_columns, resident_dict)
        if deactivate_missing:
            resident_id, instance_id = get_resident_instance_ids(resident)
            if resident_id and instance_id:
                if instance_id in resident_ids:
                    resident_ids[instance_id].append(resident_id)
                else:
                    resident_ids[instance_id] = [resident_id]
        return resident

    num_updated, num_skipped, missing_records = sch_client.set_residents_batch(api, iterate, columns, {}, 10)

    num_deactivated = 0
    if deactivate_missing:
        for instance in instances:
            instance_id = instance['id']
            if instance_id in resident_ids and len(resident_ids[instance_id]) > 0:
                del instance['id']
                api.printme("deactivating records for", ' ')
                for key in instance:
                    api.printme(key + "='" + instance[key], "' ")
                api.printme()

                result = api.set_residents_inactive(resident_ids[instance_id], instance)
                num_deactivated += result['updated']

    api.printme("Records updated: " + str(num_updated))
    api.printme("Records skipped: " + str(num_skipped))
    api.printme("Records deactivated: " + str(num_deactivated))
    if len(missing_records) > 0:
        api.printme("Missing records:")
        for model, conditions in missing_records.items():
            api.printme("  " + model, ": ")
            for field, value in conditions.items():
                api.printme(field + " = '" + value, "' ")
            api.printme()
api.printme('------ End csv_import ------')
