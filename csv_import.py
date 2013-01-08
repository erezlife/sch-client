#!/usr/bin/python

import sch_client
import json
import os
import csv


__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
sch_client.initLogging(__location__, 'csv_import')
sch_client.printme('------ Begin csv_import ------')
config = json.load(open(os.path.join(__location__, 'config.json')))
columns = json.load(open(os.path.join(__location__, config['import_map'])))
api = sch_client.API(config['uri'], config['key'], config['secret'])

csvname = config['import_csv'] if 'import_csv' in config else 'import.csv'
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

    # given a resident and a rule, determines if that resident satisfies the rule
    def match_rule(rule, resident):
        field_name = rule['field']
        try:
            value = resident[named_columns[field_name]]
        except KeyError:
            raise Exception("Column named '" + field_name + "' is not defined")

        # convert to a float if possible, otherwise use string
        try:
            value = float(value)
        except ValueError:
            pass

        operator = rule['operator'] if 'operator' in rule else 'EQ'
        if operator == 'EQ':
            return value == rule['value']
        elif operator == 'LT':
            return value < rule['value']
        elif operator == 'LTE':
            return value <= rule['value']
        elif operator == 'GT':
            return value > rule['value']
        elif operator == 'GTE':
            return value >= rule['value']
        elif operator == 'NE':
            return value != rule['value']
        else:
            raise Exception("Operator '" + operator + "' not defined")

    # get a list of calculated column values for the given resident
    def get_calculated_columns(resident):
        outputs = []
        for i, column in enumerate(calculated_columns):
            outputs.append(column['default'])
            for condition in column['conditions']:
                if isinstance(condition['rules'], dict):
                    valid = match_rule(condition['rules'], resident)
                else:
                    valid = True
                    for rule in condition['rules']:
                        valid = valid and match_rule(rule, resident)
                        if not valid: break
                if valid:
                    outputs[i] = condition['output']
                    break
        return outputs

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

        resident += get_calculated_columns(resident)
        if deactivate_missing:
            resident_id, instance_id = get_resident_instance_ids(resident)
            if resident_id and instance_id:
                if instance_id in resident_ids:
                    resident_ids[instance_id].append(resident_id)
                else:
                    resident_ids[instance_id] = [resident_id]
        return resident

    num_updated, num_skipped, missing_records = sch_client.set_residents_batch(api, iterate, columns, {}, 50)

    num_deactivated = 0
    if deactivate_missing:
        for instance in instances:
            instance_id = instance['id']
            if instance_id in resident_ids and len(resident_ids[instance_id]) > 0:
                del instance['id']
                sch_client.printme("deactivating records for", ' ')
                for key in instance:
                    sch_client.printme(key + "='" + instance[key], "' ")
                sch_client.printme()

                result = api.set_residents_inactive(resident_ids[instance_id], instance)
                num_deactivated += result['updated']

    sch_client.printme("Records updated: " + str(num_updated))
    sch_client.printme("Records skipped: " + str(num_skipped))
    sch_client.printme("Records deactivated: " + str(num_deactivated))
    if len(missing_records) > 0:
        sch_client.printme("Missing records:")
        for model, conditions in missing_records.items():
            sch_client.printme("  " + model, ": ")
            for field, value in conditions.items():
                sch_client.printme(field + " = '" + value, "' ")
            sch_client.printme()
sch_client.printme('------ End csv_import ------')
