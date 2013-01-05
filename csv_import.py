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
named_columns = {}
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

    # define iterator for batch resident function
    def iterate():
        try:
            resident = next(reader)
        except StopIteration:
            return None

        resident += get_calculated_columns(resident)
        return resident

    num_updated = sch_client.set_residents_batch(api, iterate, columns, {}, 50)
    sch_client.printme("Records updated: " + str(num_updated))

sch_client.printme('------ End csv_import ------')
