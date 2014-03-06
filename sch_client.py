#!/usr/bin/python

import urllib
import json
import sys
import logging
import traceback
import uuid
from copy import copy

if sys.version_info < (3, 0):
    import urllib2
else:
    import urllib.request
    import urllib.parse

# Global Logger
logger = logging.getLogger('sch_client')
log_buffer = ''
operation_id = uuid.uuid1()

def log_handler(type, value, tb):
    global logger
    trace = ''.join(traceback.format_exception(type, value, tb))
    sys.stdout.write(trace)
    logger.critical(trace)


def init_logging(dir, name):
    global logger
    hdlr = logging.FileHandler(dir + '/' + name + '.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s - ' + operation_id.hex + ' - %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    sys.excepthook = log_handler


def printme(s='', end='\n'):
    sys.stdout.write(str(s) + end)
    if logger:
        global log_buffer
        log_buffer += str(s) + end
        if len(end) > 0 and end[-1] == '\n':
            logger.info(log_buffer.rstrip())
            log_buffer = ''


def urlopen(req):
    if sys.version_info < (3, 0):
        return urllib2.urlopen(req)
    else:
        return urllib.request.urlopen(req)


def urlencode(s):
    if sys.version_info < (3, 0):
        return urllib.urlencode(s)
    else:
        return urllib.parse.urlencode(s)


def create_request(*params):
    if sys.version_info < (3, 0):
        return urllib2.Request(*params)
    else:
        return urllib.request.Request(*params)


def prepare_query(query, params):
    param_vals = []
    start = 0
    while True:
        start = query.find('$%$', start)
        if start == -1: break
        end = query.find('$%$', start + 3)
        key = query[start + 3:end]
        if key not in params:
            raise Exception("key '" + key + "' from SQL not found in input parameters")
        param_vals.append(params[key])
        query = query[:start] + '?' + query[end + 3:]
    return query, param_vals


def set_residents_batch(api, iterate, columns, params, batch_size=10):
    updated = 0
    skipped = 0
    total = 0
    batch_count = 0
    missing_records = {}
    col_filter = lambda x: not ('ignore' in x and x['ignore'])
    filtered_columns = list(filter(col_filter, columns))
    while True:
        data = []
        current_row = 1
        while len(data) < batch_size:
            row = iterate()
            if not row: break
            if len(row) != len(columns):
                raise Exception('Number of fields in CSV on record ' + str(total + current_row) + ' does not match manifest')
            record = []
            for i, val in enumerate(row):
                if col_filter(columns[i]):
                    if val is not None:
                        val = str(val).rstrip()
                    record.append(val)
            data.append(record)
            current_row += 1

        batch_count += 1
        api.printme("saving batch " + str(batch_count), "")
        api.printme(" records " + str(total + 1) + " - " + str(batch_size + total))
        result = api.set_residents(filtered_columns, data, params)
        updated += result['updated']
        skipped += result['skipped']
        total = updated + skipped
        missing_records.update(result['missing_records'])
        if len(data) < batch_size: break

    return updated, skipped, missing_records


class NestedDict(dict):
    def __getitem__(self, key):
        if key in self: return self.get(key)
        return self.setdefault(key, NestedDict())


class API:

    def __init__(self, uri, key, secret, identifier=None):
        self.identifier = identifier
        self.uri = uri
        self.key = key
        self.secret = secret
        self.auth()

    # call global printme logging function with config identifier inserted if defined
    def printme(self, s='', end='\n'):
        if self.identifier:
            s = self.identifier + ': ' + str(s);
        printme(s, end)

    def get_residents(self, options):
        options = copy(options)
        options['token'] = self.token
        uri = self.uri + '/resident?' + urlencode(options)

        try:
            self.response = urlopen(uri)
        except Exception as e:
            self.printme(e)
            self.printme(e.read().decode('utf8'))
            exit(1)

        return json.loads(self.response.read().decode('utf8'))

    def set_residents(self, columns, data, options):
        options = copy(options)
        options['token'] = self.token
        options['columns'] = columns
        options['data'] = data
        req_data = json.dumps(options).encode('utf8')
        uri = self.uri + '/resident'

        req = create_request(uri, req_data)
        req.add_header('Content-Type', 'application/json')
        req.get_method = lambda: 'PUT'

        try:
            self.response = urlopen(req)
        except Exception as e:
            self.printme(e)
            self.printme(e.read().decode('utf8'))
            exit(1)

        response = self.response.read().decode('utf8')
        try:
            json_response = json.loads(response)
        except Exception:
            self.printme("Unable to parse JSON output from API.")
            self.printme("Response:")
            self.printme(response)
            exit(1)

        return json_response

    def set_residents_inactive(self, residents, options):
        options = copy(options)
        options['token'] = self.token
        options['residents'] = residents
        options['data'] = {'model': 'ResidentInstance', 'field': 'isActive', 'value': False}

        req_data = json.dumps(options).encode('utf8')
        uri = self.uri + '/resident/update_complement'

        req = create_request(uri, req_data)
        req.add_header('Content-Type', 'application/json')
        req.get_method = lambda: 'POST'

        try:
            self.response = urlopen(req)
        except Exception as e:
            self.printme(e)
            self.printme(e.read().decode('utf8'))
            exit(1)

        response = self.response.read().decode('utf8')
        try:
            json_response = json.loads(response)
        except Exception:
            self.printme("Unable to parse JSON output from API.")
            self.printme("Response:")
            self.printme(response)
            exit(1)

        return json_response

    def get_rooms(self, options):
        options = copy(options)
        options['token'] = self.token
        uri = self.uri + '/room?' + urlencode(options)
        try:
            self.response = urlopen(uri)
        except Exception as e:
            self.printme(e)
            self.printme(e.read().decode('utf8'))
            exit(1)

        return json.loads(self.response.read().decode('utf8'))

    def get_instances(self, active=True, include_id=False):
        options = {
            'token': self.token,
            'active': 1 if active else 0
        }
        uri = self.uri + '/instance?' + urlencode(options)
        try:
            self.response = urlopen(uri)
        except Exception as e:
            self.printme(e)
            self.printme(e.read().decode('utf8'))
            exit(1)

        instances = json.loads(self.response.read().decode('utf8'))
        for instance in instances:
            if len(instance) == 1:
                msg = "API ERROR: Instance '" + str(instance['id']) + "' does not have mapped fields"
                raise Exception(msg)
            if not include_id:
                del instance['id']
        return instances

    def auth(self):
        data = json.dumps({'key': self.key, 'secret': self.secret}).encode('utf8')
        req = create_request(self.uri + '/auth', data, {'Content-Type': 'application/json'})

        try:
            self.response = urlopen(req)
        except Exception as e:
            self.printme(e)
            self.printme(e.read().decode('utf8'))
            exit(1)

        self.token = json.loads(self.response.read().decode('utf8'))['token']
