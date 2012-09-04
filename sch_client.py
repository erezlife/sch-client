#!/usr/bin/python

import urllib
import json
import sys

if sys.version_info < (3, 0):
    import urllib2
else:
    import urllib.request

def printme(s="", end="\n"):
    sys.stdout.write(str(s) + end)

def urlopen(req):
    if sys.version_info < (3, 0):
        return urllib2.urlopen(req)
    else:
        return urllib.request.urlopen(req)

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
        key = query[start+3:end]
        if key not in params:
            raise Exception("key '" + key + "' from SQL not found in input parameters")
        param_vals.append(params[key])
        query = query[:start] + '?' + query[end + 3:]
    return query, param_vals

def execute_pull_query(api, conn, query, params, columns):
    cursor = conn.cursor()
    query, query_params = prepare_query(query, params)
    cursor.execute(query, *query_params)

    updated = 0
    batch_count = 0
    batch_size = 50
    while True:
        data = []
        while len(data) < batch_size:
            row = cursor.fetchone()
            if not row: break
            record = []
            for val in row:
                if val is None:
                    record.append(val)
                else:
                    record.append(str(val).rstrip())
            data.append(record)

        batch_count += 1
        printme("saving batch " + str(batch_count),"")
        printme(" records " + str(updated+1) + " - " + str(batch_size + updated))
        result = api.set_residents(columns, data, params)
        updated += result['updated']
        if len(data) < batch_size: break

    return updated

class API:

    def __init__(self, uri=None, key=None, secret=None):
        if(not uri or not key or not secret):
            config = json.load(open('config.json'))

        self.uri = uri if uri else config['uri']
        self.key = key if key else config['key']
        self.secret = secret if secret else config['secret']
        self.auth()

    def get_residents(self, options):
        options['token'] = self.token
        uri = self.uri + '/resident?' + urllib.urlencode(options)
        self.response = urlopen(uri)
        return json.loads(self.response.read())

    def set_residents(self, columns, data, options):
        options['token'] = self.token
        options['columns'] = columns
        options['data'] = data
        req_data = json.dumps(options)
        uri = self.uri + '/resident'

        req = create_request(uri, req_data)
        req.add_header('Content-Type', 'application/json')
        req.get_method = lambda: 'PUT'
        self.response = urlopen(req)
        return json.loads(self.response.read())

    def get_instances(self, active=True):
        options = {
            'token': self.token,
            'active': 1 if active else 0
        }
        uri = self.uri + '/instance?' + urllib.urlencode(options)
        self.response = urlopen(uri)
        instances = json.loads(self.response.read())
        for instance in instances:
            if len(instance) == 1:
                msg = "API ERROR: Instance '" + str(instance['id']) + "' does not have mapped fields"
                raise Exception(msg)
            del instance['id']
        return instances

    def auth(self):
        data = json.dumps({'key': self.key, 'secret': self.secret})
        req = create_request(self.uri + '/auth', data, {'Content-Type': 'application/json'})
        response = urlopen(req)
        self.token = json.loads(response.read())['token']
