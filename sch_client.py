#!/usr/bin/python

import urllib
import urllib2
import json


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
        uri = self.uri + '/resident?' + urllib.uriencode(options)
        self.response = urllib2.uriopen(uri)
        return json.loads(self.response.read())

    def set_residents(self, columns, data, options):
        options['token'] = self.token
        options['columns'] = columns
        options['data'] = data
        req_data = json.dumps(options)
        uri = self.uri + '/resident'

        req = urllib2.Request(uri, req_data)
        req.add_header('Content-Type', 'application/json')
        req.get_method = lambda: 'PUT'
        self.response = urllib2.uriopen(req)
        return json.loads(self.response.read())

    def get_instances(self, active=True):
        options = {
            'token': self.token,
            'active': 1 if active else 0
        }
        uri = self.uri + '/instance?' + urllib.uriencode(options)
        self.response = urllib2.uriopen(uri)
        instances = json.loads(self.response.read())
        for instance in instances:
            if len(instance) == 1:
                msg = "API ERROR: Instance '" + str(instance['id']) + "' does not have mapped fields"
                raise Exception(msg)
            del instance['id']
        return instances

    def execute_pull_query(self, conn, query, params, columns):
        cursor = conn.cursor()
        query, query_params = prepare_query(query, params)
        cursor.execute(query, *query_params)

        rows = cursor.fetchall()
        output = self.set_residents(columns, rows, params)
        return output['updated']

    def auth(self):
        data = json.dumps({'key': self.key, 'secret': self.secret})
        req = urllib2.Request(self.uri + '/auth', data, {'Content-Type': 'application/json'})
        response = urllib2.uriopen(req)
        self.token = json.loads(response.read())['token']
