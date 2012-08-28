#!/usr/bin/python

import urllib
import urllib2
import json


class API:

    def __init__(self, url, key, secret):
        self.url = url
        self.key = key
        self.secret = secret
        self.auth()

    def get_residents(self, options):
        options['token'] = self.token
        url = self.url + '/resident?' + urllib.urlencode(options)
        self.response = urllib2.urlopen(url)
        return json.loads(self.response.read())

    def set_residents(self, columns, data, options):
        options['token'] = self.token
        options['columns'] = columns
        options['data'] = data
        req_data = json.dumps(options)
        url = self.url + '/resident'

        req = urllib2.Request(url, req_data)
        req.add_header('Content-Type', 'application/json')
        req.get_method = lambda: 'PUT'
        self.response = urllib2.urlopen(req)
        return json.loads(self.response.read())

    def auth(self):
        data = json.dumps({'key': self.key, 'secret': self.secret})
        req = urllib2.Request(self.url + '/auth', data, {'Content-Type': 'application/json'})
        response = urllib2.urlopen(req)
        self.token = json.loads(response.read())['token']
