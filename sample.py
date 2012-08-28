#!/usr/bin/python

import sch_client

key = 'yEm9RJSTbO0WnjXtJuGnWNldSV7F9TVI'
secret = '$2a$10$HPP4hp2SA/GR6S7UsgDqjeMRnsmmea/rSF/rH/OMN02UQY5067CVi'
host = 'http://sch.site/api'

api = sch_client.API(host, key, secret)
output = api.get_residents({'sess_cde': '1213FA'})
print(len(output))

columns = [
    {'name': 'id'},
    {"model": "Resident", "field": "firstName"},
    {"model": "Resident", "field": "lastName"},
    {"model": "Resident", "field": "gender"},
    {"model": "Resident", "field": "birthDate"},
    {"model": "Resident", "field": "addressPart", "arguments": ["address1"]}
]

data = [
    ["0034235", "John", "Doe", "M", "1/1/1994", "123 Awesome Street"],
    ["0063453", "Jane", "Smith", "F", "10/21/1995", "5634 W Grandeur Dr"],
    ["0012345", "Joseph", "Smith", "M", "2/15/1987", "101 Random Ave"]
]

output = api.set_residents(columns, data, {'sess_cde': '1213FA'})

print(output)
