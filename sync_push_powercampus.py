#!/usr/bin/python

import sch_client
import json
import pyodbc

config = json.load(open('config.json'))
api = sch_client.API(config['uri'], config['key'], config['secret'])
connection = pyodbc.connect(config['db_connection'])

residency_select = """
SELECT RESIDENT_COMMUTER
FROM Residency
WHERE PEOPLE_CODE_ID = $%$id$%$
AND ACADEMIC_YEAR = $%$ACADEMIC_YEAR$%$
AND ACADEMIC_TERM = $%$ACADEMIC_TERM$%$
AND ACADEMIC_SESSION = $%$ACADEMIC_SESSION$%$
"""

residency_update = """
UPDATE Residency
SET DORM_CAMPUS = $%$DORM_CAMPUS$%$,
 RESIDENT_COMMUTER = 'R',
 DORM_PLAN = $%$DORM_PLAN$%$,
 DORM_BUILDING = $%$DORM_BUILDING$%$,
 DORM_ROOM = $%$DORM_ROOM$%$,
 FOOD_PLAN = $%$FOOD_PLAN$%$,
 REVISION_OPID = 'SCH',
 REVISION_DATE = DATEADD(dd, 0, DATEDIFF(dd, 0, GETDATE())),
 REVISION_TIME = DATEADD(ss, DATEDIFF(ss, DATEADD(dd, 0, DATEDIFF(dd, 0, GETDATE())), GETDATE()), 0)
WHERE PEOPLE_CODE_ID = $%$id$%$
AND ACADEMIC_YEAR = $%$ACADEMIC_YEAR$%$
AND ACADEMIC_TERM = $%$ACADEMIC_TERM$%$
AND ACADEMIC_SESSION = $%$ACADEMIC_SESSION$%$"""

instances = api.get_instances()
for instance in instances:
    residents = api.get_residents(instance)
    for resident in residents:
        params = instance
        params['id'] = resident['id'] if resident['id'][0] == 'P' else 'P' + resident['id']
        if resident['residency']:
            # standard update
            params.update(resident['residency'])
            query, query_params = sch_client.prepare_query(residency_update, params)
        else:
            # check that we aren't overriding existing commuter status
            cursor = connection.cursor()
            query, query_params = sch_client.prepare_query(residency_select, params)
            #cursor.execute(query, *query_params)

        sch_client.printme(query)
        sch_client.printme(query_params)
        exit()
    # sch_client.printme(len(residents))
