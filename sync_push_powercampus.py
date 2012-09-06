#!/usr/bin/python

import sch_client
import json
import pyodbc

config = json.load(open('config.json'))
api = sch_client.API(config['uri'], config['key'], config['secret'])
connection = pyodbc.connect(config['db_connection'])
cursor = connection.cursor()

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
 REVISION_OPID = 'SCH',
 REVISION_DATE = DATEADD(dd, 0, DATEDIFF(dd, 0, GETDATE())),
 REVISION_TIME = DATEADD(ss, DATEDIFF(ss, DATEADD(dd, 0, DATEDIFF(dd, 0, GETDATE())), GETDATE()), 0)
WHERE PEOPLE_CODE_ID = $%$id$%$
AND ACADEMIC_YEAR = $%$ACADEMIC_YEAR$%$
AND ACADEMIC_TERM = $%$ACADEMIC_TERM$%$
AND ACADEMIC_SESSION = $%$ACADEMIC_SESSION$%$"""

mealplan_update = """
UPDATE Residency
SET FOOD_PLAN = $%$FOOD_PLAN$%$,
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

        # Update Residency
        if resident['residency']:
            # standard update
            sch_client.printme("Updating Residency for " + params['id'], ": ")
            sch_client.printme(json.dumps(resident['residency']))
            params.update(resident['residency'])
            params['ACADEMIC_SESSION'] = config['powercampus']['push_params']['ACADEMIC_SESSION']
            query, query_params = sch_client.prepare_query(residency_update, params)
            cursor.execute(query, *query_params)
        else:
            # check that we aren't overriding existing commuter status
            sch_client.printme("Setting null Residency for " + params['id'])
            query, query_params = sch_client.prepare_query(residency_select, params)
            exit()
            cursor.execute(query, *query_params)

        # Update Meal Plan separately so updates are only run if value is set
        # FOOD_PLAN should never be set to null by this script
        if resident['meal_plan']:
            sch_client.printme("Updating meal plan for " + params['id'], ": ")
            sch_client.printme(json.dumps(resident['meal_plan']))
            params['FOOD_PLAN'] = resident['meal_plan']['FOOD_PLAN']
            query, query_params = sch_client.prepare_query(mealplan_update, params)
            cursor.execute(query, *query_params)

    # sch_client.printme(len(residents))
connection.commit()
connection.close()
