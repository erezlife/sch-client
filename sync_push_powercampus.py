#!/usr/bin/python

import sch_client
import json
import pyodbc

config = json.load(open('config.json'))
api = sch_client.API(config['uri'], config['key'], config['secret'])
connection = pyodbc.connect(config['db_connection'])
cursor = connection.cursor()
verbose = False

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
 RESIDENT_COMMUTER = $%$RESIDENT_COMMUTER$%$,
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

res_update_count = 0
res_null_count = 0
meal_update_count = 0
instances = api.get_instances()
for instance in instances:
    sch_client.printme("Processing instance", ' ')
    for key in instance:
        sch_client.printme(key + "=" + instance[key], ' ')
    sch_client.printme()

    residents = api.get_residents(instance)
    sch_client.printme("Total Residents: " + str(len(residents)))
    for resident in residents:
        params = instance
        params['id'] = resident['id'] if resident['id'][0] == 'P' else 'P' + resident['id']
        params['ACADEMIC_SESSION'] = config['powercampus']['push_params']['ACADEMIC_SESSION']

        # Update Residency
        if resident['residency']:
            # standard update
            if verbose:
                sch_client.printme("Updating Residency for " + params['id'], ": ")
                sch_client.printme(json.dumps(resident['residency']))
            params.update(resident['residency'])
            params['RESIDENT_COMMUTER'] = 'R'
            query, query_params = sch_client.prepare_query(residency_update, params)
            res_update_count += cursor.execute(query, *query_params).rowcount
        else:
            # check that we aren't overriding existing commuter status
            if verbose:
                sch_client.printme("Setting null Residency for " + params['id'])
            query, query_params = sch_client.prepare_query(residency_select, params)
            cursor.execute(query, *query_params)
            row = cursor.fetchone()
            params['DORM_CAMPUS'] = None
            params['DORM_PLAN'] = None
            params['DORM_BUILDING'] = None
            params['DORM_ROOM'] = None
            params['RESIDENT_COMMUTER'] = None
            if row and row.RESIDENT_COMMUTER and row.RESIDENT_COMMUTER != 'R':
                params['RESIDENT_COMMUTER'] = row.RESIDENT_COMMUTER

            query, query_params = sch_client.prepare_query(residency_update, params)
            res_null_count += cursor.execute(query, *query_params).rowcount

        # Update Meal Plan separately so updates are only run if value is set
        # FOOD_PLAN should never be set to null by this script
        if resident['meal_plan']:
            if verbose:
                sch_client.printme("Updating meal plan for " + params['id'], ": ")
                sch_client.printme(json.dumps(resident['meal_plan']))
            params['FOOD_PLAN'] = resident['meal_plan']['FOOD_PLAN']
            query, query_params = sch_client.prepare_query(mealplan_update, params)
            meal_update_count += cursor.execute(query, *query_params).rowcount

    connection.commit()
    sch_client.printme("Residency updates: " + str(res_update_count + res_null_count), " ")
    sch_client.printme("(" + " (" + str(res_update_count) + " placed, " + str(res_null_count) + " unplaced)")
    sch_client.printme("Meal Plan updates: " + str(meal_update_count))
    sch_client.printme("Record(s) not found: " + str(len(residents) - res_update_count - res_null_count))

connection.close()
