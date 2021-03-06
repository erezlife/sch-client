#!/usr/bin/python

import sch_client
import json
import pyodbc
import os

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
sch_client.init_logging(__location__, 'sync_push_powercampus')
sch_client.printme('------ Begin sync_push_powercampus ------')
config = json.load(open(os.path.join(__location__, 'config.json')))
api = sch_client.API(config['uri'], config['key'], config['secret'])
connection = pyodbc.connect(config['db_connection'])
cursor = connection.cursor()
verbose = False

residency_select = """
SELECT RESIDENT_COMMUTER, DORM_CAMPUS, DORM_BUILDING, DORM_ROOM, DORM_PLAN
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

residency_update_no_status = """
UPDATE Residency
SET DORM_CAMPUS = $%$DORM_CAMPUS$%$,
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
    res_update_count = 0
    res_null_count = 0
    res_null_skipped_count = 0
    meal_update_count = 0
    sch_client.printme("Processing instance", ' ')
    for key in instance['key']:
        sch_client.printme(key + "=" + instance['key'][key], ' ')
    sch_client.printme()

    rooms = api.get_rooms(instance['key'])
    dorm_room_set = set()
    dorm_building_set = set()
    dorm_campus_set = set()

    sch_client.printme("Total Rooms: " + str(len(rooms)))
    # save hall/room information for future reference
    for room in rooms:
        dorm_room_set.add((room['DORM_CAMPUS'], room['DORM_BUILDING'], room['DORM_ROOM']))
        dorm_building_set.add(room['DORM_BUILDING'])
        dorm_campus_set.add(room['DORM_CAMPUS'])

    residents = api.get_residents(instance['key'])
    sch_client.printme("Total Residents: " + str(len(residents)))
    for resident in residents:
        params = instance['key']
        params['id'] = resident['id'] if resident['id'][0] == 'P' else 'P' + resident['id']
        params['ACADEMIC_SESSION'] = config['powercampus']['push_params']['ACADEMIC_SESSION']
        params['DORM_CAMPUS'] = None
        params['DORM_PLAN'] = None
        params['DORM_BUILDING'] = None
        params['DORM_ROOM'] = None

        # Update Residency
        if resident['residency']:
            # standard update
            if verbose:
                sch_client.printme("Updating Residency for " + params['id'], ": ")
                sch_client.printme(json.dumps(resident['residency']))
            params.update(resident['residency'])
            if 'DORM_ROOM' in resident['residency'] and resident['residency']['DORM_ROOM']:
                params['RESIDENT_COMMUTER'] = 'R'
                query, query_params = sch_client.prepare_query(residency_update, params)
            else:
                query, query_params = sch_client.prepare_query(residency_update_no_status, params)
            res_update_count += cursor.execute(query, *query_params).rowcount
        else:
            query, query_params = sch_client.prepare_query(residency_select, params)
            cursor.execute(query, *query_params)
            row = cursor.fetchone()
            not_resident = not row or row.RESIDENT_COMMUTER != 'R' and not row.DORM_CAMPUS and not row.DORM_BUILDING and not row.DORM_ROOM
            known_room = row and (row.DORM_CAMPUS, row.DORM_BUILDING, row.DORM_ROOM) in dorm_room_set
            billcode_override = row and row.DORM_PLAN and not row.DORM_CAMPUS and not row.DORM_BUILDING and not row.DORM_ROOM

            # only erase residency for rooms controlled by SCH
            if not_resident or known_room or billcode_override:
                if verbose:
                    sch_client.printme("Setting null Residency for " + params['id'])

                params['RESIDENT_COMMUTER'] = 'C'

                # check that we aren't overriding existing alternate status
                if row and row.RESIDENT_COMMUTER and row.RESIDENT_COMMUTER != 'R':
                    params['RESIDENT_COMMUTER'] = row.RESIDENT_COMMUTER

                query, query_params = sch_client.prepare_query(residency_update, params)
                res_null_count += cursor.execute(query, *query_params).rowcount
            else:
                if verbose:
                    sch_client.printme("Skip setting null Residency for " + params['id'])
                res_null_skipped_count += 1

        # Update Meal Plan if specified
        if 'push_mealplan' in config and config['push_mealplan']:
            if resident['meal_plan']:
                params.update(resident['meal_plan'])
            else:
                params['MEAL_PLAN'] = None
            if verbose:
                sch_client.printme("Updating meal plan for " + params['id'], ": ")
                sch_client.printme(json.dumps(resident['meal_plan']))
            params['FOOD_PLAN'] = resident['meal_plan']['FOOD_PLAN'] if resident['meal_plan'] else None
            query, query_params = sch_client.prepare_query(mealplan_update, params)
            meal_update_count += cursor.execute(query, *query_params).rowcount

    connection.commit()
    sch_client.printme("Residency updates: " + str(res_update_count + res_null_count), " ")
    sch_client.printme("(" + str(res_update_count) + " placed, " + str(res_null_count) + " unplaced, " + str(res_null_skipped_count) + " skipped)")
    sch_client.printme("Meal Plan updates: " + str(meal_update_count))
    sch_client.printme("Record(s) not found: " + str(len(residents) - res_update_count - res_null_count))

connection.close()
sch_client.printme('------ End sync_push_powercampus ------')
