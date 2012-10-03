#!/usr/bin/python

import sch_client
import json
import pyodbc
import os
from collections import defaultdict
from copy import copy

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
config = json.load(open(os.path.join(__location__, 'config.json')))
api = sch_client.API(config['uri'], config['key'], config['secret'])
connection = pyodbc.connect(config['db_connection'])
cursor = connection.cursor()
verbose = False

room_assign_update = """
UPDATE ROOM_ASSIGN
SET ID_NUM = $%$id$%$,
    ASSIGN_DTE = $%$assign_time$%$,
    JOB_TIME = GETDATE(),
    JOB_NAME = 'sch.import_residency',
    USER_NAME = 'SCH',
    ROOM_ASSIGN_STS = 'A'
WHERE SESS_CDE = $%$SESS_CDE$%$
AND BLDG_LOC_CDE = $%$BLDG_LOC_CDE$%$
AND BLDG_CDE = $%$BLDG_CDE$%$
AND ROOM_CDE = $%$ROOM_CDE$%$
AND ROOM_SLOT_NUM = $%$slot$%$"""

room_assign_clear = """
UPDATE ROOM_ASSIGN
SET ID_NUM = NULL,
    ASSIGN_DTE = NULL,
    JOB_TIME = GETDATE(),
    JOB_NAME = 'sch.import_residency',
    USER_NAME = 'SCH',
    ROOM_ASSIGN_STS = 'U'
WHERE SESS_CDE = $%$SESS_CDE$%$
AND BLDG_LOC_CDE = $%$BLDG_LOC_CDE$%$
AND BLDG_CDE = $%$BLDG_CDE$%$
AND ROOM_CDE = $%$ROOM_CDE$%$"""

room_assign_select = """
SELECT  BLDG_LOC_CDE,
        BLDG_CDE,
        ROOM_CDE
FROM    ROOM_ASSIGN
WHERE SESS_CDE = $%$SESS_CDE$%$
AND ID_NUM = $%$id$%$
"""

stud_sess_assign_update = """
UPDATE STUD_SESS_ASSIGN
SET MEAL_PLAN = $%$MEAL_PLAN$%$,
    ROOM_ASSIGN_STS = $%$ROOM_ASSIGN_STS$%$,
    RESID_COMMUTER_STS = $%$RESID_COMMUTER_STS$%$,
    JOB_TIME = GETDATE(),
    JOB_NAME = 'sch.import_residency',
    USER_NAME = 'SCH'
WHERE SESS_CDE = $%$SESS_CDE$%$
AND ID_NUM = $%$id$%$"""

# update RESID_COMMUTER_STS meal_plan only
stud_sess_assign_update_meal = """
UPDATE STUD_SESS_ASSIGN
SET MEAL_PLAN = $%$MEAL_PLAN$%$,
    JOB_TIME = GETDATE(),
    JOB_NAME = 'sch.import_residency',
    USER_NAME = 'SCH'
WHERE SESS_CDE = $%$SESS_CDE$%$
AND ID_NUM = $%$id$%$"""

stud_sess_assign_select = """
SELECT ROOM_ASSIGN_STS, RESID_COMMUTER_STS
FROM STUD_SESS_ASSIGN
WHERE SESS_CDE = $%$SESS_CDE$%$
AND ID_NUM = $%$id$%$
"""

sess_room_master_update = """
UPDATE SESS_ROOM_MASTER
SET ROOM_CAPACITY = $%$capacity$%$,
    NUM_RESIDENTS = $%$num_residents$%$,
    NUM_VACANCIES = $%$num_vacancies$%$,
    ROOM_STS = $%$room_sts$%$,
    OCCUPANT_GENDER = $%$occupant_gender$%$,
    ROOM_TYPE = $%$ROOM_TYPE$%$,
    JOB_TIME = GETDATE(),
    JOB_NAME = 'sch.import_residency',
    USER_NAME = 'SCH'
WHERE SESS_CDE = $%$SESS_CDE$%$
AND BLDG_LOC_CDE = $%$BLDG_LOC_CDE$%$
AND BLDG_CDE = $%$BLDG_CDE$%$
AND ROOM_CDE = $%$ROOM_CDE$%$
"""

sess_bldg_master_update = """
UPDATE SESS_BLDG_MASTER
SET SESS_BLDG_CAP = $%$capacity$%$,
    NUM_RESIDENTS = $%$num_residents$%$,
    NUM_VACANCIES = $%$num_vacancies$%$,
    JOB_TIME = GETDATE(),
    JOB_NAME = 'sch.import_residency',
    USER_NAME = 'SCH'
WHERE SESS_CDE = $%$SESS_CDE$%$
AND BLDG_LOC_CDE = $%$BLDG_LOC_CDE$%$
AND BLDG_CDE = $%$BLDG_CDE$%$
"""

stud_roommates_delete = """
DELETE FROM STUD_ROOMMATES
WHERE SESS_CDE = %s
AND BLDG_LOC_CDE IN %s
AND BLDG_CDE IN %s
"""

stud_roommates_insert = """
INSERT INTO STUD_ROOMMATES (SESS_CDE, ID_NUM, REQ_ACTUAL_FLAG, ROOMMATE_ID, BLDG_LOC_CDE, BLDG_CDE, ROOM_CDE, USER_NAME, JOB_NAME, JOB_TIME)
VALUES ($%$SESS_CDE$%$, $%$id$%$, 'A', $%$roommate_id$%$, $%$BLDG_LOC_CDE$%$, $%$BLDG_CDE$%$, $%$ROOM_CDE$%$, 'SCH', 'sch.import_residency', GETDATE());
"""

room_assign_count = 0
stud_sess_assign_count = 0
sess_room_master_count = 0
sess_bldg_master_count = 0
res_null_count = 0
meal_update_count = 0
room_occupants = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
instances = api.get_instances()

for instance in instances:
    sch_client.printme("Processing instance", ' ')
    for key in instance:
        sch_client.printme(key + "=" + instance[key], ' ')
    sch_client.printme()

    # save room data to SESS_ROOM_MASTER
    rooms = api.get_rooms(instance)
    room_set = set()
    bldg_loc_set = set()
    bldg_set = set()

    halls = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    sch_client.printme("Total Rooms: " + str(len(rooms)))
    for room in rooms:
        # save hall/room information for future reference
        room_set.add((room['BLDG_LOC_CDE'], room['BLDG_CDE'], room['ROOM_CDE']))
        bldg_loc_set.add(room['BLDG_LOC_CDE'])
        bldg_set.add(room['BLDG_CDE'])

        halls[room['BLDG_LOC_CDE']][room['BLDG_CDE']]['capacity'] += room['capacity']
        halls[room['BLDG_LOC_CDE']][room['BLDG_CDE']]['num_residents'] += room['num_residents']

        params = copy(instance)
        params.update(room)
        params['ROOM_TYPE'] = params['ROOM_TYPE'] if 'ROOM_TYPE' in params else None
        params['occupant_gender'] = params['gender'] if params['gender'] else 'I'
        params['num_vacancies'] = room['capacity'] - room['num_residents']
        if room['num_residents'] == room['capacity']:
            params['room_sts'] = 'F'
        elif room['num_residents'] == 0:
            params['room_sts'] = 'V'
        else:
            params['room_sts'] = 'P'

        query, query_params = sch_client.prepare_query(sess_room_master_update, params)
        sess_room_master_count += cursor.execute(query, *query_params).rowcount

    # save hall data to SESS_BLDG_MASTER
    for bldg_loc_cde in halls:
        for bldg_cde in halls[bldg_loc_cde]:
            params = {
                'BLDG_LOC_CDE': bldg_loc_cde,
                'BLDG_CDE': bldg_cde
            }
            params.update(halls[bldg_loc_cde][bldg_cde])
            params.update(instance)
            params['num_vacancies'] = params['capacity'] - params['num_residents']
            query, query_params = sch_client.prepare_query(sess_bldg_master_update, params)
            sess_bldg_master_count += cursor.execute(query, *query_params).rowcount

    # clear all ROOM_ASSIGN data that exists in SCH
    for room_tuple in room_set:
        params = copy(instance)
        params['BLDG_LOC_CDE'] = room_tuple[0]
        params['BLDG_CDE'] = room_tuple[1]
        params['ROOM_CDE'] = room_tuple[2]
        query, query_params = sch_client.prepare_query(room_assign_clear, params)
        sess_bldg_master_count += cursor.execute(query, *query_params).rowcount

    # update ROOM_ASSIGN and STUD_SESS_ASSIGN data for all residents in SCH
    residents = api.get_residents(instance)
    sch_client.printme("Total Residents: " + str(len(residents)))
    for resident in residents:
        params = copy(instance)
        params['id'] = resident['id']

        if resident['meal_plan']:
            params.update(resident['meal_plan'])
        else:
            params['MEAL_PLAN'] = None

        if resident['residency']:
            bldg_loc_cde = resident['residency']['BLDG_LOC_CDE']
            bldg_cde = resident['residency']['BLDG_CDE']
            room_cde = resident['residency']['ROOM_CDE']
            room_occupants[bldg_loc_cde][bldg_cde][room_cde].append(resident['id'])

            # standard update
            if verbose:
                sch_client.printme("Updating ROOM_ASSIGN for " + params['id'], ": ")
                sch_client.printme(json.dumps(resident['residency']))
            params.update(resident['residency'])
            query, query_params = sch_client.prepare_query(room_assign_update, params)
            room_assign_count += cursor.execute(query, *query_params).rowcount

            if verbose:
                sch_client.printme("Updating STUD_SESS_ASSIGN for " + params['id'])
            params['ROOM_ASSIGN_STS'] = 'A'
            params['RESID_COMMUTER_STS'] = 'R'
            query, query_params = sch_client.prepare_query(stud_sess_assign_update, params)
            stud_sess_assign_count += cursor.execute(query, *query_params).rowcount
        else:
            # do not override students in halls/rooms we do not track
            if verbose:
                sch_client.printme("Setting null STUD_SESS_ASSIGN for " + params['id'])

            query, query_params = sch_client.prepare_query(stud_sess_assign_select, params)
            cursor.execute(query, *query_params)
            stud_row = cursor.fetchone()

            # set room_assign_sts to unset if resident previously marked as a resident, otherwise only set meal_plan
            if stud_row and stud_row.RESID_COMMUTER_STS == 'R':
                params['ROOM_ASSIGN_STS'] = 'U'
                params['RESID_COMMUTER_STS'] = None
                query, query_params = sch_client.prepare_query(stud_sess_assign_update, params)
            else:
                query, query_params = sch_client.prepare_query(stud_sess_assign_update_meal, params)
            res_null_count += cursor.execute(query, *query_params).rowcount

    # delete old roommates for bldg_loc and bldg codes we know
    params = copy(instance)
    bldg_loc_param = tuple("'" + x + "'" for x in bldg_loc_set)
    bldg_param = tuple("'" + x + "'" for x in bldg_set)
    query_string = stud_roommates_delete % (params['SESS_CDE'], bldg_loc_param, bldg_param)
    query, query_params = sch_client.prepare_query(query_string, params)
    cursor.execute(query, *query_params)

    # insert new roommates
    for bldg_loc_cde in room_occupants:
        for bldg_cde in room_occupants[bldg_loc_cde]:
            for room_cde in room_occupants[bldg_loc_cde][bldg_cde]:
                for res_id in room_occupants[bldg_loc_cde][bldg_cde][room_cde]:
                    for roommate_id in room_occupants[bldg_loc_cde][bldg_cde][room_cde]:
                        if res_id != roommate_id:
                            params['id'] = res_id
                            params['roommate_id'] = roommate_id
                            params['BLDG_LOC_CDE'] = bldg_loc_cde
                            params['BLDG_CDE'] = bldg_cde
                            params['ROOM_CDE'] = room_cde

                            query, query_params = sch_client.prepare_query(stud_roommates_insert, params)
                            cursor.execute(query, *query_params)

    connection.commit()
    sch_client.printme("ROOM_ASSIGN updates: " + str(room_assign_count + res_null_count), " ")
    sch_client.printme("(" + str(room_assign_count) + " placed, " + str(res_null_count) + " unplaced)")
#    sch_client.printme("Record(s) not found: " + str(len(residents) - res_update_count - res_null_count))

connection.close()
