#!/usr/bin/python

import sch_client
import json
import pyodbc
import os
from collections import defaultdict
from copy import copy

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
sch_client.init_logging(__location__, 'sync_push_jenzabar')
sch_client.printme('------ Begin sync_push_jenzabar ------')
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
    ROOM_ASSIGN_STS = 'A',
    ROOM_TYPE = $%$ROOM_TYPE$%$
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

room_assign_clear_resident = """
UPDATE ROOM_ASSIGN
SET ID_NUM = NULL,
    ASSIGN_DTE = NULL,
    JOB_TIME = GETDATE(),
    JOB_NAME = 'sch.import_residency',
    USER_NAME = 'SCH',
    ROOM_ASSIGN_STS = 'U'
WHERE SESS_CDE = $%$SESS_CDE$%$
AND ID_NUM = $%$id$%$"""

room_assign_insert = """
INSERT INTO ROOM_ASSIGN (
    SESS_CDE,
    BLDG_LOC_CDE,
    BLDG_CDE,
    ROOM_CDE,
    ROOM_SLOT_NUM,
    ROOM_TYPE,
    ID_NUM,
    ASSIGN_DTE,
    JOB_TIME,
    JOB_NAME,
    USER_NAME,
    ROOM_ASSIGN_STS
)
VALUES (
    $%$SESS_CDE$%$,
    $%$BLDG_LOC_CDE$%$,
    $%$BLDG_CDE$%$,
    $%$ROOM_CDE$%$,
    $%$slot$%$,
    $%$ROOM_TYPE$%$,
    $%$id$%$,
    $%$assign_time$%$,
    GETDATE(),
    'sch.import_residency',
    'SCH',
    'A'
)
"""

stud_sess_assign_update = """
UPDATE STUD_SESS_ASSIGN
SET ROOM_TYPE = $%$ROOM_TYPE$%$,
    ROOM_ASSIGN_STS = $%$ROOM_ASSIGN_STS$%$,
    RESID_COMMUTER_STS = $%$RESID_COMMUTER_STS$%$,
    JOB_TIME = GETDATE(),
    JOB_NAME = 'sch.import_residency',
    USER_NAME = 'SCH'
WHERE SESS_CDE = $%$SESS_CDE$%$
AND ID_NUM = $%$id$%$"""

# update STUD_SESS_ASSIGN meal_plan only
stud_sess_assign_update_meal = """
UPDATE STUD_SESS_ASSIGN
SET MEAL_PLAN = $%$MEAL_PLAN$%$,
    JOB_TIME = GETDATE(),
    JOB_NAME = 'sch.import_residency',
    USER_NAME = 'SCH'
WHERE SESS_CDE = $%$SESS_CDE$%$
AND ID_NUM = $%$id$%$"""

stud_sess_assign_insert = """
INSERT INTO STUD_SESS_ASSIGN (
    SESS_CDE,
    ID_NUM,
    ROOM_TYPE,
    ROOM_ASSIGN_STS,
    RESID_COMMUTER_STS,
    JOB_TIME,
    JOB_NAME,
    USER_NAME
)
VALUES (
    $%$SESS_CDE$%$,
    $%$id$%$,
    $%$ROOM_TYPE$%$,
    $%$ROOM_ASSIGN_STS$%$,
    $%$RESID_COMMUTER_STS$%$,
    GETDATE(),
    'sch.import_residency',
    'SCH'
)
"""

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

sess_room_master_insert = """
INSERT INTO SESS_ROOM_MASTER (
    SESS_CDE,
    BLDG_LOC_CDE,
    BLDG_CDE,
    ROOM_CDE,
    ROOM_CAPACITY,
    NUM_RESIDENTS,
    NUM_VACANCIES,
    ROOM_STS,
    OCCUPANT_GENDER,
    ROOM_TYPE,
    JOB_TIME,
    JOB_NAME,
    USER_NAME
)
VALUES (
    $%$SESS_CDE$%$,
    $%$BLDG_LOC_CDE$%$,
    $%$BLDG_CDE$%$,
    $%$ROOM_CDE$%$,
    $%$capacity$%$,
    $%$num_residents$%$,
    $%$num_vacancies$%$,
    $%$room_sts$%$,
    $%$occupant_gender$%$,
    $%$ROOM_TYPE$%$,
    GETDATE(),
    'sch.import_residency',
    'SCH'
)
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

sess_bldg_master_insert = """
INSERT INTO SESS_BLDG_MASTER (
    SESS_CDE,
    BLDG_LOC_CDE,
    BLDG_CDE,
    SESS_BLDG_CAP,
    NUM_RESIDENTS,
    NUM_VACANCIES,
    JOB_TIME,
    JOB_NAME,
    USER_NAME
)
VALUES (
    $%$SESS_CDE$%$,
    $%$BLDG_LOC_CDE$%$,
    $%$BLDG_CDE$%$,
    $%$capacity$%$,
    $%$num_residents$%$,
    $%$num_vacancies$%$,
    GETDATE(),
    'sch.import_residency',
    'SCH'
)
"""

sess_bldg_master_select = """
SELECT * FROM SESS_BLDG_MASTER WHERE SESS_CDE = $%$SESS_CDE$%$ AND BLDG_LOC_CDE = $%$BLDG_LOC_CDE$%$ AND BLDG_CDE = $%$BLDG_CDE$%$
"""

building_master_select = """
SELECT * FROM BUILDING_MASTER WHERE BLDG_CDE = $%$BLDG_CDE$%$
"""

name_master_select = """
SELECT * FROM NAME_MASTER WHERE ID_NUM = $%$id$%$
"""

room_master_select = """
SELECT * FROM ROOM_MASTER WHERE LOC_CDE = $%$BLDG_LOC_CDE$%$ AND BLDG_CDE = $%$BLDG_CDE$%$ AND ROOM_CDE = $%$ROOM_CDE$%$
"""

cm_session_mstr_select = """
SELECT * FROM CM_SESSION_MSTR WHERE SESS_CDE = $%$SESS_CDE$%$
"""

stud_roommates_delete = """
DELETE FROM STUD_ROOMMATES
WHERE SESS_CDE = '%s'
AND (
    BLDG_LOC_CDE IN (%s) AND BLDG_CDE IN (%s)
    OR ID_NUM IN (%s)
)
"""

stud_roommates_insert = """
INSERT INTO STUD_ROOMMATES (
    SESS_CDE,
    ID_NUM,
    REQ_ACTUAL_FLAG,
    ROOMMATE_ID,
    BLDG_LOC_CDE,
    BLDG_CDE,
    ROOM_CDE,
    USER_NAME,
    JOB_NAME,
    JOB_TIME
)
VALUES (
    $%$SESS_CDE$%$,
    $%$id$%$,
    'A',
    $%$roommate_id$%$,
    $%$BLDG_LOC_CDE$%$,
    $%$BLDG_CDE$%$,
    $%$ROOM_CDE$%$,
    'SCH',
    'sch.import_residency',
    GETDATE()
)
"""


def resident_exists(id):
    query, query_params = sch_client.prepare_query(name_master_select, {"id": id})
    rowcount = len(cursor.execute(query, *query_params).fetchall())
    return rowcount > 0

def instance_exists(sess_cde):
    query, query_params = sch_client.prepare_query(cm_session_mstr_select, {"SESS_CDE": sess_cde})
    rowcount = len(cursor.execute(query, *query_params).fetchall())
    return rowcount > 0

instances = api.get_instances()

for instance in instances:
    room_assign_count_update = 0
    room_assign_count_insert = 0
    stud_sess_assign_count_update = 0
    stud_sess_assign_count_insert = 0
    mealplan_count_update = 0
    sess_room_master_count_update = 0
    sess_room_master_count_insert = 0
    sess_bldg_master_count_update = 0
    sess_bldg_master_count_insert = 0
    building_master_missing = set()
    room_master_missing = set()
    resident_missing = set()
    res_null_count = 0
    room_occupants = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    sch_client.printme("Processing instance", ' ')
    for key in instance:
        sch_client.printme(key + "=" + instance[key], ' ')
    sch_client.printme()

    if not instance_exists(instance['SESS_CDE']):
        sch_client.printme('Skipping instance. Not found in Jenzabar')
    else:
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
            rowcount = cursor.execute(query, *query_params).rowcount
            if rowcount > 0:
                sess_room_master_count_update += rowcount
            else:
                query, query_params = sch_client.prepare_query(sess_bldg_master_select, params)
                rowcount = len(cursor.execute(query, *query_params).fetchall())
                if rowcount == 0:  # building not in the session
                    # check to make sure it exist in master lookup
                    query, query_params = sch_client.prepare_query(building_master_select, params)
                    rowcount = len(cursor.execute(query, *query_params).fetchall())
                    if rowcount == 0:  # building not in master lookup table
                        building_master_missing.add(params['BLDG_CDE'])
                    else:  # insert building into session
                        query, query_params = sch_client.prepare_query(sess_bldg_master_insert, params)
                        cursor.execute(query, *query_params)
                        sess_bldg_master_count_insert += 1

                if rowcount > 0:  # building found in session
                    query, query_params = sch_client.prepare_query(room_master_select, params)
                    rowcount = len(cursor.execute(query, *query_params).fetchall())
                    if rowcount == 0:  # room not in the session
                        room_master_missing.add((params['BLDG_LOC_CDE'], params['BLDG_CDE'], params['ROOM_CDE']))
                    else:  # insert room into session
                        query, query_params = sch_client.prepare_query(sess_room_master_insert, params)
                        cursor.execute(query, *query_params)
                        sess_room_master_count_insert += 1

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
                sess_bldg_master_count_update += cursor.execute(query, *query_params).rowcount

        # clear all ROOM_ASSIGN room data that exists in SCH
        for room_tuple in room_set:
            params = copy(instance)
            params['BLDG_LOC_CDE'] = room_tuple[0]
            params['BLDG_CDE'] = room_tuple[1]
            params['ROOM_CDE'] = room_tuple[2]
            query, query_params = sch_client.prepare_query(room_assign_clear, params)
            cursor.execute(query, *query_params)

        # update ROOM_ASSIGN and STUD_SESS_ASSIGN data for all residents in SCH
        residents = api.get_residents(instance)
        sch_client.printme("Total Residents: " + str(len(residents)))
        for resident in residents:
            params = copy(instance)
            params['id'] = resident['id']


            # get the stud_sess_assign record and/or insert if it doesn't exist
            query, query_params = sch_client.prepare_query(stud_sess_assign_select, params)
            cursor.execute(query, *query_params)
            stud_row = cursor.fetchone()

            params['ROOM_ASSIGN_STS'] = 'U'
            params['RESID_COMMUTER_STS'] = None
            params['ROOM_TYPE'] = None
            # insert stud_sess_assign record if not found
            if not stud_row:
                if resident_exists(params['id']):
                    query, query_params = sch_client.prepare_query(stud_sess_assign_insert, params)
                    cursor.execute(query, *query_params)
                    stud_sess_assign_count_insert += 1
                else:  # resident doesn't exist in master table
                    resident_missing.add(params['id'])


            # update resident room assignment
            if resident['residency'] and 'BLDG_LOC_CDE' in resident['residency']:
                residency = resident['residency']
                bldg_loc_cde = residency['BLDG_LOC_CDE'] if 'BLDG_LOC_CDE' in residency else None
                bldg_cde = residency['BLDG_CDE'] if 'BLDG_CDE' in residency else None
                room_cde = residency['ROOM_CDE'] if 'ROOM_CDE' in residency else None
                room_occupants[bldg_loc_cde][bldg_cde][room_cde].append(resident['id'])

                # clear ROOM_ASSIGN data for resident (in case they were in a room we don't track)
                query, query_params = sch_client.prepare_query(room_assign_clear_resident, params)
                cursor.execute(query, *query_params)

                # standard update
                if verbose:
                    sch_client.printme("Updating ROOM_ASSIGN for " + params['id'], ": ")
                    sch_client.printme(json.dumps(resident['residency']))

                params.update(resident['residency'])
                params['ROOM_TYPE'] = params['ROOM_TYPE'] if 'ROOM_TYPE' in params else None
                query, query_params = sch_client.prepare_query(room_assign_update, params)
                rowcount = cursor.execute(query, *query_params).rowcount
                if rowcount > 0:
                    room_assign_count_update += rowcount
                else:
                    query, query_params = sch_client.prepare_query(room_assign_insert, params)
                    cursor.execute(query, *query_params)
                    room_assign_count_insert += 1

            # update resident status in STUD_SESS_ASSIGN
            if resident['residency']:
                if verbose:
                    sch_client.printme("Updating STUD_SESS_ASSIGN for " + params['id'])
                params.update(resident['residency'])
                params['ROOM_ASSIGN_STS'] = 'A'
                params['RESID_COMMUTER_STS'] = 'R'
                query, query_params = sch_client.prepare_query(stud_sess_assign_update, params)
                rowcount = cursor.execute(query, *query_params).rowcount
                if rowcount > 0:
                    stud_sess_assign_count_update += rowcount
                else:
                    if resident_exists(params['id']):
                        query, query_params = sch_client.prepare_query(stud_sess_assign_insert, params)
                        cursor.execute(query, *query_params)
                        stud_sess_assign_count_insert += 1
                    else:  # resident doesn't exist in master table
                        resident_missing.add(params['id'])

            else:
                # do not override students in halls/rooms we do not track
                if verbose:
                    sch_client.printme("Setting null STUD_SESS_ASSIGN for " + params['id'])

                # set room_assign_sts to unset if resident previously marked as a resident
                rowcount = 0
                if stud_row and stud_row.RESID_COMMUTER_STS == 'R':
                    query, query_params = sch_client.prepare_query(stud_sess_assign_update, params)
                    rowcount = cursor.execute(query, *query_params).rowcount
                    stud_sess_assign_count_update += rowcount

                res_null_count += rowcount

            if 'push_mealplan' in config and config['push_mealplan']:
                if resident['meal_plan']:
                    params.update(resident['meal_plan'])
                else:
                    params['MEAL_PLAN'] = None
                query, query_params = sch_client.prepare_query(stud_sess_assign_update_meal, params)
                rowcount = cursor.execute(query, *query_params).rowcount
                mealplan_count_update += rowcount

        # delete old roommates for bldg_loc and bldg codes we know
        bldg_loc_cdes = ','.join(map(lambda w: "'" + w + "'", bldg_loc_set))
        bldg_cdes = ','.join(map(lambda w: "'" + w + "'", bldg_set))
        res_ids = ','.join(map(lambda r: r['id'], filter(lambda r: r['residency'], residents)))
        if res_ids and bldg_cdes and bldg_loc_cdes:
            query = stud_roommates_delete % (instance['SESS_CDE'], bldg_loc_cdes, bldg_cdes, res_ids)
            cursor.execute(query)

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
        sch_client.printme("ROOM_ASSIGN updates: " + str(room_assign_count_update + res_null_count), " ")
        sch_client.printme("(" + str(room_assign_count_update) + " placed, " + str(res_null_count) + " unplaced)")

        sch_client.printme("SESS_BLDG_MASTER updates: " + str(sess_bldg_master_count_update) + " new-inserts: " + str(sess_bldg_master_count_insert) + " missing: " + str(len(building_master_missing)))
        if(len(building_master_missing) > 0):
            sch_client.printme("BUILDING_MASTER records not found:")
            for record in building_master_missing:
                sch_client.printme(" " + record)
        sch_client.printme("SESS_ROOM_MASTER updates: " + str(sess_room_master_count_update) + " new-inserts: " + str(sess_room_master_count_insert) + " missing: " + str(len(room_master_missing)))

        if(len(room_master_missing) > 0):
            sch_client.printme("ROOM_MASTER records not found:")
            for record in room_master_missing:
                sch_client.printme(" " + record[0] + " " + record[1] + " " + record[2])

        sch_client.printme("ROOM_ASSIGN updates: " + str(room_assign_count_update) + " new-inserts: " + str(room_assign_count_insert))

        sch_client.printme("STUD_SESS_ASSIGN updates: " + str(stud_sess_assign_count_update) + " new-inserts: " + str(stud_sess_assign_count_insert) + " missing: " + str(len(resident_missing)))
        sch_client.printme("STUD_SESS_ASSIGN MEAL_PLAN updates: " + str(mealplan_count_update))
        if(len(resident_missing) > 0):
            sch_client.printme("NAME_MASTER records not found:")
            for record in resident_missing:
                sch_client.printme(" " + record)

connection.close()
sch_client.printme('------ End sync_push_jenzabar ------')
