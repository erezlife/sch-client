#!/usr/bin/python

import sch_client
import json
import cx_Oracle
import os
from datetime import datetime
from copy import copy

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
sch_client.init_logging(__location__, 'sync_push_banner')
sch_client.printme('------ Begin sync_push_banner ------')
config = json.load(open(os.path.join(__location__, 'config.json')))
api = sch_client.API(config['uri'], config['key'], config['secret'])
connection = cx_Oracle.connect(config['db_connection'])
cursor = connection.cursor()
verbose = False

resident_select = """
SELECT * FROM SPRIDEN WHERE SPRIDEN_ID = $%$id$%$
"""

room_assignment_dates_select = """
SELECT * FROM SLRASCD WHERE SLRASCD_TERM_CODE = $%$TERM$%$ AND SLRASCD_ASCD_CODE = $%$ASSIGN_CODE$%$
"""

meal_assignment_dates_select = """
SELECT * FROM SLRMSCD WHERE SLRMSCD_TERM_CODE = $%$TERM$%$ AND SLRMSCD_MSCD_CODE = $%$ASSIGN_CODE$%$
"""

application_insert = """
INSERT INTO SLBRMAP (
    SLBRMAP_PIDM,
    SLBRMAP_ACTIVITY_DATE,
    SLBRMAP_ARTP_CODE, -- HOME, MEAL, HOUS
    SLBRMAP_FROM_TERM,
    SLBRMAP_TO_TERM,
    SLBRMAP_APPL_PRIORITY,
    SLBRMAP_PREFERED_BUILDING,
    SLBRMAP_PREFERED_ROOM,
    SLBRMAP_MRCD_CODE, -- meal rate code
    SLBRMAP_HAPS_CODE, -- WD, AC, IN, etc
    SLBRMAP_HAPS_DATE,
    SLBRMAP_ADD_DATE,
    SLBRMAP_DATA_ORIGIN, -- Simple Campus
    SLBRMAP_USER_ID -- SCH
)
VALUES (
    $%$PIDM$%$,
    SYSDATE,
    $%$ARTP_CODE$%$,
    $%$FROM_TERM$%$,
    $%$TO_TERM$%$,
    1,
    $%$BLDG_CODE$%$,
    $%$ROOM_NUMBER$%$,
    $%$MEAL_CODE$%$,
    $%$APP_CODE$%$,
    SYSDATE,
    SYSDATE,
    'Simple Campus',
    'SCH'
)
"""

application_select = """
SELECT a.*
FROM SLBRMAP a
INNER JOIN STVTERM fterm
    ON fterm.STVTERM_CODE = a.SLBRMAP_FROM_TERM
INNER JOIN STVTERM tterm
    ON tterm.STVTERM_CODE = a.SLBRMAP_TO_TERM
WHERE SLBRMAP_PIDM = $%$PIDM$%$
AND EXISTS (
    SELECT *
    FROM STVTERM t
    WHERE t.STVTERM_START_DATE >= fterm.STVTERM_START_DATE
    AND t.STVTERM_END_DATE <= tterm.STVTERM_END_DATE
    AND t.STVTERM_CODE = $%$TERM$%$
)
"""

# change the meal plan and housing assignment/preference of an application record
application_change_update = """
UPDATE SLBRMAP
SET SLBRMAP_ACTIVITY_DATE = SYSDATE,
    SLBRMAP_ARTP_CODE = $%$ARTP_CODE$%$, -- HOME, MEAL, HOUS
    SLBRMAP_MRCD_CODE = $%$MEAL_CODE$%$,
    SLBRMAP_PREFERED_BUILDING = $%$BLDG_CODE$%$,
    SLBRMAP_PREFERED_ROOM = $%$ROOM_NUMBER$%$,
    SLBRMAP_HAPS_DATE = SYSDATE,
    SLBRMAP_ADD_DATE = SYSDATE,
    SLBRMAP_DATA_ORIGIN = 'Simple Campus',
    SLBRMAP_USER_ID = 'SCH'
WHERE SLBRMAP_PIDM = $%$PIDM$%$
AND EXISTS (
    SELECT *
    FROM STVTERM t
    INNER JOIN STVTERM fterm
        ON t.STVTERM_START_DATE >= fterm.STVTERM_START_DATE
    INNER JOIN STVTERM tterm
        ON t.STVTERM_END_DATE <= tterm.STVTERM_END_DATE
    WHERE fterm.STVTERM_CODE = SLBRMAP.SLBRMAP_FROM_TERM
    AND tterm.STVTERM_CODE = SLBRMAP.SLBRMAP_TO_TERM
    AND t.STVTERM_CODE = $%$TERM$%$
)
"""

# change the status code (active, withdrawn, etc) of an application
application_status_update = """
UPDATE SLBRMAP
SET SLBRMAP_ACTIVITY_DATE = SYSDATE,
    SLBRMAP_HAPS_CODE = $%$APP_CODE$%$,
    SLBRMAP_HAPS_DATE = SYSDATE,
    SLBRMAP_DATA_ORIGIN = 'Simple Campus',
    SLBRMAP_USER_ID = 'SCH'
WHERE SLBRMAP_PIDM = $%$PIDM$%$
AND EXISTS (
    SELECT *
    FROM STVTERM t
    INNER JOIN STVTERM fterm
        ON t.STVTERM_START_DATE >= fterm.STVTERM_START_DATE
    INNER JOIN STVTERM tterm
        ON t.STVTERM_END_DATE <= tterm.STVTERM_END_DATE
    WHERE fterm.STVTERM_CODE = SLBRMAP.SLBRMAP_FROM_TERM
    AND tterm.STVTERM_CODE = SLBRMAP.SLBRMAP_TO_TERM
    AND t.STVTERM_CODE = $%$TERM$%$
)
"""

room_assignment_select = """
SELECT *
FROM SLRRASG
WHERE SLRRASG_PIDM = $%$PIDM$%$
AND SLRRASG_TERM_CODE = $%$TERM$%$
AND SLRRASG_ASCD_CODE = $%$ASSIGN_CODE$%$
AND SLRRASG_END_DATE = (
    SELECT MAX(SLRRASG_END_DATE)
    FROM SLRRASG
    WHERE SLRRASG_PIDM = $%$PIDM$%$
    AND SLRRASG_TERM_CODE = $%$TERM$%$
    AND SLRRASG_ASCD_CODE = $%$ASSIGN_CODE$%$
)
"""

room_assignment_insert = """
INSERT INTO SLRRASG (
    SLRRASG_PIDM,
    SLRRASG_BLDG_CODE,
    SLRRASG_ROOM_NUMBER,
    SLRRASG_TERM_CODE,
    SLRRASG_RRCD_CODE, -- rate code
    SLRRASG_BEGIN_DATE,
    SLRRASG_END_DATE,
    SLRRASG_TOTAL_DAYS,
    SLRRASG_TOTAL_MONTHS,
    SLRRASG_TOTAL_TERMS,
    SLRRASG_ASCD_CODE,
    SLRRASG_ASCD_DATE,
    SLRRASG_ONL_OR_BAT, -- B for batch
    SLRRASG_ACTIVITY_DATE,
    SLRRASG_AR_IND, -- N
    SLRRASG_ASSESS_NEEDED, -- N (assessment required)
    SLRRASG_DATA_ORIGIN,
    SLRRASG_USER_ID
)
VALUES (
    $%$PIDM$%$,
    $%$BLDG_CODE$%$,
    $%$ROOM_NUMBER$%$,
    $%$TERM$%$,
    $%$RATE_CODE$%$,
    $%$start_date$%$,
    $%$end_date$%$,
    $%$assign_duration$%$,
    0,
    0,
    $%$ASSIGN_CODE$%$,
    SYSDATE,
    'B',
    SYSDATE,
    'N',
    'N',
    'Simple Campus',
    'SCH'
)
"""

# update bldg/room of most recent room assignment for a student/term
room_assignment_update = """
UPDATE SLRRASG
SET SLRRASG_ASCD_CODE = $%$ASSIGN_CODE$%$,
    SLRRASG_BLDG_CODE = $%$BLDG_CODE$%$,
    SLRRASG_ROOM_NUMBER = $%$ROOM_NUMBER$%$,
    SLRRASG_RRCD_CODE = $%$RATE_CODE$%$,
    SLRRASG_ASCD_DATE = SYSDATE,
    SLRRASG_ACTIVITY_DATE = SYSDATE,
    SLRRASG_DATA_ORIGIN = 'Simple Campus',
    SLRRASG_USER_ID = 'SCH'
WHERE SLRRASG_PIDM = $%$PIDM$%$
AND SLRRASG_TERM_CODE = $%$TERM$%$
AND SLRRASG_ASCD_CODE = $%$ASSIGN_ACTIVE_CODE$%$
AND SLRRASG_END_DATE = (
    SELECT MAX(SLRRASG_END_DATE)
    FROM SLRRASG
    WHERE SLRRASG_PIDM = $%$PIDM$%$
    AND SLRRASG_TERM_CODE = $%$TERM$%$
    AND SLRRASG_ASCD_CODE = $%$ASSIGN_ACTIVE_CODE$%$
)
"""

# set status of most recent room assignment for a student/term
room_assignment_status_update = """
UPDATE SLRRASG
SET SLRRASG_ASCD_CODE = $%$ASSIGN_CODE$%$,
    SLRRASG_ASCD_DATE = SYSDATE,
    SLRRASG_ACTIVITY_DATE = SYSDATE,
    SLRRASG_DATA_ORIGIN = 'Simple Campus',
    SLRRASG_USER_ID = 'SCH'
WHERE SLRRASG_PIDM = $%$PIDM$%$
AND SLRRASG_TERM_CODE = $%$TERM$%$
AND SLRRASG_ASCD_CODE = $%$ASSIGN_ACTIVE_CODE$%$
AND SLRRASG_END_DATE = (
    SELECT MAX(SLRRASG_END_DATE)
    FROM SLRRASG
    WHERE SLRRASG_PIDM = $%$PIDM$%$
    AND SLRRASG_TERM_CODE = $%$TERM$%$
    AND SLRRASG_ASCD_CODE = $%$ASSIGN_ACTIVE_CODE$%$
)
"""

# change/withdraw the most recent room assignment for a student/term
room_assignment_change_update = """
UPDATE SLRRASG
SET SLRRASG_ASCD_CODE = $%$ASSIGN_CHANGE_CODE$%$,
    SLRRASG_END_DATE = SYSDATE - 1,
    SLRRASG_TOTAL_DAYS = SYSDATE - SLRRASG_BEGIN_DATE,
    SLRRASG_ASCD_DATE = SYSDATE,
    SLRRASG_ACTIVITY_DATE = SYSDATE,
    SLRRASG_DATA_ORIGIN = 'Simple Campus',
    SLRRASG_USER_ID = 'SCH'
WHERE SLRRASG_PIDM = $%$PIDM$%$
AND SLRRASG_TERM_CODE = $%$TERM$%$
AND SLRRASG_ASCD_CODE = $%$ASSIGN_ACTIVE_CODE$%$
AND SLRRASG_END_DATE = (
    SELECT MAX(SLRRASG_END_DATE)
    FROM SLRRASG
    WHERE SLRRASG_PIDM = $%$PIDM$%$
    AND SLRRASG_TERM_CODE = $%$TERM$%$
    AND SLRRASG_ASCD_CODE = $%$ASSIGN_ACTIVE_CODE$%$
)
"""

# -- meal plan ----------

meal_assignment_select = """
SELECT *
FROM SLRMASG
WHERE SLRMASG_PIDM = $%$PIDM$%$
AND SLRMASG_TERM_CODE = $%$TERM$%$
AND SLRMASG_MSCD_CODE = $%$ASSIGN_CODE$%$
AND SLRMASG_END_DATE = (
    SELECT MAX(SLRMASG_END_DATE)
    FROM SLRMASG
    WHERE SLRMASG_PIDM = $%$PIDM$%$
    AND SLRMASG_TERM_CODE = $%$TERM$%$
    AND SLRMASG_MSCD_CODE = $%$ASSIGN_CODE$%$
)
"""

meal_assignment_insert = """
INSERT INTO SLRMASG (
    SLRMASG_PIDM,
    SLRMASG_MRCD_CODE,
    SLRMASG_TERM_CODE,
    SLRMASG_BEGIN_DATE,
    SLRMASG_END_DATE,
    SLRMASG_TOTAL_DAYS,
    SLRMASG_TOTAL_MONTHS,
    SLRMASG_TOTAL_TERMS,
    SLRMASG_MSCD_CODE,
    SLRMASG_MSCD_DATE,
    SLRMASG_ONL_OR_BAT, -- B for batch
    SLRMASG_ACTIVITY_DATE,
    SLRMASG_AR_IND, -- N
    SLRMASG_ASSESS_NEEDED, -- N (assessment required)
    SLRMASG_DATA_ORIGIN,
    SLRMASG_USER_ID
)
VALUES (
    $%$PIDM$%$,
    $%$MEAL_CODE$%$,
    $%$TERM$%$,
    $%$start_date$%$,
    $%$end_date$%$,
    $%$assign_duration$%$,
    0,
    0,
    $%$ASSIGN_CODE$%$,
    SYSDATE,
    'B',
    SYSDATE,
    'N',
    'N',
    'Simple Campus',
    'SCH'
)
"""

# update meal plan of most recent meal assignment for a student/term
meal_assignment_update = """
UPDATE SLRMASG
SET SLRMASG_MSCD_CODE = $%$ASSIGN_CODE$%$,
    SLRMASG_MRCD_CODE = $%$MEAL_CODE$%$,
    SLRMASG_MSCD_DATE = SYSDATE,
    SLRMASG_ACTIVITY_DATE = SYSDATE,
    SLRMASG_DATA_ORIGIN = 'Simple Campus',
    SLRMASG_USER_ID = 'SCH'
WHERE SLRMASG_PIDM = $%$PIDM$%$
AND SLRMASG_TERM_CODE = $%$TERM$%$
AND SLRMASG_MSCD_CODE = $%$ASSIGN_ACTIVE_CODE$%$
AND SLRMASG_END_DATE = (
    SELECT MAX(SLRMASG_END_DATE)
    FROM SLRMASG
    WHERE SLRMASG_PIDM = $%$PIDM$%$
    AND SLRMASG_TERM_CODE = $%$TERM$%$
    AND SLRMASG_MSCD_CODE = $%$ASSIGN_ACTIVE_CODE$%$
)
"""

# set status of most recent meal assignment for a student/term
meal_assignment_status_update = """
UPDATE SLRMASG
SET SLRMASG_MSCD_CODE = $%$ASSIGN_CODE$%$,
    SLRMASG_MSCD_DATE = SYSDATE,
    SLRMASG_ACTIVITY_DATE = SYSDATE,
    SLRMASG_DATA_ORIGIN = 'Simple Campus',
    SLRMASG_USER_ID = 'SCH'
WHERE SLRMASG_PIDM = $%$PIDM$%$
AND SLRMASG_TERM_CODE = $%$TERM$%$
AND SLRMASG_MSCD_CODE = $%$ASSIGN_ACTIVE_CODE$%$
AND SLRMASG_END_DATE = (
    SELECT MAX(SLRMASG_END_DATE)
    FROM SLRMASG
    WHERE SLRMASG_PIDM = $%$PIDM$%$
    AND SLRMASG_TERM_CODE = $%$TERM$%$
    AND SLRMASG_MSCD_CODE = $%$ASSIGN_ACTIVE_CODE$%$
)
"""

# change/withdraw the most recent meal assignment for a student/term
meal_assignment_change_update = """
UPDATE SLRMASG
SET SLRMASG_MSCD_CODE = $%$ASSIGN_CHANGE_CODE$%$,
    SLRMASG_END_DATE = SYSDATE - 1,
    SLRMASG_TOTAL_DAYS = SYSDATE - SLRMASG_BEGIN_DATE,
    SLRMASG_MSCD_DATE = SYSDATE,
    SLRMASG_ACTIVITY_DATE = SYSDATE,
    SLRMASG_DATA_ORIGIN = 'Simple Campus',
    SLRMASG_USER_ID = 'SCH'
WHERE SLRMASG_PIDM = $%$PIDM$%$
AND SLRMASG_TERM_CODE = $%$TERM$%$
AND SLRMASG_MSCD_CODE = $%$ASSIGN_ACTIVE_CODE$%$
AND SLRMASG_END_DATE = (
    SELECT MAX(SLRMASG_END_DATE)
    FROM SLRMASG
    WHERE SLRMASG_PIDM = $%$PIDM$%$
    AND SLRMASG_TERM_CODE = $%$TERM$%$
    AND SLRMASG_MSCD_CODE = $%$ASSIGN_ACTIVE_CODE$%$
)
"""

instances = api.get_instances()
for instance in instances:
    resident_missing = set()
    app_insert_count = 0
    app_update_count = 0
    app_deactivate_count = 0
    app_correct_count = 0
    room_update_count = 0
    room_insert_count = 0
    room_correct_count = 0
    meal_update_count = 0
    meal_correct_count = 0
    meal_insert_count = 0

    sch_client.printme("Processing instance", ' ')
    for key in instance['key']:
        sch_client.printme(key + "=" + instance['key'][key], ' ')
    sch_client.printme()

    residents = api.get_residents(instance['key'])
    sch_client.printme("Total Residents: " + str(len(residents)))
    resident_number = 0

    instance_start_date = datetime.strptime(instance['start_date'], '%Y-%m-%d');
    instance_end_date = datetime.strptime(instance['end_date'], '%Y-%m-%d');
    sch_client.printme('Instance Dates: ', ' ')
    sch_client.printme(instance_start_date.strftime('%Y-%m-%d'),' - ')
    sch_client.printme(instance_end_date.strftime('%Y-%m-%d'))

    params = copy(instance['key'])

    # if ASSIGN_CODE not included in instance key, load default
    if 'ASSIGN_ACTIVE_CODE' not in params:
        params['ASSIGN_ACTIVE_CODE'] = config['banner']['ASSIGN_ACTIVE_CODE']

    if 'ASSIGN_INACTIVE_CODE' not in params:
        params['ASSIGN_INACTIVE_CODE'] = config['banner']['ASSIGN_INACTIVE_CODE']

    if 'ASSIGN_CHANGE_CODE' not in params:
        params['ASSIGN_CHANGE_CODE'] = config['banner']['ASSIGN_CHANGE_CODE']

    # if APP_CODE not included in instance key, load default
    if 'APP_ACTIVE_CODE' not in params:
        params['APP_ACTIVE_CODE'] = config['banner']['ASSIGN_ACTIVE_CODE']

    if 'APP_INACTIVE_CODE' not in params:
        params['APP_INACTIVE_CODE'] = config['banner']['APP_INACTIVE_CODE']

    if 'APP_CHANGE_CODE' not in params:
        params['APP_CHANGE_CODE'] = config['banner']['APP_CHANGE_CODE']

    # default ASSIGN_CODE and APP_CODE to 'active'
    params['ASSIGN_CODE'] = params['ASSIGN_ACTIVE_CODE']
    params['APP_CODE'] = params['APP_ACTIVE_CODE']

    # get room assignment and meal assignment start/end dates
    query, query_params = sch_client.prepare_query(room_assignment_dates_select, params, ':0')
    cursor.execute(query, query_params)
    dates_row = cursor.fetchone()
    if not dates_row:
        sch_client.printme("ERROR: No Room Assignment dates defined in Banner. Skipping Instance")
    else:
        room_start_date = dates_row[2]
        room_end_date = dates_row[3]
        sch_client.printme('Room Assignment Dates: ', ' ')
        sch_client.printme(room_start_date.strftime('%Y-%m-%d'),' - ')
        sch_client.printme(room_end_date.strftime('%Y-%m-%d'))

        query, query_params = sch_client.prepare_query(meal_assignment_dates_select, params, ':0')
        cursor.execute(query, query_params)
        dates_row = cursor.fetchone()
        meal_start_date = dates_row[2]
        meal_end_date = dates_row[3]
        sch_client.printme('Meal Assignment Dates: ', ' ')
        sch_client.printme(meal_start_date.strftime('%Y-%m-%d'),' - ')
        sch_client.printme(meal_end_date.strftime('%Y-%m-%d'))

        for resident in residents:
            resident_number += 1

            params['id'] = resident['id']

            # check that this resident exists in Banner
            query, query_params = sch_client.prepare_query(resident_select, params, ':0')
            cursor.execute(query, query_params)
            resident_row = cursor.fetchone()
            if not resident_row:
                resident_missing.add(params['id'])

                if verbose:
                    sch_client.printme("Skipping Resident " + str(resident_number) + ": " + params['id'])
            else:
                if verbose:
                    sch_client.printme("Processing Resident " + str(resident_number) + ": " + params['id'])

                pidm = resident_row[0]
                params['PIDM'] = pidm

                query, query_params = sch_client.prepare_query(application_select, params, ':0')
                cursor.execute(query, query_params)
                app_record = cursor.fetchone()

                query, query_params = sch_client.prepare_query(room_assignment_select, params, ':0')
                cursor.execute(query, query_params)
                room_assignment_record = cursor.fetchone()

                query, query_params = sch_client.prepare_query(meal_assignment_select, params, ':0')
                cursor.execute(query, query_params)
                meal_assignment_record = cursor.fetchone()

                # current assignments from banner
                banner_bldg_code = room_assignment_record[1] if room_assignment_record else None
                banner_room_number = room_assignment_record[2] if room_assignment_record else None
                banner_rate_code = room_assignment_record[4] if room_assignment_record else None
                banner_meal_code = meal_assignment_record[2] if meal_assignment_record else None

                # current assignments from sch
                bldg_code = resident['residency']['BLDG_CODE'] if resident['residency'] else None
                room_number = resident['residency']['ROOM_NUMBER'] if resident['residency'] else None
                rate_code = resident['residency']['RATE_CODE'] if resident['residency'] else None
                meal_code = resident['meal_plan']['MEAL_CODE'] if resident['meal_plan'] else None

                # determine if housing has changed
                housing_change = False
                if banner_bldg_code != bldg_code or banner_room_number != room_number or banner_rate_code != rate_code:
                    housing_change = True

                # determine if meal plan has changed
                meal_change = False
                if banner_meal_code != meal_code:
                    meal_change = True

                # set UPDATE query parameters with current assignments
                params['BLDG_CODE'] = bldg_code
                params['ROOM_NUMBER'] = room_number
                params['MEAL_CODE'] = meal_code

                # update application if either housing or meal assignment changed
                if housing_change or meal_change:

                    # set up datetime values for assignment insertion
                    # application (SLRRMAP) start/end dates are based on SCH instance
                    params['start_date'] = max(datetime.now(), instance_start_date)
                    params['end_date'] = instance_end_date
                    params['assign_duration'] = (params['end_date'] - params['start_date']).days + 1

                    # use standard logic designating application type (meal only, housing only, etc) if offcampus app present
                    if 'OFFCAMPUS_SUBMISSION_TIME' in resident and resident['OFFCAMPUS_SUBMISSION_TIME']:
                        if resident['residency'] and resident['meal_plan']:
                            params['ARTP_CODE'] = 'HOME'
                        elif resident['residency']:
                            params['ARTP_CODE'] = 'HOUS'
                        else:
                            params['ARTP_CODE'] = 'MEAL'
                    # if off campus not submitted/approved, always use "HOME" code
                    else:
                        params['ARTP_CODE'] = 'HOME'

                    # insert new application if there are changes and no application
                    if not app_record:
                        params['FROM_TERM'] = instance['key']['TERM']
                        if not instance['terminating_instance']:
                            msg = sch_client.printme('Terminating Instance not mapped for Instance ' + str(instance['key']['TERM']))
                            raise Exception(msg)
                        params['TO_TERM'] = instance['terminating_instance']['TERM']

                        query, query_params = sch_client.prepare_query(application_insert, params, ':0')
                        cursor.execute(query, query_params)
                        app_insert_count += cursor.rowcount

                    # update application if room or meal plan is assigned
                    elif room_number or meal_code:
                        query, query_params = sch_client.prepare_query(application_change_update, params, ':0')
                        cursor.execute(query, query_params)
                        app_update_count += cursor.rowcount

                    # deactivate/withdraw application if no room or meal plan is assigned
                    else:

                        # use inactive code if prior to instance start
                        if datetime.now() < instance_start_date:
                            params['APP_CODE'] = params['APP_INACTIVE_CODE']
                        else:
                            params['APP_CODE'] = params['APP_CHANGE_CODE']

                        query, query_params = sch_client.prepare_query(application_status_update, params, ':0')
                        cursor.execute(query, query_params)
                        app_deactivate_count += cursor.rowcount

                else:
                    app_correct_count += 1

                # deactivate and update housing assignment if needed
                if housing_change:

                    # set up datetime values for assignment insertion
                    # housing assignment (SLRRASG) start/end dates are based on SLRASCD
                    params['start_date'] = max(datetime.now(), room_start_date)
                    params['end_date'] = room_end_date
                    params['assign_duration'] = (params['end_date'] - params['start_date']).days + 1

                    # update old assignment if prior to term start
                    if room_assignment_record and datetime.now() < room_start_date:
                        # update bldg/room/rate
                        if resident['residency']:
                            params['ASSIGN_CODE'] = params['ASSIGN_ACTIVE_CODE']
                            params['RATE_CODE'] = resident['residency']['RATE_CODE']
                            query_statement = room_assignment_update
                        # deactivate assignment
                        else:
                            params['ASSIGN_CODE'] = params['ASSIGN_INACTIVE_CODE']
                            query_statement = room_assignment_status_update

                        query, query_params = sch_client.prepare_query(query_statement, params, ':0')
                        cursor.execute(query, query_params)
                        room_update_count += cursor.rowcount

                    # change/withdraw old assignment before inserting new assignment
                    elif room_assignment_record:
                        query, query_params = sch_client.prepare_query(room_assignment_change_update, params, ':0')
                        cursor.execute(query, query_params)
                        room_update_count += cursor.rowcount

                    # insert new assignment if none exists
                    # or term has already started (since old assignment has been deactivated)
                    if resident['residency'] and (
                        not room_assignment_record or datetime.now() >= room_start_date
                    ):
                        params['ASSIGN_CODE'] = params['ASSIGN_ACTIVE_CODE']
                        params['RATE_CODE'] = rate_code
                        query, query_params = sch_client.prepare_query(room_assignment_insert, params, ':0')
                        cursor.execute(query, query_params)
                        room_insert_count += cursor.rowcount


                # deactivate and update meal assignment if needed
                if meal_change:

                    # set up datetime values for assignment insertion
                    # meal assignment (SLRMASG) start/end dates are based on SLRMSCD
                    params['start_date'] = max(datetime.now(), meal_start_date)
                    params['end_date'] = meal_end_date
                    params['assign_duration'] = (params['end_date'] - params['start_date']).days + 1

                    # update old assignment if prior to term start
                    if meal_assignment_record and datetime.now() < meal_start_date:
                        # update meal rate code
                        if resident['meal_plan']:
                            params['ASSIGN_CODE'] = params['ASSIGN_ACTIVE_CODE']
                            params['MEAL_CODE'] = meal_code
                            query_statement = meal_assignment_update
                        # deactivate assignment
                        else:
                            params['ASSIGN_CODE'] = params['ASSIGN_INACTIVE_CODE']
                            query_statement = meal_assignment_status_update

                        query, query_params = sch_client.prepare_query(query_statement, params, ':0')
                        cursor.execute(query, query_params)
                        room_update_count += cursor.rowcount

                    # change/withdraw old assignment before inserting new assignment
                    elif meal_assignment_record:
                        query, query_params = sch_client.prepare_query(meal_assignment_change_update, params, ':0')
                        cursor.execute(query, query_params)
                        room_update_count += cursor.rowcount

                    # insert new assignment if none exists
                    # or term has already started (since old assignment has been deactivated)
                    if resident['meal_plan'] and (
                        not meal_assignment_record or datetime.now() >= meal_start_date
                    ):
                        params['ASSIGN_CODE'] = params['ASSIGN_ACTIVE_CODE']
                        params['MEAL_CODE'] = meal_code
                        query, query_params = sch_client.prepare_query(meal_assignment_insert, params, ':0')
                        cursor.execute(query, query_params)
                        meal_insert_count += cursor.rowcount

    connection.commit()
    sch_client.printme("Application Record [SLBRMAP]: ("
        + str(app_insert_count) + " insert(s) "
        + str(app_update_count) + " update(s) "
        + str(app_deactivate_count) + " deactivation(s) "
        + str(app_correct_count) + " unchanged)"
    )
    sch_client.printme("Room Assignment Record [SLRRASG]: ("
        + str(room_update_count) + " change(s) "
        + str(room_insert_count) + " insert(s) "
        + str(room_correct_count) + " unchanged)"
    )
    sch_client.printme("Meal Assignment Record [SLRMASG]: ("
        + str(meal_update_count) + " change(s) "
        + str(meal_insert_count) + " insert(s) "
        + str(meal_correct_count) + " unchanged)"
    )
    sch_client.printme("Student record(s) not found: " + str(len(resident_missing)))

    if(len(resident_missing) > 0):
        sch_client.printme("Student records not found in SPRIDEN:")
        for record in resident_missing:
            sch_client.printme(" " + record)

connection.close()
sch_client.printme('------ End sync_push_banner ------')
