#!/usr/bin/python

import sch_client
import json
import cx_Oracle
import os

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
    $%$ASSIGN_CODE$%$,
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
    SLBRMAP_HAPS_CODE = $%$ASSIGN_CODE$%$,
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
    $%$ASSIGN_DURATION$%$,
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
AND SLRRASG_ASCD_CODE <> $%$ASSIGN_INACTIVE_CODE$%$
AND SLRRASG_END_DATE = (
    SELECT MAX(SLRRASG_END_DATE)
    FROM SLRRASG
    WHERE SLRRASG_PIDM = $%$PIDM$%$
    AND SLRRASG_TERM_CODE = $%$TERM$%$
    AND SLRRASG_ASCD_CODE <> $%$ASSIGN_INACTIVE_CODE$%$
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
AND SLRRASG_ASCD_CODE <> $%$ASSIGN_INACTIVE_CODE$%$
AND SLRRASG_END_DATE = (
    SELECT MAX(SLRRASG_END_DATE)
    FROM SLRRASG
    WHERE SLRRASG_PIDM = $%$PIDM$%$
    AND SLRRASG_TERM_CODE = $%$TERM$%$
    AND SLRRASG_ASCD_CODE <> $%$ASSIGN_INACTIVE_CODE$%$
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
    SLRMASG_RRCD_CODE, -- rate code
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
    $%$RATE_CODE$%$,
    $%$start_date$%$,
    $%$end_date$%$,
    $%$ASSIGN_DURATION$%$,
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
AND SLRMASG_MSCD_CODE <> $%$ASSIGN_INACTIVE_CODE$%$
AND SLRMASG_END_DATE = (
    SELECT MAX(SLRMASG_END_DATE)
    FROM SLRMASG
    WHERE SLRMASG_PIDM = $%$PIDM$%$
    AND SLRMASG_TERM_CODE = $%$TERM$%$
    AND SLRMASG_MSCD_CODE <> $%$ASSIGN_INACTIVE_CODE$%$
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
AND SLRMASG_MSCD_CODE <> $%$ASSIGN_INACTIVE_CODE$%$
AND SLRMASG_END_DATE = (
    SELECT MAX(SLRMASG_END_DATE)
    FROM SLRMASG
    WHERE SLRMASG_PIDM = $%$PIDM$%$
    AND SLRMASG_TERM_CODE = $%$TERM$%$
    AND SLRMASG_MSCD_CODE <> $%$ASSIGN_INACTIVE_CODE$%$
)
"""

instances = api.get_instances()
for instance in instances:
    resident_missing = set()
    app_insert_count = 0
    app_update_count = 0
    app_correct_count = 0
    room_update_count = 0
    room_correct_count = 0
    meal_update_count = 0
    meal_correct_count = 0

    sch_client.printme("Processing instance", ' ')
    for key in instance['key']:
        sch_client.printme(key + "=" + instance['key'][key], ' ')
    sch_client.printme()

    residents = api.get_residents(instance['key'])
    sch_client.printme("Total Residents: " + str(len(residents)))
    for resident in residents:
        params = instance['key']

        params['id'] = resident['id']

        # check that this resident exists in Banner
        query, query_params = sch_client.prepare_query(resident_select, params, ':0')
        cursor.execute(query, query_params)
        resident_row = cursor.fetchone()
        if not resident_row:
            resident_missing.add(params['id'])
        else:
            pidm = resident_row[0]
            params['PIDM'] = pidm

            query, query_params = sch_client.prepare_query(application_select, params, ':0')
            cursor.execute(query, query_params)
            app_record = cursor.fetchone()

            print app_record

            exit()

    connection.commit()
    # sch_client.printme("Residency updates: " + str(res_update_count + res_null_count), " ")
    # sch_client.printme("(" + str(res_update_count) + " placed, " + str(res_null_count) + " unplaced, " + str(res_null_skipped_count) + " skipped)")
    # sch_client.printme("Meal Plan updates: " + str(meal_update_count))
    # sch_client.printme("Record(s) not found: " + str(len(residents) - res_update_count - res_null_count))

    if(len(resident_missing) > 0):
        sch_client.printme("Student records not found in SPRIDEN:")
        for record in resident_missing:
            sch_client.printme(" " + record)

connection.close()
sch_client.printme('------ End sync_push_banner ------')
