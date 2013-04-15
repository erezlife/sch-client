-- Alternate PowerCampus sync script
-- Used to pull additional custom fields for Franklin College

SELECT  p.PEOPLE_ID,
    pu.username + '@franklincollege.edu' as email,
    p.FIRST_NAME,
    p.MIDDLE_NAME,
    p.LAST_NAME,
    p.BIRTH_DATE,
    phone.PhoneNumber,
    r.mail_slot,
    CASE WHEN a.Population = 'T' THEN 'T' ELSE (
      CASE WHEN a.class_level = 1 THEN 'N'
      ELSE 'R' END
    ) END as ResidentType,
    a.Class_level,
    coalesce(a.GENDER, 'F') as Gender,
    addr.ADDRESS_LINE_1,
    addr.ADDRESS_LINE_2,
    addr.CITY,
    addr.STATE,
    addr.ZIP_CODE,
    cc.LONG_DESC,
    a.EligibleForHousing,
    a.credits as enrolledCredits,
    COALESCE(gpa.total_credits, 0) as total_credits,
    gpa.gpa,
    CASE WHEN sl.PEOPLE_CODE_ID IS NOT NULL THEN 1 ELSE 0 END as hold
FROM  [dbo].[PersonUser] pu
INNER JOIN [dbo].[PEOPLE] p
ON pu.PersonId = p.PersonId
INNER JOIN (
  SELECT DISTINCT
      a.PEOPLE_CODE_ID,
      a.FULL_PART,
      a.ENROLL_SEPARATION,
      a.POPULATION,
      a.CLASS_LEVEL,
      a.credits,
      d.GENDER,
      CASE WHEN
      ((a.[APPLICATION_FLAG] = 'Y' AND a.[APP_STATUS] = '8' AND a.[FULL_PART] = 'F') -- Incoming students
      OR
      (a.[APPLICATION_FLAG] <> 'Y' AND a.[FULL_PART] = 'F') -- Returning students
      ) /*AND a.[ENROLL_SEPARATION] = 'Z' */ THEN 1 ELSE 0 END as EligibleForHousing
  FROM  Academic a
  INNER JOIN DEMOGRAPHICS d  -- Gender shouldn't be null...but apparently sometimes demographics are set wrong (eg no year, term, session)
  ON d.PEOPLE_CODE_ID = a.PEOPLE_CODE_ID
  --AND d.ACADEMIC_SESSION = a.ACADEMIC_SESSION
  --AND d.ACADEMIC_YEAR = a.ACADEMIC_YEAR
  --AND d.ACADEMIC_TERM = a.ACADEMIC_TERM
  WHERE a.status = 'A'
  --AND a.[POPULATION] IN ('R', 'T')
  AND a.[POPULATION] NOT IN ('H')
  AND a.ACADEMIC_SESSION = ' '
  AND a.ACADEMIC_YEAR = $%$ACADEMIC_YEAR$%$
  AND a.ACADEMIC_TERM = $%$ACADEMIC_TERM$%$
) AS a ON a.PEOPLE_CODE_ID = p.PEOPLE_CODE_ID
INNER JOIN (
  SELECT DISTINCT
    PEOPLE_CODE_ID,
    MAIL_SLOT
  FROM Residency
  WHERE ACADEMIC_TERM = $%$ACADEMIC_TERM$%$
  AND ACADEMIC_YEAR = $%$ACADEMIC_YEAR$%$
  AND ACADEMIC_SESSION = ' '
) AS r ON r.people_code_id = p.people_code_id
INNER JOIN Address addr
  ON addr.people_org_code_id = p.people_code_id
  AND addr.ADDRESS_TYPE = '00'
LEFT JOIN vwuStoplists sl
  ON sl.PEOPLE_CODE_ID = p.PEOPLE_CODE_ID
LEFT JOIN CODE_COUNTRY cc
  ON cc.CODE_VALUE = addr.COUNTRY
LEFT JOIN PersonPhone phone
  ON phone.PersonPhoneId = p.PrimaryPhoneId
LEFT JOIN vwuArgosStudentGPAOverall gpa
  ON gpa.people_code_id = p.people_code_id
