SELECT stsd.id_num,
        nm.first_name,
        nm.last_name,
        bm.gender,
        bm.birth_dte,
        addr.addr_line_1 as email,
        nm.mobile_phone,
        stsd.career_hrs_earned as TotalCredits,
        stsd.hrs_enrolled as EnrolledCredits,
        CASE WHEN c.stage IS NOT NULL AND c.stage <> 'DEP' THEN 'false' ELSE 'true' END as eligible,
        CASE WHEN c.id_num IS NOT NULL THEN 'New' ELSE 'Returning' END as residenttype,
        CASE WHEN ext.udef_1a_1 = 'A' THEN 'true' ELSE 'false' END as ApprovedException
FROM stud_term_sum_div stsd
INNER JOIN name_master nm
	ON nm.id_num = stsd.id_num
INNER JOIN biograph_master bm
	ON bm.id_num = stsd.id_num
INNER JOIN addr_all_email_addrs addr
	ON addr.id_num = stsd.id_num
LEFT JOIN candidacy c
	ON c.yr_cde = stsd.yr_cde
	AND c.trm_cde = stsd.trm_cde
	AND c.id_num = stsd.id_num
LEFT JOIN stud_sess_asgn_ext ext
    ON ext.id_num = stsd.id_num
    AND ext.sess_cde = $%$SESS_CDE$%$
WHERE stsd.yr_cde = $%$YR_CDE$%$
AND stsd.trm_cde = $%$TRM_CDE$%$