-- Table of same mobileNo count between DisbursementSheetRawData own
SELECT 
    th1.id tid1,
    CONCAT(th1.name, ',', dt1.name) thana1,
    sm.first_school_code schoolCode1,
    sm.second_school_code schoolCode2,
    th2.id tid2,
    CONCAT(th2.name, ',', dt2.name) thana1,
    first_school_row_count dsSheet1,
    mng1.row_count mongo1,
    second_school_row_count dsSheet2,
    mng2.row_count mongo2,
    itd1.distinctMobCount distMob1,
    itd2.distinctMobCount distMob2,
    same_mobile_no_count sameCnt,
    rdi1.rowCount received1,
    rdi2.rowCount received2,
    rdi1.RCount Rcount1,
    rdi2.RCount Rcount2,
    "Not started" Analysis,
    100*same_mobile_no_count/itd1.distinctMobCount pct1,
    100*same_mobile_no_count/itd2.distinctMobCount pct1,
    (Case when th1.id = th2.id then "TRUE" else "FALSE" END) smaeThana,
    '' User,
    '' Action,
    '' ActionStatus
    
FROM
    bugs.sameMobieNoCountBetweenDssheetAndDsheet sm
        INNER JOIN
    pesp_db_p23_live.school sc1 ON sm.first_school_code = sc1.school_code
        INNER JOIN
    pesp_db_p23_live.thana th1 ON sc1.thana_id = th1.id
        INNER JOIN
    pesp_db_p23_live.district dt1 ON th1.district_id = dt1.id
        INNER JOIN
    bugs.schoolRowCountDsSheet_intermediate itd1 ON itd1.school_code = sm.first_school_code
        LEFT JOIN
    bugs.mongoSchoolRowCount mng1 ON sm.first_school_code = mng1.school_code
        LEFT JOIN
    bugs.schoolRowCountReceiveddata_intermediate rdi1 ON sm.first_school_code = rdi1.school_code
        INNER JOIN
    pesp_db_p23_live.school sc2 ON sc2.school_code = sm.second_school_code
        INNER JOIN
    pesp_db_p23_live.thana th2 ON sc2.thana_id = th2.id
        INNER JOIN
    pesp_db_p23_live.district dt2 ON th2.district_id = dt2.id
        INNER JOIN
    bugs.schoolRowCountDsSheet_intermediate itd2 ON itd2.school_code = sm.second_school_code
        LEFT JOIN
    bugs.mongoSchoolRowCount mng2 ON sm.second_school_code = mng2.school_code
        LEFT JOIN
    bugs.schoolRowCountReceiveddata_intermediate rdi2 ON sm.second_school_code = rdi2.school_code
        LEFT JOIN
    pesp_db_p23_live.duplicate_school dps
    on ((dps.schoolCode1 = sm.first_school_code and dps.schoolCode2 = sm.second_school_code ) or
    (dps.schoolCode2 = sm.first_school_code and dps.schoolCode1 = sm.second_school_code ))
WHERE
    1 AND sc1.thana_id <= 999
        AND sc2.thana_id <= 999
        AND (100 * same_mobile_no_count / itd1.distinctMobCount >= 20
        OR 100 * same_mobile_no_count / itd2.distinctMobCount >=20)
        And same_mobile_no_count >=15
;