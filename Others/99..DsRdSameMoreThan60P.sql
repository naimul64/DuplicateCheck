-- Choose your necessary sql query according to your table --

-- Table of same mobileNo count between disbursementSheetRawData and ReceivedData
SELECT 
    sm.ds_school_code,
    CONCAT(th1.name, ',', dt1.name) thana,
    sm.rd_school_code,
    CONCAT(th2.name, ',', dt2.name) thana,
    DisbursementSheetRowCount,
    ReceivedDataRowCount,
    same_mobile_no_count,
    itd1.distinctMobCount dsDistMobCnt,
    itd2.distinctMobCount rdDistMobCnt
FROM
    bugs.sameMobieNoCountBetweenDssheetRecvddata sm
        INNER JOIN
    pesp_db_p23_live.school sc1 ON sm.ds_school_code = sc1.school_code
        INNER JOIN
    pesp_db_p23_live.thana th1 ON sc1.thana_id = th1.id
        INNER JOIN
    pesp_db_p23_live.district dt1 ON th1.district_id = dt1.id
        INNER JOIN
    pesp_db_p23_live.school sc2 ON sc2.school_code = sm.rd_school_code
        INNER JOIN
    pesp_db_p23_live.thana th2 ON sc2.thana_id = th2.id
        INNER JOIN
    pesp_db_p23_live.district dt2 ON th2.district_id = dt2.id
        INNER JOIN
    bugs.schoolRowCountDsSheet_intermediate itd1 ON itd1.school_code = sm.ds_school_code
        INNER JOIN
    bugs.schoolRowCountReceiveddata_intermediate itd2 ON itd2.school_code = sm.rd_school_code
WHERE
    1
    AND sc1.thana_id <= 999
    AND sc2.thana_id <= 999
        AND (100 * same_mobile_no_count / itd1.distinctMobCount > 60
        OR 100 * same_mobile_no_count / itd2.distinctMobCount > 60)
    ;



-- Table of same mobileNo count between disbursementSheetRawData and NewSchoolRecord
SELECT 
    sm.ds_school_code,
    CONCAT(th1.name, ',', dt1.name) thana,
    sm.nsr_school_code,
    CONCAT(th2.name, ',', dt2.name) thana,
    DisbursementSheetRowCount,
    NewschoolrecordDataRowCount,
    same_mobile_no_count,
    itd1.distinctMobCount dsDistMobCnt,
    itd2.distinctMobCount nsrDistMobCnt
FROM
    bugs.sameMobieNoCountBetweenDssheetNewschoolrecord sm
        INNER JOIN
    pesp_db_p23_live.school sc1 ON sm.ds_school_code = sc1.school_code
        INNER JOIN
    pesp_db_p23_live.thana th1 ON sc1.thana_id = th1.id
        INNER JOIN
    pesp_db_p23_live.district dt1 ON th1.district_id = dt1.id
        INNER JOIN
    pesp_db_p23_live.school sc2 ON sc2.school_code = sm.nsr_school_code
        INNER JOIN
    pesp_db_p23_live.thana th2 ON sc2.thana_id = th2.id
        INNER JOIN
    pesp_db_p23_live.district dt2 ON th2.district_id = dt2.id
        INNER JOIN
    bugs.schoolRowCountDsSheet_intermediate itd1 ON itd1.school_code = sm.ds_school_code
        INNER JOIN
    bugs.schoolRowCountReceiveddata_intermediate itd2 ON itd2.school_code = sm.nsr_school_code
WHERE
    1
    AND sc1.thana_id <= 999
    AND sc2.thana_id <= 999
        AND (100 * same_mobile_no_count / itd1.distinctMobCount > 60
        OR 100 * same_mobile_no_count / itd2.distinctMobCount > 60)
    ;



-- Table of same mobileNo count between ReceivedData and NewSchoolRecord
SELECT 
    sm.rd_school_code,
    CONCAT(th1.name, ',', dt1.name) thana,
    sm.nsr_school_code,
    CONCAT(th2.name, ',', dt2.name) thana,
    ReceiveddataRowCount,
    NewschoolrecordRowCount,
    same_mobile_no_count,
    itd1.distinctMobCount rdDistMobCnt,
    itd2.distinctMobCount nsrDistMobCnt
FROM
    bugs.sameMobieNoCountBetweenNewschoolrecordRecvddata sm
        INNER JOIN
    pesp_db_p23_live.school sc1 ON sm.rd_school_code = sc1.school_code
        INNER JOIN
    pesp_db_p23_live.thana th1 ON sc1.thana_id = th1.id
        INNER JOIN
    pesp_db_p23_live.district dt1 ON th1.district_id = dt1.id
        INNER JOIN
    pesp_db_p23_live.school sc2 ON sc2.school_code = sm.nsr_school_code
        INNER JOIN
    pesp_db_p23_live.thana th2 ON sc2.thana_id = th2.id
        INNER JOIN
    pesp_db_p23_live.district dt2 ON th2.district_id = dt2.id
        INNER JOIN
    bugs.schoolRowCountDsSheet_intermediate itd1 ON itd1.school_code = sm.rd_school_code
        INNER JOIN
    bugs.schoolRowCountReceiveddata_intermediate itd2 ON itd2.school_code = sm.nsr_school_code
WHERE
    1
    AND sc1.thana_id <= 999
    AND sc2.thana_id <= 999
        AND (100 * same_mobile_no_count / itd1.distinctMobCount > 60
        OR 100 * same_mobile_no_count / itd2.distinctMobCount > 60);





-- Table of same mobileNo count between DisbursementSheetRawData own
SELECT 
    sm.first_school_code,
    CONCAT(th1.name, ',', dt1.name) thana,
    sm.second_school_code,
    CONCAT(th2.name, ',', dt2.name) thana,
    first_school_row_count,
    second_school_row_count,
    itd1.distinctMobCount first_school_dis_Mob_cnt,
    itd2.distinctMobCount second_school_dis_Mob_cnt,
    mng1.row_count first_school_mongo,
    mng2.row_count second_school_mongo,
    rdi1.rowCount first_receivd_count,
    rdi2.rowCount second_receivd_count,
    same_mobile_no_count
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
WHERE
    1 AND sc1.thana_id <= 999
        AND sc2.thana_id <= 999
        AND (100 * same_mobile_no_count / itd1.distinctMobCount >= 20
        OR 100 * same_mobile_no_count / itd2.distinctMobCount >=20)
;




-- Table of same mobileNo count between Newschoolrecord own
SELECT 
    sm.first_school_code,
    CONCAT(th1.name, ',', dt1.name) thana,
    sm.second_school_code,
    CONCAT(th2.name, ',', dt2.name) thana,
    first_school_row_count,
    second_school_row_count,
    same_mobile_no_count,
    itd1.distinctMobCount first_school_dis_Mob_cnt,
    itd2.distinctMobCount second_school_dis_Mob_cnt
FROM
    bugs.sameMobieNoCountBetweenNewschoolrecordAndNewschoolrecord sm
        INNER JOIN
    pesp_db_p23_live.school sc1 ON sm.first_school_code = sc1.school_code
        INNER JOIN
    pesp_db_p23_live.thana th1 ON sc1.thana_id = th1.id
        INNER JOIN
    pesp_db_p23_live.district dt1 ON th1.district_id = dt1.id
        INNER JOIN
    pesp_db_p23_live.school sc2 ON sc2.school_code = sm.second_school_code
        INNER JOIN
    pesp_db_p23_live.thana th2 ON sc2.thana_id = th2.id
        INNER JOIN
    pesp_db_p23_live.district dt2 ON th2.district_id = dt2.id
        INNER JOIN
    bugs.schoolRowCountDsSheet_intermediate itd1 ON itd1.school_code = sm.first_school_code
        INNER JOIN
    bugs.schoolRowCountDsSheet_intermediate itd2 ON itd2.school_code = sm.second_school_code
WHERE
    1
    AND sc1.thana_id <= 999
    AND sc2.thana_id <= 999
        AND (100 * same_mobile_no_count / itd1.distinctMobCount > 60
        OR 100 * same_mobile_no_count / itd2.distinctMobCount > 60);





-- Table of same mobileNo count between ReceivedData own
SELECT 
    sm.first_school_code,
    CONCAT(th1.name, ',', dt1.name) thana,
    sm.second_school_code,
    CONCAT(th2.name, ',', dt2.name) thana,
    first_school_row_count,
    second_school_row_count,
    same_mobile_no_count,
    itd1.distinctMobCount first_school_dis_Mob_cnt,
    itd2.distinctMobCount second_school_dis_Mob_cnt
FROM
    bugs.sameMobieNoCountBetweenReceivedAndReceived sm
        INNER JOIN
    pesp_db_p23_live.school sc1 ON sm.first_school_code = sc1.school_code
        INNER JOIN
    pesp_db_p23_live.thana th1 ON sc1.thana_id = th1.id
        INNER JOIN
    pesp_db_p23_live.district dt1 ON th1.district_id = dt1.id
        INNER JOIN
    pesp_db_p23_live.school sc2 ON sc2.school_code = sm.second_school_code
        INNER JOIN
    pesp_db_p23_live.thana th2 ON sc2.thana_id = th2.id
        INNER JOIN
    pesp_db_p23_live.district dt2 ON th2.district_id = dt2.id
        INNER JOIN
    bugs.schoolRowCountDsSheet_intermediate itd1 ON itd1.school_code = sm.first_school_code
        INNER JOIN
    bugs.schoolRowCountDsSheet_intermediate itd2 ON itd2.school_code = sm.second_school_code
WHERE
    1
    AND sc1.thana_id <= 999
    AND sc2.thana_id <= 999
        AND (100 * same_mobile_no_count / itd1.distinctMobCount > 60
        OR 100 * same_mobile_no_count / itd2.distinctMobCount > 60);