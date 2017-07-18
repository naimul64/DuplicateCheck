drop table if exists bugs.`schoolRowCountReceiveddata_intermediate`;

create table bugs.`schoolRowCountReceiveddata_intermediate`
SELECT 
    t1.school_code, t1.rowCount, t1.distinctMobCount, t2.RCount
FROM
    (SELECT 
        School_code,
            COUNT(*) rowCount,
            COUNT(DISTINCT MobileNo) distinctMobCount
    FROM
        pesp_db_p23_live.receiveddata
    GROUP BY School_code) t1
        LEFT JOIN
    (SELECT 
        school_code, COUNT(DISTINCT MobileNo) RCount
    FROM
        pesp_db_p23_live.receiveddata
    WHERE
        Remark = 'R'
        group by school_code) t2
        on t1.school_code = t2.school_code
;

ALTER TABLE `bugs`.`schoolRowCountReceiveddata_intermediate` 
ADD INDEX `school_code` (`School_code` ASC);





drop table if exists bugs.`schoolRowCountDsSheet_intermediate`;

create table bugs.`schoolRowCountDsSheet_intermediate`
Select school_code , count(*) rowCount, count(distinct mobile_no) distinctMobCount
from pesp_db_p23_live.disbursement_sheet_raw_data_report
group by school_code;

ALTER TABLE `bugs`.`schoolRowCountDsSheet_intermediate` 
ADD INDEX `school_code` (`School_code` ASC);




drop table if exists bugs.`schoolRowCountNewschoolRecord_intermediate`;

create table bugs.`schoolRowCountNewschoolRecord_intermediate`
Select school_code , count(*) rowCount, count(distinct MobileNo) distinctMobCount
from pesp_db_p23_live.newschoolrecord
group by school_code;

ALTER TABLE `bugs`.`schoolRowCountNewschoolRecord_intermediate` 
ADD INDEX `school_code` (`School_code` ASC);