-- drop table if exists bugs.dssheetSchoolCodeMobNo;

-- create table bugs.dssheetSchoolCodeMobNo
-- select school_code, mobile_no
-- from pesp_db_p23_live.disbursement_sheet_raw_data_report
-- ;

-- ALTER TABLE `bugs`.`dssheetSchoolCodeMobNo` 
-- ADD INDEX `schoolCode` (`school_code` ASC),
-- ADD INDEX `mobileNo` (`mobile_no` ASC);



drop table if exists bugs.rcvddataSchoolCodeMobNo;

create table bugs.rcvddataSchoolCodeMobNo
select school_code, MobileNo
from pesp_db_p23_live.receiveddata
group by school_code, MobileNo;

ALTER TABLE `bugs`.`rcvddataSchoolCodeMobNo` 
ADD INDEX `schoolCode` (`school_code` ASC),
ADD INDEX `mobileNo` (`MobileNo` ASC);



drop table if exists bugs.newschoolrecordSchoolCodeMobNo;

create table bugs.newschoolrecordSchoolCodeMobNo
select school_code, MobileNo
from pesp_db_p23_live.newschoolrecord
group by school_code, MobileNo;

ALTER TABLE `bugs`.`newschoolrecordSchoolCodeMobNo` 
ADD INDEX `schoolCode` (`school_code` ASC),
ADD INDEX `mobileNo` (`MobileNo` ASC);

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