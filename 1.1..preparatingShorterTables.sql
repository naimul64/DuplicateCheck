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