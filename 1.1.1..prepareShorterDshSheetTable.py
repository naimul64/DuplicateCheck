#!/usr/bin/python

import MySQLdb
import sys

# Open database connection
db = MySQLdb.connect("192.168.168.18", "naimul", "naimul@#678", "bugs")

# prepare a cursor object using cursor() method
cursor = db.cursor()

cursor.execute(" drop table if exists bugs.dssheetSchoolCodeMobNo ")
db.commit()
print "Table Dropped"


cursor.execute("""
    create table bugs.dssheetSchoolCodeMobNo
    select school_code, mobile_no
    from pesp_db_p23_live.disbursement_sheet_raw_data_report
    limit 1
""")
db.commit()
cursor.execute(" TRUNCATE bugs.dssheetSchoolCodeMobNo ")
db.commit()
print "Table Created"

schoolCodeCollectQuery = "Select distinct school_code from pesp_db_p23_live.disbursement_sheet_raw_data_report"
cursor.execute(schoolCodeCollectQuery)
result = cursor.fetchall()
print "School code fetched."

total = len(result)
count = 0
for row in result:
    count = count + 1
    if not row[0] or row[0].strip() == '':
        continue

    cursor.execute("""
        Insert INTO bugs.dssheetSchoolCodeMobNo
        select school_code, mobile_no
        from pesp_db_p23_live.disbursement_sheet_raw_data_report
        where school_code = '"""+ row[0].strip() +"""'
        group by school_code, mobile_no
    """)
    
    if count % 1000 == 0:
        db.commit()
        sys.stdout.write("\rDone " + str(count) + " of " + str(total) + " schools.") 
        sys.stdout.flush()
db.commit()
sys.stdout.write("\rDone " + str(count) + " of " + str(total) + " schools.") 
sys.stdout.flush()


cursor.execute("""
    ALTER TABLE `bugs`.`dssheetSchoolCodeMobNo`
    ADD INDEX `schoolCode` (`school_code` ASC),
    ADD INDEX `mobileNo` (`mobile_no` ASC)
""")
db.commit()
print "\nIndexes added on school code and mobile no."

print "\n<--------------- Finished --------------->"