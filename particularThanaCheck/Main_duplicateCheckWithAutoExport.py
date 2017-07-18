#!/usr/bin/python

import MySQLdb
import os
import datetime
import csv
import sys
import pygame
import time

def getThanaIdListString():
    thanaList = open('thanaList.txt', 'r').readlines()

    thanaIdListString = ''
    for thanaId in thanaList:
        thanaId = thanaId.strip()
        thanaIdListString = thanaIdListString + thanaId + ','
    thanaIdListString = thanaIdListString[:-1]
    return thanaIdListString


def getSchoolCodeListString():
    schoolCodeList = open('schoolCodeList.txt', 'r').readlines()

    schoolCodeListString = ''
    for schoolCode in schoolCodeList:
        schoolCode = schoolCode.strip()
        schoolCodeListString = schoolCodeListString + "'" + schoolCode + "'" + ','
    schoolCodeListString = schoolCodeListString[:-1]
    return schoolCodeListString


def processRcvddataSccodeMobTable(db, thanaIdListString, schoolCodeListString):
    cursor = db.cursor()
    cursor.execute("""
        DELETE FROM bugs.rcvddataSchoolCodeMobNo
    WHERE
        school_code IN ("""+ schoolCodeListString +""")
    """)
    db.commit()
    print "Deleted from  bugs.rcvddataSchoolCodeMobNo table of thana " + thanaIdListString

    cursor.execute("""
        INSERT INTO bugs.rcvddataSchoolCodeMobNo
        SELECT rd.school_code, rd.MobileNo from pesp_db_p23_live.receiveddata rd
        inner join pesp_db_p23_live.school sc
        on rd.school_code = sc.school_code
        where sc.thana_id in (""" + thanaIdListString + """)
        group by rd.school_code, rd.MobileNo
    """)
    db.commit()
    print "Re-inserted in  bugs.rcvddataSchoolCodeMobNo table of thana " + thanaIdListString


def processDssheetdataSccodeMobTable(db, thanaIdListString, schoolCodeListString):
    cursor = db.cursor()
    cursor.execute("""
      DELETE FROM bugs.dssheetSchoolCodeMobNo
        WHERE
            school_code IN ("""+ schoolCodeListString +""")
    """)
    db.commit()
    print "Deleted from bugs.dssheetSchoolCodeMobNo of thana " + thanaIdListString

    cursor.execute("""
        INSERT INTO bugs.dssheetSchoolCodeMobNo
        SELECT ds.school_code, ds.mobile_no from pesp_db_p23_live.disbursement_sheet_raw_data_report ds
        inner join pesp_db_p23_live.school sc
        on ds.school_code = sc.school_code
        where sc.thana_id in (""" + thanaIdListString + """)
        group by ds.school_code, ds.mobile_no
        ;

    """)
    db.commit()
    print "Re-inserted in bugs.dssheetSchoolCodeMobNo of thana " + thanaIdListString


def processRevddataCountTable(db, thanaIdListString, schoolCodeListString):
    cursor = db.cursor()
    cursor.execute("""
        DELETE FROM bugs.schoolRowCountReceiveddata_intermediate
        WHERE
            school_code IN ("""+schoolCodeListString+""")
    """)
    db.commit()
    print "Deleted from bugs.schoolRowCountReceiveddata_intermediate for thana " + thanaIdListString

    cursor.execute("""
        INSERT INTO bugs.`schoolRowCountReceiveddata_intermediate`
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
            INNER JOIN
            pesp_db_p23_live.school sc
            on t1.school_code = sc.school_code
                LEFT JOIN
            (SELECT
                school_code, COUNT(DISTINCT MobileNo) RCount
            FROM
                pesp_db_p23_live.receiveddata
            WHERE
                Remark = 'R'
                group by school_code) t2
                on t1.school_code = t2.school_code
            where sc.thana_id in (""" + thanaIdListString + """)
    """)
    db.commit()
    print "Re-inserted in bugs.schoolRowCountReceiveddata_intermediate for thana " + thanaIdListString


def processDssheetdataCountTable(db, thanaIdListString, schoolCodeListString):
    cursor = db.cursor()
    cursor.execute("""
    DELETE FROM bugs.schoolRowCountDsSheet_intermediate
    WHERE
        school_code IN ("""+ schoolCodeListString +""")
    """)
    db.commit()
    print "Deleted from bugs.schoolRowCountDsSheet_intermediate for thana " + thanaIdListString

    try:
        cursor.execute("""
        ALTER TABLE bugs.schoolRowCountDsSheet_intermediate
        DROP INDEX school_code
        """)
        db.commit()
    except Exception as e:
        print e

    cursor.execute("""INSERT INTO bugs.schoolRowCountDsSheet_intermediate
    Select school_code , count(*) rowCount, count(distinct mobile_no) distinctMobCount
    from pesp_db_p23_live.disbursement_sheet_raw_data_report
    where
    school_code IN (SELECT
            school_code
        FROM
            pesp_db_p23_live.school

        WHERE
            thana_id IN ("""+ thanaIdListString +"""))
    group by school_code
    """)
    db.commit()

    cursor.execute("""
    ALTER TABLE bugs.schoolRowCountDsSheet_intermediate
    ADD INDEX `school_code` (school_code ASC);
    """)
    db.commit()


    print "Re-inserted in bugs.schoolRowCountDsSheet_intermediate for thana " + thanaIdListString


def insertSchoolCodeToTxtFile(db, thanaIdListString):
    cursor = db.cursor()
    cursor.execute("""
        SELECT school_code FROM
        pesp_db_p23_live.school
        where thana_id in (""" + thanaIdListString + """)
    """)
    schoolCodes = cursor.fetchall()
    f = open('schoolCodeList.txt','w')
    for schoolCode in schoolCodes:
        if schoolCode is not None and schoolCode[0] is not None:
            f.write(schoolCode[0].strip() + '\n')
    f.close()
    cursor.close()
    print "School codes fetched."

def saveResultInCSV(db):
    cursor = db.cursor()
    cursor.execute("""
            SELECT
                th1.id tid1,
                CONCAT(th1.name, ',', dt1.name) thana1,
                sc1.school_name,
                concat(sm.first_school_code,'(',sc1.processing_status,')') schoolCode1,
                concat(sm.second_school_code ,'(',sc2.processing_status,')') schoolCode2,
                sc2.school_name,
                th2.id tid2,
                CONCAT(th2.name, ',', dt2.name) thana2,
                same_mobile_no_count sameCnt,
                first_school_row_count dsSheet1,
                ifnull(mng1.row_count,0) mongo1,
                second_school_row_count dsSheet2,
                ifnull(mng2.row_count,0) mongo2,
                itd1.distinctMobCount distMob1,
                itd2.distinctMobCount distMob2,
                ifnull(rdi1.rowCount,0) received1,
                ifnull(rdi2.rowCount,0) received2,
                ifnull(rdi1.RCount,0) Rcount1,
                ifnull(rdi2.RCount,0) Rcount2,
                100*same_mobile_no_count/itd1.distinctMobCount pct1,
                100*same_mobile_no_count/itd2.distinctMobCount pct1,
                (Case when th1.id = th2.id then "TRUE" else "FALSE" END) smaeThana

            FROM
                bugs.sameMobieNoCountBetweenDssheetAndDsheetSmaller sm
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
                    And same_mobile_no_count >=15
                    
                    order by same_mobile_no_count desc
    """)
    rows = cursor.fetchall()
    description = cursor.description
    header = ()
    for desc in description:
        header= header+(desc[0],)
    cursor.close()
    now = datetime.datetime.now().strftime("%y%m%d%H%M")
    if not os.path.exists('export'):
        os.makedirs('export')
    #fp = open('export/duplicateList' + str(now) + '.csv', 'w')
    fp = open(os.path.join('export','duplicateList' + str(now) + '.csv'), 'wb')
    myFile = csv.writer(fp)
    myFile.writerow(header)
    myFile.writerows(rows)
    fp.close()

    print "\nExported to CSV."

def playNotificationSount():
    pygame.init()
    pygame.mixer.music.load("notification.mp3")
    pygame.mixer.music.play()
    time.sleep(10)


# Open database connection
db = MySQLdb.connect("192.168.168.18", "naimul", "naimul@#678", "bugs")

thanaIdListString = getThanaIdListString()
insertSchoolCodeToTxtFile(db, thanaIdListString)
schoolCodeListString = getSchoolCodeListString()
processRcvddataSccodeMobTable(db, thanaIdListString, schoolCodeListString)
processDssheetdataSccodeMobTable(db, thanaIdListString,schoolCodeListString)
processRevddataCountTable(db, thanaIdListString, schoolCodeListString)
processDssheetdataCountTable(db, thanaIdListString, schoolCodeListString)

os.system("python DsheetAndDsheet_smaller_set_of_school.py")
saveResultInCSV(db)


#playNotificationSount()