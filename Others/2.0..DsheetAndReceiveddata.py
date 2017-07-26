#!/usr/bin/python

import MySQLdb

# Open database connection
db = MySQLdb.connect("192.168.168.18", "naimul", "naimul@#678", "bugs")

# prepare a cursor object using cursor() method
cursor = db.cursor()

tableDropQuery = "DROP TABLE IF EXISTS bugs.sameMobieNoCountBetweenDssheetRecvddata"
cursor.execute(tableDropQuery)
db.commit()

tableCreateQuery = """
    CREATE TABLE bugs.sameMobieNoCountBetweenDssheetRecvddata (
    ds_school_code VARCHAR(13) NOT NULL,
    rd_school_code VARCHAR(13) NOT NULL,
    DisbursementSheetRowCount INT (5),
    ReceivedDataRowCount INT (5),
    same_mobile_no_count INT(5) NULL)
"""
cursor.execute(tableCreateQuery)
db.commit()

schoolCodeCollectQuery = "Select distinct school_code from bugs.rcvddataSchoolCodeMobNo"
cursor.execute(schoolCodeCollectQuery)
result = cursor.fetchall()
print "School fetched."

cursor.execute("select school_code, rowCount from bugs.schoolRowCountDsSheet_intermediate")
dsResult = cursor.fetchall()
dsCntMap = {}
for dsr in dsResult:
    dsCntMap[dsr[0]] = dsr[1]


cursor.execute("select school_code, rowCount from bugs.schoolRowCountReceiveddata_intermediate")
rdResult = cursor.fetchall()
rdCntMap = {}
for rdr in rdResult:
    rdCntMap[rdr[0]] = rdr[1]


count = 0
for row in result:
    thanaId = ''
    district = ''
    thana = ''
    schoolCode = row[0]
    countInDsData = 0
    countInReceivedData = 0
    count = count + 1
    print 'For school no: ' + str(count) + " of " + str(len(result)) + " schools, schoolCode: " + schoolCode + " ."
    if schoolCode.strip() == '':
        continue
    query = """
        SELECT
            ds1.school_code,
            ds2.school_code,
            COUNT(*) same_mobile_cnt
        FROM
            bugs.dssheetSchoolCodeMobNo ds1
                INNER JOIN
            bugs.rcvddataSchoolCodeMobNo ds2 ON ds1.mobile_no = ds2.MobileNo
        WHERE
        length(ds1.mobile_no) = 11 AND cast(ds1.mobile_no as unsigned) != 0
        AND substring(ds1.mobile_no, 1,3) in ('015','016','017','018','019')
        AND length(ds2.MobileNo) = 11 AND cast(ds2.MobileNo as unsigned) != 0
        AND substring(ds2.MobileNo, 1,3) in ('015','016','017','018','019')
            AND ds1.school_code != '""" + schoolCode + """'
            AND ds2.school_code = '""" + schoolCode + """'
                AND CAST(ds1.school_code AS UNSIGNED) <= CAST(ds2.school_code AS UNSIGNED)
        GROUP BY ds1.school_code , ds2.school_code
    """
    cursor.execute(query)
    resultNow = cursor.fetchall()

    for rownow in resultNow:
        try:
            dsCntMap[rownow[0]] + rdCntMap[rownow[1]]
        except KeyError:
            print "One of the School codes not in dssheet or receiveddata"
            continue

        queryNow = """
             INSERT INTO sameMobieNoCountBetweenDssheetRecvddata (
             ds_school_code,
            rd_school_code,
            DisbursementSheetRowCount,
            ReceivedDataRowCount,
            same_mobile_no_count)
              VALUES
              (\"""" + rownow[0] + """\",\""""+ rownow[1] +"""\",""" + str(dsCntMap[rownow[0]]) + """,""" + str(rdCntMap[rownow[1]]) +\
                   ""","""+ str(rownow[2]) +""")
         """
        cursor.execute(queryNow)

    db.commit()
    print 'Done for schoolCode ' + str(schoolCode)
cursor.close()
db.close()