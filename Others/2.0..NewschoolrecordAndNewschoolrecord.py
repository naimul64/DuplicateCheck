#!/usr/bin/python

import MySQLdb

# Open database connection
db = MySQLdb.connect("192.168.168.18", "naimul", "naimul@#678", "bugs")

# prepare a cursor object using cursor() method
cursor = db.cursor()

tableDropQuery = "DROP TABLE IF EXISTS bugs.sameMobieNoCountBetweenNewschoolrecordAndNewschoolrecord"
cursor.execute(tableDropQuery)
db.commit()

tableCreateQuery = """
    CREATE TABLE bugs.sameMobieNoCountBetweenNewschoolrecordAndNewschoolrecord (
    first_school_code VARCHAR(13) NOT NULL,
    second_school_code VARCHAR(13) NOT NULL,
    first_school_row_count INT (5),
    second_school_row_count INT (5),
    same_mobile_no_count INT(5) NULL)
"""
cursor.execute(tableCreateQuery)
db.commit()

schoolCodeCollectQuery = "Select distinct school_code from bugs.newschoolrecordSchoolCodeMobNo"
cursor.execute(schoolCodeCollectQuery)
result = cursor.fetchall()
print "School fetched."

cursor.execute("select school_code, rowCount from bugs.schoolRowCountNewschoolRecord_intermediate")
nsrResult = cursor.fetchall()
nsrCntMap = {}
for nsrr in nsrResult:
    nsrCntMap[nsrr[0]] = nsrr[1]


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
            bugs.newschoolrecordSchoolCodeMobNo ds1
                INNER JOIN
            bugs.newschoolrecordSchoolCodeMobNo ds2 ON ds1.MobileNo = ds2.MobileNo
        WHERE
        length(ds1.MobileNo) = 11 AND cast(ds1.MobileNo as unsigned) != 0
        AND substring(ds1.MobileNo, 1,3) in ('015','016','017','018','019')
        AND length(ds2.MobileNo) = 11 AND cast(ds2.MobileNo as unsigned) != 0
        AND substring(ds2.MobileNo, 1,3) in ('015','016','017','018','019')
            AND ds1.school_code = '""" + schoolCode + """'
            AND ds2.school_code != '""" + schoolCode + """'
                AND CAST(ds1.school_code AS UNSIGNED) <= CAST(ds2.school_code AS UNSIGNED)
        GROUP BY ds1.school_code , ds2.school_code
    """
    cursor.execute(query)
    resultNow = cursor.fetchall()

    for rownow in resultNow:
        try:
            nsrCntMap[rownow[0]] + nsrCntMap[rownow[1]]
        except KeyError:
            print "One of the School codes not in newschoolrecord"
            continue

        queryNow = """
             INSERT INTO sameMobieNoCountBetweenNewschoolrecordAndNewschoolrecord (
             first_school_code,
            second_school_code,
            first_school_row_count,
            second_school_row_count,
            same_mobile_no_count)
              VALUES
              (\"""" + rownow[0] + """\",\""""+ rownow[1] +"""\",""" + str(nsrCntMap[rownow[0]]) + """,""" + str(nsrCntMap[rownow[1]]) +\
                   ""","""+ str(rownow[2]) +""")
         """
        cursor.execute(queryNow)

    db.commit()
    print 'Done for schoolCode ' + str(schoolCode)
cursor.close()
db.close()