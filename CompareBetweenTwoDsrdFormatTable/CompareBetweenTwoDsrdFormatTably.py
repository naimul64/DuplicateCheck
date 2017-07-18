import configparser
import MySQLdb
import sys

schema_4_intermediate_table1 = ''
schoolwise_cnt_tbl1 = ''
schema_4_intermediate_table2 = ''
schoolwise_cnt_tbl2 = ''
schema_of_main_table1 = ''
main_table1 = ''
schema_of_main_table2 = ''
main_table2 = ''
same_cnt_table_schema = ''
same_cnt_table = ''


def get_reader():
    config = configparser.ConfigParser()
    config.sections()
    config.read('config.ini')

    return config


def get_db_connection():
    config = get_reader()
    section = config.default_section

    return MySQLdb.connect(config.get(section, 'db_ip'),
                           config.get(section, 'db_user_name'),
                           config.get(section, 'db_password'),
                           config.get(section, 'db_schema'))


def set_schema_table_variables():
    config = get_reader()

    global schema_of_main_table1
    schema_of_main_table1 = config.get('DB_SCHEMA_TABLE', 'schema1')
    global main_table1
    main_table1 = config.get('DB_SCHEMA_TABLE', 'table1')
    global schema_of_main_table2
    schema_of_main_table2 = config.get('DB_SCHEMA_TABLE', 'schema2')
    global main_table2
    main_table2 = config.get('DB_SCHEMA_TABLE', 'table2')
    global schema_4_intermediate_table1
    schema_4_intermediate_table1 = config.get('DB_SCHEMA_TABLE', 'intermediateTableSchema1')
    global schoolwise_cnt_tbl1
    schoolwise_cnt_tbl1 = config.get('DB_SCHEMA_TABLE', 'intermediateCntTable1')
    global schema_4_intermediate_table2
    schema_4_intermediate_table2 = config.get('DB_SCHEMA_TABLE', 'intermediateTableSchema2')
    global schoolwise_cnt_tbl2
    schoolwise_cnt_tbl2 = config.get('DB_SCHEMA_TABLE', 'intermediateCntTable2')
    global same_cnt_table_schema
    same_cnt_table_schema = config.get('DB_SCHEMA_TABLE', 'sameCntTableSchema')
    global same_cnt_table
    same_cnt_table = config.get('DB_SCHEMA_TABLE', 'sameCntTable')


def create_intermediate_tables(db_con):
    print "Creating intermediate table 1. Please wait for sometime."
    create_first_intermediate_table(db_con)
    print "Intermediate table 1 done."
    print "Creating intermediate table 2. Please wait for sometime."
    create_second_intermediate_table(db_con)
    print "Intermediate table 2 done."


def create_first_intermediate_table(db_con):
    cursor = db_con.cursor()
    cursor.execute("drop table if exists %s.%s" % (schema_4_intermediate_table1, schoolwise_cnt_tbl1))
    db_con.commit()
    cursor.execute("""
        create table %s.%s
        Select school_code , count(*) rowCount, count(distinct mobile_no) distinctMobCount
        from %s.%s
        group by school_code
    """ % (schema_4_intermediate_table1, schoolwise_cnt_tbl1, schema_of_main_table1, main_table1))
    db_con.commit()
    cursor.execute("""
        ALTER TABLE %s.%s
        ADD INDEX `school_code` (`School_code` ASC)
    """ % (schema_4_intermediate_table1, schoolwise_cnt_tbl1))
    db_con.commit()


def create_second_intermediate_table(db_con):
    cursor = db_con.cursor()
    cursor.execute("drop table if exists %s.%s" % (schema_4_intermediate_table2, schoolwise_cnt_tbl2))
    db_con.commit()
    cursor.execute("""
        create table %s.%s
        Select school_code , count(*) rowCount, count(distinct mobile_no) distinctMobCount
        from %s.%s
        group by school_code
    """ % (schema_4_intermediate_table2, schoolwise_cnt_tbl2, schema_of_main_table2, main_table2))
    db_con.commit()
    cursor.execute("""
        ALTER TABLE %s.%s
        ADD INDEX `school_code` (`School_code` ASC)
    """ % (schema_4_intermediate_table2, schoolwise_cnt_tbl2))
    db_con.commit()


def get_schoolCode_rowCnt_map1(db_con):
    cursor = db_con.cursor()
    cursor.execute("select school_code, rowCount from %s.%s" % (schema_4_intermediate_table1, schoolwise_cnt_tbl1))
    dsResult = cursor.fetchall()
    school_rowCnt_map = {}
    for dsr in dsResult:
        school_rowCnt_map[dsr[0]] = dsr[1]
    return school_rowCnt_map


def get_schoolCode_rowCnt_map2(db_con):
    cursor = db_con.cursor()
    cursor.execute("select school_code, rowCount from %s.%s" % (schema_4_intermediate_table2, schoolwise_cnt_tbl2))
    rdResult = cursor.fetchall()
    school_rowCnt_map = {}
    for rdr in rdResult:
        school_rowCnt_map[rdr[0]] = rdr[1]
    return school_rowCnt_map


def create_same_cnt_table(db_con):
    cursor = db_con.cursor()
    tableDropQuery = "DROP TABLE IF EXISTS %s.%s" % (same_cnt_table_schema, same_cnt_table)
    cursor.execute(tableDropQuery)
    db_con.commit()

    tableCreateQuery = """
        CREATE TABLE %s.%s (
        school_code1 VARCHAR(13) NOT NULL,
        school_code2 VARCHAR(13) NOT NULL,
        row_count1 INT (5),
        row_count2 INT (5),
        same_mobile_no_count INT(5) NULL)
    """ % (same_cnt_table_schema, same_cnt_table)
    cursor.execute(tableCreateQuery)
    db_con.commit()
    print "Same count table re-created"


def first_db_school_code_fetch(db_con):
    cursor = db_con.cursor()
    schoolCodeCollectQuery = "Select distinct school_code from %s.%s" % (
    schema_4_intermediate_table1, schoolwise_cnt_tbl1)
    cursor.execute(schoolCodeCollectQuery)
    result = cursor.fetchall()
    print "School codes fetched."
    school_code_list = []
    for row in result:
        if row[0] is not None:
            school_code_list.append(row[0])
    return school_code_list



def perform_duplicate_check_and_insert_in_db(db_con, schoolCode_list, school_rowCntMap1, school_rowCntMap2):
    cursor = db_con.cursor()
    count = 0
    total = len(schoolCode_list)
    for school_code in schoolCode_list:
        query = """
            SELECT
                ds1.school_code,
                ds2.school_code,
                COUNT(*) same_mobile_cnt
            FROM
                %s.%s ds1
                    INNER JOIN
                %s.%s ds2 ON ds1.mobile_no = ds2.mobile_no
            WHERE
            length(ds1.mobile_no) = 11 AND cast(ds1.mobile_no as unsigned) != 0
            AND substring(ds1.mobile_no, 1,3) in ('015','016','017','018','019')
            AND length(ds2.mobile_no) = 11 AND cast(ds2.mobile_no as unsigned) != 0
            AND substring(ds2.mobile_no, 1,3) in ('015','016','017','018','019')
                AND ds1.school_code != '%s'
            GROUP BY ds1.school_code , ds2.school_code
        """ % (schema_of_main_table1, main_table1, schema_of_main_table2, main_table2,school_code)
        cursor.execute(query)
        results = cursor.fetchall()

        for rownow in results:
            queryNow = """
             INSERT INTO sameMobieNoCountBetweenDssheetRecvddata (
             school_code1,
            school_code2,
            row_count1,
            row_count2,
            same_mobile_no_count)
              VALUES
              (\"""" + rownow[0] + """\",\""""+ rownow[1] +"""\",""" + str(school_rowCntMap1[rownow[0]]) + """,""" + str(school_rowCntMap2[rownow[1]]) +\
                   ""","""+ str(rownow[2]) +""")
         """
        cursor.execute(queryNow)
        count += 1
        sys.stdout.write("\rDone " + str(count) + " of " + str(total) + " schools.")
        sys.stdout.flush()
        if (count % 1000 == 0):
            db_con.commit()
    print "finished. Check %s.%s table" %(same_cnt_table_schema,same_cnt_table)

def Main():
    db_con = get_db_connection()
    set_schema_table_variables()
    # create_intermediate_tables(db_con=db_con)
    schoolCode_rowCnt_map1 = get_schoolCode_rowCnt_map1(db_con=db_con)
    schoolCode_rowCnt_map2 = get_schoolCode_rowCnt_map2(db_con=db_con)
    create_same_cnt_table(db_con=db_con)
    school_code_list = first_db_school_code_fetch(db_con=db_con)
    perform_duplicate_check_and_insert_in_db(db_con=db_con,
                                             schoolCode_list=school_code_list,
                                             school_rowCntMap1=schoolCode_rowCnt_map1,
                                             school_rowCntMap2=schoolCode_rowCnt_map2)


Main()
