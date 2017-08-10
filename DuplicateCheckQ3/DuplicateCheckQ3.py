__author__ = 'insan'

import MySQLdb
import sys
import os
import csv
import datetime

import configparser






################################
schema_of_main_table = ""
schema_4_intermediate_table = ""
schoolwise_intermediate_tbl_l1 = ""
schoolwise_intermediate_tbl_l2 = ""
distinct_school_parent_table = ""
same_cnt_table_schema = ""
same_cnt_table = ""

progress_bar_len = 25


################################
def set_schema_table_variables():
    config = get_reader()

    global schema_of_main_table
    schema_of_main_table = config.get('DB_SCHEMA_TABLE', 'schema')
    global schema_4_intermediate_table
    schema_4_intermediate_table = config.get('DB_SCHEMA_TABLE', 'intermediateTableSchema')
    global schoolwise_intermediate_tbl_l1
    schoolwise_intermediate_tbl_l1 = config.get('DB_SCHEMA_TABLE', 'intermediateTable_l1')
    global schoolwise_intermediate_tbl_l2
    schoolwise_intermediate_tbl_l2 = config.get('DB_SCHEMA_TABLE', 'intermediateTable_l2')
    global distinct_school_parent_table
    distinct_school_parent_table = config.get('DB_SCHEMA_TABLE', 'distinctSchoolParentTable')
    global same_cnt_table_schema
    same_cnt_table_schema = config.get('DB_SCHEMA_TABLE', 'sameCntTableSchema')
    global same_cnt_table
    same_cnt_table = config.get('DB_SCHEMA_TABLE', 'sameCntTable')


def drop_table_if_exists(db_con, schema_name, table_name):
    cursor = db_con.cursor()
    cursor.execute("""
        DROP TABLE IF EXISTS %s.%s
    """ % (schema_name, table_name))
    db_con.commit()


def get_progress_bar(count, total):
    bar_value = progress_bar_len * count / total
    bar = "<"
    for i in range(bar_value):
        bar += "-"
    for i in range(progress_bar_len - bar_value):
        bar += "."
    bar += ">"
    return bar


def perform_duplicate_check_and_insert_in_db(db_con, school_id_list):
    cusror = db_con.cursor()

    count = 0
    total = len(school_id_list)
    for school_id in school_id_list:
        query = """
            INSERT INTO %s.%s
                    SELECT
              t1.school_id sc_id_1,
              t2.school_id sc_id_1,
              count(*) common_cnt
            FROM bugs.distinctSchoolParentTable t1 INNER JOIN bugs.distinctSchoolParentTable t2
                ON t1.mobile_no = t2.mobile_no
            WHERE t1.school_id > t2.school_id
              AND t1.school_id = %d
              AND LENGTH (t1.mobile_no) = 11
            GROUP BY t1.school_id, t2.school_id
        """ % (schema_4_intermediate_table, same_cnt_table, school_id)
        cusror.execute(query)
        count += 1

        if count % 1000 == 0:
            db_con.commit()
        bar = get_progress_bar(count, total)
        sys.stdout.write("\r" + bar + "  Done " + str(count) + " of " + str(total) + " schools.")
        sys.stdout.flush()

    db_con.commit()

    print "finished. Check %s.%s table" % (same_cnt_table_schema, same_cnt_table)


def school_id_fetch_from_l2_db(db_con):
    cursor = db_con.cursor()
    schoolCodeCollectQuery = "Select distinct school_id from %s.%s" % (
        schema_4_intermediate_table, schoolwise_intermediate_tbl_l2)
    cursor.execute(schoolCodeCollectQuery)
    result = cursor.fetchall()
    school_code_list = []
    for row in result:
        if row[0] is not None:
            school_code_list.append(row[0])
    print "School codes fetched."
    return school_code_list


def create_same_cnt_table(db_con):
    cursor = db_con.cursor()
    drop_table_if_exists(db_con, same_cnt_table_schema, same_cnt_table)

    tableCreateQuery = """
        CREATE TABLE %s.%s (
        sc_id_1 INT (9) NOT NULL,
        sc_id_2 INT (9) NOT NULL,
        common_cnt INT(5) NULL)
    """ % (same_cnt_table_schema, same_cnt_table)
    cursor.execute(tableCreateQuery)
    db_con.commit()
    print "Same count table re-created"


def get_schoolCode_rowCnt_map1(db_con):
    cursor = db_con.cursor()
    cursor.execute(
        "select school_code, rowCount from %s.%s" % (schema_4_intermediate_table, schoolwise_intermediate_tbl_l1))
    dsResult = cursor.fetchall()
    school_rowCnt_map = {}
    for dsr in dsResult:
        school_rowCnt_map[dsr[0]] = dsr[1]
    return school_rowCnt_map


def create_DsrdRep_like_table_by_joining(db_con):
    cursor = db_con.cursor()
    drop_table_if_exists(db_con, schema_4_intermediate_table, schoolwise_intermediate_tbl_l1)
    query = """
        CREATE TABLE %s.%s
    SELECT
      sc.code school_code,
      sc.id school_id,
      p.mobile mobile_no
    FROM %s.student_stipend ss INNER JOIN %s.student st ON ss.student_id = st.id
      INNER JOIN %s.school sc ON st.school_id = sc.id
      INNER JOIN %s.parent p ON st.parent_id = p.id
    WHERE ss.session_id = 2 AND ss.status != 'DELETED'
    GROUP BY p.id, sc.id
    """ % (schema_4_intermediate_table, schoolwise_intermediate_tbl_l1, schema_of_main_table, schema_of_main_table,
           schema_of_main_table,
           schema_of_main_table)
    cursor.execute(query)
    db_con.commit()

    cursor.execute(
        """CREATE INDEX index1 ON %s.%s (school_id)""" % (schema_4_intermediate_table, schoolwise_intermediate_tbl_l1))
    cursor.execute(
        """CREATE INDEX index2 ON %s.%s (mobile_no)""" % (schema_4_intermediate_table, schoolwise_intermediate_tbl_l1))
    db_con.commit()


def create_second_level_intermediate_table(db_con):
    cursor = db_con.cursor()
    drop_table_if_exists(db_con, schema_4_intermediate_table, schoolwise_intermediate_tbl_l2)

    query = """
        create table %s.%s
        Select school_id, count(*) rowCount, count(distinct mobile_no) distinctMobCount
        from %s.%s
        group by school_id
    """ % (schema_4_intermediate_table, schoolwise_intermediate_tbl_l2, schema_4_intermediate_table,
           schoolwise_intermediate_tbl_l1)

    cursor.execute(query)
    db_con.commit()

    cursor.execute(
        """CREATE INDEX index1 ON %s.%s (school_id)""" % (schema_4_intermediate_table, schoolwise_intermediate_tbl_l2))
    cursor.execute("""CREATE INDEX index2 ON %s.%s (distinctMobCount)""" % (
        schema_4_intermediate_table, schoolwise_intermediate_tbl_l2))
    db_con.commit()


def create_distinct_mob_school_table(db_con):
    cursor = db_con.cursor()
    drop_table_if_exists(db_con, schema_4_intermediate_table, distinct_school_parent_table)

    query = """
    CREATE TABLE %s.%s
    SELECT
      sc.code  school_code,
      sc.id school_id,
      p.mobile mobile_no
    FROM %s.student_stipend ss INNER JOIN %s.student st ON ss.student_id = st.id
      INNER JOIN %s.school sc ON st.school_id = sc.id
      INNER JOIN %s.parent p ON st.parent_id = p.id
    WHERE ss.session_id = 2 AND ss.status != 'DELETED'
      AND p.mobile IS NOT NULL AND p.mobile != ''
    GROUP BY p.id, sc.id
    """ % (schema_4_intermediate_table, distinct_school_parent_table, schema_of_main_table, schema_of_main_table,
           schema_of_main_table,
           schema_of_main_table)
    cursor.execute(query)

    db_con.commit()

    cursor.execute(
        """CREATE INDEX index1 ON %s.%s (school_id)""" % (schema_4_intermediate_table, distinct_school_parent_table))
    cursor.execute(
        """CREATE INDEX index2 ON %s.%s (mobile_no)""" % (schema_4_intermediate_table, distinct_school_parent_table))
    db_con.commit()


def create_intermediate_tables(db_con):
    print "Creating intermediate tables. This may take more than 10 minutes. \nPlease wait patiently."
    create_DsrdRep_like_table_by_joining(db_con)
    create_second_level_intermediate_table(db_con)
    create_distinct_mob_school_table(db_con)
    print "Done!!!\n================\n===================="


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


def export(db_con):
    cursor = db_con.cursor()
    query = """
        SELECT
          sc1.code                 school_code1,
          sc2.code                 school_code2,
          intTbl1.rowCount         total1,
          intTbl2.rowCount         total2,
          intTbl1.distinctMobCount distMobile1,
          intTbl2.distinctMobCount distMobile2,
          sm.common_cnt            common
        FROM %s.%s sm
          INNER JOIN %s.school sc1 ON sm.sc_id_1 = sc1.id
          INNER JOIN %s.school sc2 ON sm.sc_id_2 = sc2.id
          INNER JOIN %s.%s intTbl1 ON sm.sc_id_1 = intTbl1.school_id
          INNER JOIN %s.%s intTbl2 ON sm.sc_id_2 = intTbl2.school_id
          ORDER BY sm.common_cnt DESC
    """ % (schema_4_intermediate_table, same_cnt_table, schema_of_main_table, schema_of_main_table,
           schema_4_intermediate_table, schoolwise_intermediate_tbl_l2, schema_4_intermediate_table,
           schoolwise_intermediate_tbl_l2)
    cursor.execute(query)
    rows = cursor.fetchall()
    description = cursor.description
    header = ()
    for desc in description:
        header = header + (desc[0],)
    cursor.close()
    if not os.path.exists('export'):
        os.makedirs('export')
    now = datetime.datetime.now().strftime("%y%m%d%H%M")
    fp = open(os.path.join('export', 'duplicateList' + str(now) + '.csv'), 'wb')
    myFile = csv.writer(fp)
    myFile.writerow(header)
    myFile.writerows(rows)
    fp.close()

    print "\nExported to CSV."


def Main():
    start_time = datetime.datetime.now()
    db_con = get_db_connection()
    set_schema_table_variables()
    create_intermediate_tables(db_con=db_con)
    create_same_cnt_table(db_con=db_con)
    school_id_list = school_id_fetch_from_l2_db(db_con=db_con)
    perform_duplicate_check_and_insert_in_db(db_con=db_con,
                                             school_id_list=school_id_list)
    export(db_con=db_con)
    end_time = datetime.datetime.now()
    print "Total process took time: " + str(end_time - start_time)


Main()
