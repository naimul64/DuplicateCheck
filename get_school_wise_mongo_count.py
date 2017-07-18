from pymongo import MongoClient
from bson.code import Code

map_function_js = """
function () {
	var rows = 0;
	for(var i = 0, size = this.data.length; i < size; i++){
	  if(this.data[i].status === 'VALID'){
	    rows++;
	  }
	}
    emit(this.schoolCode.value.modified, rows);
}
"""

reduce_function_js = """
function (key, values) {
	var total = 0;
	for(var i = 0, size = values.length; i < size; i++){
	  total += values[i];
	}
    return total;
}
"""

map = Code(map_function_js)
reduce = Code(reduce_function_js)
client = MongoClient('icr.surecash.net', 27017)

db = client.stipend_icr
db.authenticate('khalid', 'pr0g0t1@2017')
print 'Authenticated'

result = db.disbursement_sheets.inline_map_reduce(map, reduce)


hostIp = "192.168.168.18"
userName = "naimul"
password = "naimul@#678"
dbName = "bugs"
db = MySQLdb.connect(hostIp, userName, password , dbName)
cursor = db.cursor()

tableDropQuery = "DROP TABLE IF EXISTS bugs.mongoSchoolRowCount"
cursor.execute(tableDropQuery)
db.commit()
print "Table dropped"

tableCreateQuery = """
    CREATE TABLE bugs.mongoSchoolRowCount (
    school_code VARCHAR(13) NOT NULL,
    row_count INT (5))
"""
cursor.execute(tableCreateQuery)
db.commit()
print "Table re-created"

cnt = 0
total = len(result)
for doc in result:
    school_code = doc['_id']
    count = int(doc['value'])

    cnt = cnt + 1
    schoolCode = schoolCode[0].strip()
    count = getRawDataCountFromAPI(schoolCode)
    query = """
        Insert into bugs.mongoSchoolRowCount ('school_code', 'row_count') VALUES
        ('"""+ schoolCode +"""', """+ str(count) +""")
    """
    cursor.execute(query)
    if cnt%1000 == 0:
        db.commit()
        print str(cnt) + " of " + str(total) + " inserted"
print "Inserted all"


cursor.execute("""
    ALTER TABLE `bugs`.`mongoSchoolRowCount`
    ADD INDEX `schoolCode` (`school_code` ASC)
""")
db.commit()
print "Index added"

db.close()

