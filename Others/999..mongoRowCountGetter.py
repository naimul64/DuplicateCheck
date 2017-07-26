# import mysql.connector
import csv
import json
from datetime import datetime

import requests


def getRawDataFromAPI(school_code, file_name):
    url = "http://icr.surecash.net:8080/icr-server/ds/beneficiary"

    querystring = {"schoolCode": school_code}

    headers = {
        'sessionid': "58dbb46f7acf85e2890c5658-1490793628350",
        }

    response = requests.request("GET", url, headers=headers, params=querystring)

    rows = json.loads(response.text)

    raw_data = []

    c = csv.writer(open(file_name, "wb"))
    n = -1 
    for row in rows:
        n += 1
        raw_data.append([])
        raw_data[n].append(row['name'])
        raw_data[n].append(row['nid'])
        raw_data[n].append(row['mobile'])
        raw_data[n].append(row['amount'])
        raw_data[n].append(row['compositeId'])
        c.writerow(raw_data[n])

def getRawDataCountFromAPI(school_code):
    url = "http://icr.surecash.net:8080/icr-server/ds/beneficiary"

    querystring = {"schoolCode": school_code}

    headers = {
        'sessionid': "58dbb46f7acf85e2890c5658-1490793628350",
        }

    response = requests.request("GET", url, headers=headers, params=querystring)

    rows = json.loads(response.text)
    return len(rows)


def getReceivedDataFromAPI(school_code, file_name):
    url = "http://192.168.10.117/SureCash/data-api"

    querystring = {"schoolCode": school_code}

    response = requests.request("GET", url, params=querystring)

    rows = json.loads(response.text)

    received_data = []

    c = csv.writer(open(file_name, "wb"))
    n = -1 
    for row in rows:
        n += 1
        received_data.append([])
        received_data[n].append(row['Beneficiary'])
        received_data[n].append(row['NID'])
        received_data[n].append(row['MobileNo'])
        received_data[n].append(row['Amount'])
        received_data[n].append(row['Remark'])
        received_data[n].append(row['composite_id'])
        c.writerow(received_data[n])

def getReceivedDataCountFromAPI(school_code):
    url = "http://192.168.10.138/SureCash/data-api"

    querystring = {"schoolCode": school_code, "countOnly": 1}

    response = requests.request("GET", url, params=querystring)

    rows = json.loads(response.text)

    return rows[0]["Count"]


def getRawReportDataFromAPI(school_code, file_name):
    url = "http://192.168.10.117/SureCash/data-api/raw_report"

    querystring = {"schoolCode": school_code}

    response = requests.request("GET", url, params=querystring)

    rows = json.loads(response.text)

    received_data = []

    c = csv.writer(open(file_name, "wb"))
    n = -1 
    for row in rows:
        n += 1
        received_data.append([])
        received_data[n].append(row['name'])
        received_data[n].append(row['nid'])
        received_data[n].append(row['mobile_no'])
        received_data[n].append(row['amount'])
        c.writerow(received_data[n])



# f = open('schools.csv', 'rb')
# reader = csv.reader(f)
# for row in reader:
#     sc = str(row[0])
#     # print sc + "," + str(getRawDataCountFromAPI(sc))
#     print sc + "," + str(getReceivedDataCountFromAPI(sc))
#     sys.stdout.flush()


#sc = sys.argv[1]
a = datetime.now()
print getRawDataCountFromAPI('99101010101')
b = datetime.now()
print b-a