import logging
import random
import time
from pprint import pprint
import json
import datetime
import azure.functions as func
import pymysql

hostname = 'localhost'
username = 'root'
password = 'root'
database = 'test'

def str_time_prop(start, end, format, prop):
    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))
    ptime = stime + prop * (etime - stime)
    return time.strftime(format, time.localtime(ptime))


def random_date(start, end, prop):
    return str_time_prop(start, end, '%m/%d/%Y %I:%M %p', prop)

def get_pie_insight():
	return get_cheese_insight()

def get_bar_insight():
	return get_cheese_insight()

def get_cheese_insight():

	result_json = []
	result_insert = []

	row = ["transaction_time", "user_acc_id", "recipient_acc_id", "amount"]

	user_acc_id = range(1000, 1100)
	recipient_acc_id = range(1000, 1100)
	amount = random.randrange(10000, 10000000)

	for i  in range(10):
		json_row = {}
		time_str = random_date("1/1/2017 1:30 PM", "01/02/2020 4:50 AM", random.random())
		time_stamp = time.mktime(datetime.datetime.strptime(time_str, "%m/%d/%Y %H:%M %p").timetuple())
		json_row["transaction_time"] = time_stamp
		json_row["user_acc_id"] = random.randrange(1000, 1100)
		json_row["recipient_acc_id"] = random.randrange(5000, 5100)
		json_row["amount"] = random.randrange(10000, 10000000)
		# result_json.append(json_row)

		json_tuple = (json_row["transaction_time"], str(json_row["user_acc_id"]), str(json_row["recipient_acc_id"]), json_row["amount"])
		result_insert.append(json_tuple)

	# return json.dumps(result_json)
	return result_insert


def doQuery( conn ) :
    cur = conn.cursor()
    cur.execute( "SELECT fname, lname FROM employee" )
    print cur.fetchall()
    for firstname, lastname in cur.fetchall():
        print firstname, lastname

def addTransactionDetailToDB(conn, transaction_detail):
	cur = conn.cursor()
	insert_sql = "INSERT INTO transactions (transaction_time, user_acc_id, recipient_acc_id, amount) VALUES (%s, %s, %s, %d)"
	cur.executemany(insert_sql, transaction_detail)
	conn.commit()
	print(cur.rowcount, "was inserted.")

	cur.execute("SELECT (transaction_time, user_acc_id, recipient_acc_id, amount) FROM transactions")
	for transaction_time, user_acc_id, recipient_acc_id, amount in cur.fetchall():
		print transaction_time, user_acc_id, recipient_acc_id, amount	

def connect_to_mysql():
	print "Using pymysql.."
	myConnection = pymysql.connect( host=hostname, user=username, passwd=password, db=database )
	# doQuery(myConnection)
	addTransactionDetailToDB(myConnection, get_cheese_insight())
	myConnection.close()

connect_to_mysql()

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    chart = req.params.get('chart')
    if not chart:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if chart == "pie":
        return func.HttpResponse(get_pie_insight())
    elif chart == "transaction":
    	return func.HttpResponse(get_cheese_insight())
    elif chart == "bar":
    	return func.HttpResponse(get_bar_insight())
    else:
        return func.HttpResponse(
             "Please pass a name on the query string or in the request body",
             status_code=400

    