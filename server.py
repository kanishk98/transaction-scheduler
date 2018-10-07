from flask import Flask, request 
import json
from request_handler import send_to_scheduler
from datamodel import Operation, Item, Schedule
import jsonpickle
import time

operations = []

app = Flask(__name__)

@app.route('/add-operation', methods=['POST'])
def add_operation():
	arr = json.loads(request.data)
	schedule = Schedule(operations, locktable, tids)
	for data in arr:
		i = data['item']
		var = i['variable']
		k = i['kind']
		item = Item(k, var)
		operation = Operation(item.kind, item, data['tid'])
		schedule = send_to_scheduler(operation, schedule)
	return str(jsonpickle.encode(arr))

operations = []
locktable = {}
tids = []

app.run(debug=True)
