from flask import Flask, request 
import json
from request_handler import send_to_scheduler
import fifo
import ldsf
from datamodel import Operation, Item, Schedule
import jsonpickle
import time

operations = []

app = Flask(__name__)

@app.route('/add-operation', methods=['POST'])
def add_operation():
	arr = json.loads(request.data)
	print('Request received')
	for data in arr:
		i = data['item']
		var = i['variable']
		k = i['kind']
		item = Item(k, var)
		operation = Operation(item.kind, item, data['tid'])
		send_to_scheduler(operation, len(arr))
	with open('./bldsf_time.txt', 'r') as f:
		return f.readline()
	return 'OK'


@app.route('/add-operation-fifo', methods=['POST'])
def add_operation_fifo():
	arr = json.loads(request.data)
	print('Request received')
	for data in arr:
		i = data['item']
		var = i['variable']
		k = i['kind']
		item = Item(k, var)
		operation = Operation(item.kind, item, data['tid'])
		time_taken = fifo.organise_operations(operation)
	print('TIME TAKEN FOR FIFO: ' + str(time_taken) + ' units')
	return str(time_taken)

@app.route('/add-operation-ldsf', methods=['POST'])
def add_operation_ldsf():
	arr = json.loads(request.data)
	print('Request received')
	for data in arr:
		i = data['item']
		var = i['variable']
		k = i['kind']
		item = Item(k, var)
		operation = Operation(item.kind, item, data['tid'])
		ldsf.organise_operations(operation, len(arr))
	with open('./ldsf_time.txt', 'r') as f:
		return f.readline()
	return 'OK'


app.run(debug=True, threaded=True)