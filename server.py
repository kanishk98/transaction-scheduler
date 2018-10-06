from flask import Flask, request 
import json
from request_handler import send_to_scheduler
from datamodel import Operation, Item
import jsonpickle

operations = []

app = Flask(__name__)

@app.route('/add-operation', methods=['POST'])
def add_operation():
	data = json.loads(request.data)
	i = data['item']
	var = i['variable']
	k = i['kind']
	item = Item(k, var)
	operation = Operation(item.kind, item, data['tid'])
	send_to_scheduler(operation)
	return str(jsonpickle.encode(operation))


app.run(debug=True)
