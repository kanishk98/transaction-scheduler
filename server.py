from flask import Flask, request 
import json

operations = []

app = Flask(__name__)

@app.route('/add-operation', methods=['POST'])
def add_operation():
	op_json = request.data
	return str(json.loads(op_json))

app.run()
