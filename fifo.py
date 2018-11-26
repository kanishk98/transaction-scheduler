import jsonpickle
from time import sleep

queue = []
executing = False
time_taken = 0

def organise_operations(operation):
	global queue
	queue.append(operation)
	while executing:
		continue
	execute(queue[0])
	queue.pop(0)
	return time_taken

def execute(operation):
	print(str(operation.tid) + ' is being executed')
	global executing
	executing = True
	global time_taken
	time_taken = time_taken + 5
	executing = False