import time
from scheduler import organise_operations
from datamodel import Operation, Schedule, Item
import jsonpickle



def send_to_scheduler(operation, schedule, length):
	schedule = organise_operations(operation, schedule, length)
	print(jsonpickle.encode(schedule))
	return schedule
