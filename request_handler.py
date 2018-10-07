import time
from scheduler import organise_operations
from datamodel import Operation, Schedule, Item
import jsonpickle



def send_to_scheduler(operation, schedule):
	schedule = organise_operations(operation, schedule)
	return schedule
