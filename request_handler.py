import time
from scheduler import organise_operations
from datamodel import Operation, Schedule, Item
import jsonpickle



def send_to_scheduler(operation):
	operations = []
	tids = []
	schedule = Schedule(operations, {}, tids)
	schedule = organise_operations(operation, schedule)
	time.sleep(5)

