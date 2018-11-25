import time
from scheduler import organise_operations
from datamodel import Operation, Schedule, Item
import jsonpickle



def send_to_scheduler(operation, length):
	schedule = organise_operations(operation, length)
