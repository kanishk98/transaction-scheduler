"""
accepts Operation objects and aggregates them into transactions
schedules the transactions to maintain consistency
"""

from datamodel import Transaction, Operation, Schedule
import jsonpickle

def organise_operations(operation, schedule):
	# operation represents new db operation
	# needs to be added to schedule
	schedule.tids.append(operation.tid)
	locktable = schedule.locktable
	dependent_on = None
	try:
		dependent_on = locktable[operation.item]
		dependent_on.append(operation.tid)
	except Exception as e:
		dependent_on = [] 
		# item operation uses is not used by anything else
		dependent_on.append(operation.tid)	
		print(dependent_on)
	finally:
		locktable[operation.item] = dependent_on
		schedule.locktable = locktable
		schedule.operations.append(operation)
		print(jsonpickle.encode(schedule.locktable))
		create_dependency_graph(schedule)	
		return schedule

def create_dependency_graph(schedule):
	# iterate over locktable to check dependencies
	# todo: shouldn't we save graph in a file to make processing faster?
	graph = {}
	for key in schedule.locktable:
		print(key)
		tids = schedule.locktable[key]
		for tid in tids:
			dset = []
			try:
				dset = graph[tid]
			except Exception as e:
				graph[tid] = []
			finally:
				# adds object to dset of every transaction
				dset.append(key)
				graph[tid] = dset

	print(jsonpickle.encode(graph))