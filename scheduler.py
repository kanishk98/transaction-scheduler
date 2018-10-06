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

	# graph between transactions and item sets created
	print(jsonpickle.encode(graph))

	dep_dict = find_dep_dict(graph, schedule.locktable)
	print(jsonpickle.encode(dep_dict))

	tid = ldsf(dep_dict)
	print(tid)

def find_dep_dict(graph, locktable):
	# returns set containing number of transactions dependent on corresponding transaction
	dep_dict = {}
	for transaction in graph:
		items = graph[transaction]
		dep_dict[transaction] = 0
		for item in items:
			# finding dset size using locktable
			dep_dict[transaction] = dep_dict[transaction] + len(locktable[item]) - 1
	return dep_dict

def ldsf(dep_dict):
	# todo: add timestamp ordering for transactions with equal dependency set size
	m = -1
	tid = -1
	for transaction in dep_dict:
		if dep_dict[transaction] > m:
			tid = transaction
			m = dep_dict[transaction]

	return tid