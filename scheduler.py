"""
accepts Operation objects and aggregates them into transactions
schedules the transactions to maintain consistency
"""

from datamodel import Transaction, Operation, Schedule
import jsonpickle
import math

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
		schedule.operations.append(operation)
		schedule.locktable = locktable
		print(jsonpickle.encode(schedule.locktable))
		create_dependency_graph(locktable)
		return schedule

def create_dependency_graph(locktable):
	# iterate over locktable to check dependencies
	graph = {}
	for key in locktable:
		print(key)
		tids = locktable[key]
		for tid in tids:
			dset = []
			t = Transaction(tid, key.kind)
			try:
				dset = graph[t]
			except Exception as e:
				graph[t] = []
			finally:
				# adds object to dset of every transaction
				dset.append(key)
				graph[t] = dset

	# graph between transactions and item sets created
	print('Graph: ' + str(jsonpickle.encode(graph)))

	dep_dict = find_dep_dict(graph, locktable)
	print('Dependency dictionary: ' + str(jsonpickle.encode(dep_dict)))

	e_dep_dict = refined_deps(dep_dict, True)
	print('Exclusive dependency transactions: ' + str(jsonpickle.encode(e_dep_dict)))

	s_dep_dict = refined_deps(dep_dict, False)
	print('Shared dependency transactions: ' + str(jsonpickle.encode(s_dep_dict)))

	# m is the size of ldset
	# tid is the transaction id associated with an m-sized dset
	m, tid = ldsf(e_dep_dict)
	print(m)
	print(tid)

	# creating batch
	# todo: find more efficient method to maximise function
	batch = bldsf(s_dep_dict)



def find_dep_dict(graph, locktable):
	# returns set containing number of transactions dependent on corresponding transaction
	dep_dict = {}
	for transaction in graph:
		items = graph[transaction]
		variables = []
		for item in items:
			variables.append(item.variable)
		dep_dict[transaction] = 0
		for t in graph:
			i = graph[t]
			for item in i:
				if item.variable in variables:
					# t has a dependency between itself and transaction
					dep_dict[transaction] = dep_dict[transaction] + 1
					break
		dep_dict[transaction] = dep_dict[transaction] - 1 
	return dep_dict


def ldsf(dep_dict):
	# fifo ordering followed for transactions with same dset size
	# todo: once conflicting ops are supported, replace fifo with proper to
	m = -1
	tid = -1
	for transaction in dep_dict:
		if dep_dict[transaction] > m and dep_dict:
			tid = transaction.tid
			m = dep_dict[transaction]

	return m, tid

def bldsf(dep_dict):
	batch = []
	m = []

	return batch

def refined_deps(dep_dict, kind):
	copy = dict(dep_dict)
	for transaction in dep_dict:
		if transaction.kind != kind:
			del copy[transaction]

	return copy
