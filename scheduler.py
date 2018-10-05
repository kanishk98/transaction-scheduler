"""
accepts Operation objects and aggregates them into transactions
schedules the transactions to maintain consistency
"""

from datamodel import Transaction, Operation, Item, Schedule
import jsonpickle

def organise_operations(operation, schedule):
	# operation represents new db operation
	# needs to be added to schedule

	locktable = schedule.locktable
	dependent_on = None
	try:
		dependent_on = locktable[operation.item]
	except Exception as e:
		dependent_on = [] 
		# item operation uses is not used by anything else
		locktable[operation.item] = operation.tid
	finally:
		print(jsonpickle.encode(locktable[operation.item]))

		

# def create_dependency_graph(schedule):
	# only transactions with an empty dset need to be checked for finding new links
	# if we know that a transaction in schedule depends on an item and b transaction also depends on same item
	# then b depends on a (a is in dset of b)

	
o1 = Operation('r', Item(variable='x'), 'A') # r1(x)
operations = []
operations.append(o1)
tids = []
tids.append('A')
organise_operations(o1, Schedule([], {}, tids))