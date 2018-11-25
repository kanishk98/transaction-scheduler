"""
accepts Operation objects and aggregates them into transactions
schedules the transactions to maintain consistency
"""

from datamodel import Operation, Schedule, Item
import jsonpickle
import math
import itertools
from time import sleep
import json

def organise_operations(operation, length):
	# operation represents new db operation
	# needs to be added to schedule
	global master_schedule
	schedule = master_schedule
	schedule.tids.append(operation.tid)
	locktable = clean_locktable(schedule.locktable)
	dependent_on = None
	try:
		dependent_on = locktable[operation.item]
		dependent_on.append(operation.tid)
	except Exception as e:
		dependent_on = [] 
		# item operation uses is not used by anything else
		dependent_on.append(operation)	
		# print(dependent_on)
	finally:
		locktable[operation.item] = dependent_on
		schedule.operations.append(operation)
		schedule.locktable = locktable
		master_schedule = schedule
		create_dependency_graph(operation.item, locktable, length)
		return schedule

def clean_locktable(locktable):
	copy = dict(locktable)
	for item in locktable:
		if len(locktable[item]) == 0:
			del copy[item]
	return copy

def create_dependency_graph(item, locktable, length, graph={}):
	old_length = length
	global master_length
	master_length = master_length + length
	length = master_length
	# iterate over locktable to check dependencies
	# print(jsonpickle.encode(locktable))
	print('CREATING DEPENDENCY GRAPH')
	for key in locktable:
		# key here represents an item used by some operations in schedule
		ops = locktable[key]
		for op in ops:
			dset = []
			try:
				dset = graph[op]
			except KeyError as e:
				graph[op] = []
			finally:
				# adds object to dset of every transaction
				dset.append(key)
				graph[op] = dset
	# graph between transactions and item sets created
	copy = dict(graph)
	for g in graph:
		try:
			if executed[g]:
				del copy[g]
		except KeyError:
			continue

	graph = copy
	print('Graph: ' + str(jsonpickle.encode(graph)))
	print('LENGTH: ' + str(length))
	if len(graph) == length:
		core_algorithm(graph, locktable, item)
	else:
		master_length = master_length - old_length

def core_algorithm(graph, locktable, item):
	global master_dep_dict
	dep_dict = find_dep_dict(graph, locktable)
	diff = dict(set(master_dep_dict) - set(dep_dict))
	print('Difference: ' + jsonpickle.encode(diff))
	# adding difference back to dep_dict
	for key in diff:
		dep_dict[key] = diff[key]
	master_dep_dict = dep_dict
	print('Dependency dictionary: ' + str(jsonpickle.encode(master_dep_dict)))
	while len(dep_dict) > 0:
		e_dep_dict = refined_deps(dep_dict, True)
		print('Exclusive dependency transactions: ' + str(jsonpickle.encode(e_dep_dict)))

		s_dep_dict = refined_deps(dep_dict, False)
		print('Shared dependency transactions: ' + str(jsonpickle.encode(s_dep_dict)))

		# e is the size of ldset
		# t is the transaction associated with an e-sized dset
		e, t = ldsf(e_dep_dict)
		# print(e)
		# print(tid)

		# creating batch
		# todo: find more efficient method to maximise function
		s, batch = bldsf(s_dep_dict)
		# print(s)
		# print(bldsf)
		dep_dict = schedule_operation(e, t, s, batch, locktable, dep_dict, graph)


def schedule_operation(e, t, s, batch, locktable, dep_dict, graph):
	"""
	compares e and s and schedules operations accordingly
	"""
	# todo: confirm that condition being used is correct
	
	# logfile = open('logfile.txt', 'a')

	if e == -1:
		e = 0
	if s == -1:
		s = 0
	print('Exclusive dset size: ' + str(e))
	print('Shared dset size: ' + str(s))
	sleep(20)
	if e > s:
		# exclusive transaction won
		print('Exclusive transaction ' + str(t.tid) + ' gets scheduled')
		# logfile.write(str(t.tid) + '\n')
		del dep_dict[t]
		# prevent starvation of transactions with small dsets
		dep_dict = age_transactions(dep_dict)
		exec_operation([t], graph)
	else:
		b = []
		exec_batch = []
		# batch of transactions won
		for transaction in batch:
			if type(transaction) == tuple:
				b.append(transaction[0].tid)
				exec_batch.append(transaction[0])
				del dep_dict[transaction[0]]
			else:
				b.append(transaction.tid)
				exec_batch.append(transaction)
				del dep_dict[transaction]
			dep_dict = age_transactions(dep_dict)		
		print('Shared transactions ' + str(b) + ' all get scheduled')
		exec_operation(exec_batch, graph)
		# logfile.write(str(b) + '\n')
	return dep_dict

def exec_operation(transactions, graph):
	for transaction in transactions:
		# outer loop iterates over scheduled operations
		global locked
		locked = True
		global master_schedule
		global executed
		executed[transaction] = True
		# print('Master schedule: ' + str(jsonpickle.encode(master_schedule)))
		schedule = master_schedule
		new_operations = schedule.operations
		new_tids = schedule.tids
		for tid in schedule.tids:
			try:
				new_tids.remove(transaction.tid)
			except ValueError as e:
				print('ValueError')
		for operation in schedule.operations:
			if operation == transaction:
				new_operations.remove(operation)
				# index by item and remove operation from locktable
				ops_locktable = schedule.locktable[operation.item]
				try:
					ops_locktable.remove(operation.tid)
					schedule.locktable[operation.item] = ops_locktable
				except ValueError as e:
					del schedule.locktable[operation.item]
		master_schedule = Schedule(new_operations, schedule.locktable, new_tids)
		locked = False


def age_transactions(dep_dict):
	for transaction in dep_dict:
		dep_dict[transaction] = dep_dict[transaction] + 0.5
	return dep_dict


def find_dep_dict(graph, locktable):
	"""
	returns set containing number of transactions dependent on corresponding transaction
	read operations do not depend on other read operations
	"""
	dep_dict = {}
	for transaction in graph:
		items = graph[transaction]
		variables = []
		for item in items:
			variables.append(item.variable)
		dep_dict[transaction] = 1
		for t in graph:
			i = graph[t]
			for item in i:
				if t == transaction:
					# comparing same 2 transactions
					break
				if item.variable in variables:
					# t has a dependency between itself and transaction
					dep_dict[transaction] = dep_dict[transaction] + 1
					if not (item.kind or items[i.index(item)].kind):
						dep_dict[transaction] = dep_dict[transaction] - 1
					break
	return dep_dict


def ldsf(dep_dict):
	# fifo ordering followed for transactions with same dset size
	# todo: once conflicting ops are supported, replace fifo with proper to
	m = -1
	t = None
	for transaction in dep_dict:
		if dep_dict[transaction] > m:
			t = transaction
			m = dep_dict[transaction]

	return m, t

def bldsf(dep_dict):
	batch = []
	m = 0.0
	if len(dep_dict) <= 1:
		for transaction in dep_dict:
			# print(jsonpickle.encode(dep_dict))
			m = dep_dict[transaction]
			batch.append(transaction)
		return m, batch
	
	k = len(dep_dict)
	t_arr = []
	n_arr = []
	for t in dep_dict:
		t_arr.append(t)
	for t in t_arr:
		n_arr.append(dep_dict[t])
	# print('K IS HERE: ' + str(k))
	# todo: check if range(1, k) or (1, k + 1) should be used
	for i in range(1, k):
		temp_batch = list(itertools.combinations(t_arr, i))
		print(jsonpickle.encode(temp_batch))
		total = 0
		for transaction in temp_batch:
			# print('TRANSACTION: ' + str(jsonpickle.encode(transaction)))
			# print(dep_dict[transaction[0]])
			# print('TRANSACTION[0]: ' + str(jsonpickle.encode(transaction[0])))
			total = total + dep_dict[transaction[0]]
		if total/math.sqrt(k) > m:
			m = total/math.sqrt(k)
			batch = temp_batch
		
	return m, batch


def refined_deps(dep_dict, kind):
	copy = dict(dep_dict)
	for transaction in dep_dict:
		if transaction.kind != kind:
			del copy[transaction]

	return copy

def shared_lock_requests(item, graph):
	copy = dict(graph)
	if not item.kind:
		return graph
	for transaction in graph:
		print('Inside for loop')
		if not (item in graph[transaction]):
			del copy[transaction]
	return copy

master_length = 0
master_schedule = Schedule([], {}, [])
master_dep_dict = {}
locked = False
executed = {}
