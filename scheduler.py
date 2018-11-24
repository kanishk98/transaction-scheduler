"""
accepts Operation objects and aggregates them into transactions
schedules the transactions to maintain consistency
"""

from datamodel import Transaction, Operation, Schedule
import jsonpickle
import math
import itertools

def organise_operations(operation, schedule, length):
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
		# print(dependent_on)
	finally:
		locktable[operation.item] = dependent_on
		schedule.operations.append(operation)
		schedule.locktable = locktable
		# print(jsonpickle.encode(schedule.locktable))
		create_dependency_graph(operation.item, locktable, length)
		return schedule

def create_dependency_graph(item, locktable, length):
	# iterate over locktable to check dependencies
	graph = {}
	for key in locktable:
		# key here represents an item used by some operations in schedule
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
	# print('Graph: ' + str(jsonpickle.encode(graph)))
	if len(graph) == length:
		core_algorithm(graph, locktable, item)

def core_algorithm(graph, locktable, item):
	dep_dict = find_dep_dict(graph, locktable)
	print('Dependency dictionary: ' + str(jsonpickle.encode(dep_dict)))

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
		# print(batch)

		dep_dict = schedule_operation(e, t, s, batch, dep_dict)

def schedule_operation(e, t, s, batch, dep_dict):
	"""
	compares e and s and schedules operations accordingly
	"""
	# todo: confirm that condition being used is correct

	logfile = open('logfile.txt', 'a')

	if e == -1:
		e = 0
	if s == -1:
		s = 0

	print('Exclusive dset size :' + str(e))
	print('Shared dset size: ' + str(s))
	if e > s:
		# exclusive transaction won
		print(str(t.tid) + ' gets scheduled')
		logfile.write(str(t.tid) + '\n')
		del dep_dict[t]
		# prevent starvation of transactions with small dsets
		dep_dict = age_transactions(dep_dict)
	else:
		b = []
		# batch of transactions won
		for transaction in batch:
			if type(transaction) == tuple:
				b.append(transaction[0].tid)
				del dep_dict[transaction[0]]
			else:
				b.append(transaction.tid)
				del dep_dict[transaction]
			dep_dict = age_transactions(dep_dict)		
		print(str(b) + ' all get scheduled')
		logfile.write(str(b) + '\n')
	return dep_dict


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
		dep_dict[transaction] = 0
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
	for i in range(1, k):
		temp_batch = list(itertools.combinations(t_arr, i))
		# print(jsonpickle.encode(temp_batch))
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
