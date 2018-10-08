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
		# print(key)
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
	dep_dict, t_dep_graph = find_dep_dict(graph, locktable)
	print('Dependency dictionary: ' + str(jsonpickle.encode(dep_dict)))
	print('T-dependency graph: ' + str(jsonpickle.encode(t_dep_graph)))

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

		dep_dict = schedule_operation(e, t, s, batch, dep_dict, t_dep_graph)	

def schedule_operation(e, t, s, batch, dep_dict, t_dep_graph):
	"""
	compares e and s and schedules operations accordingly
	"""
	# todo: confirm that condition being used is correct

	# cleaning batch object
	new_batch = []
	for b in batch:
		if type(b) == tuple:
			# b contains transactions
			for temp_transaction in b:
				if type(temp_transaction) == Transaction:
					new_batch.append(temp_transaction)
				else:
					print('Ouch :' + str(jsonpickle.encode(temp_transaction)))

	new_batch = set(new_batch)
	batch = new_batch
	print('Exclusive dset size: ' + str(e))
	print('Shared dset size: ' + str(s))
	if e >= s:
		# exclusive transaction won
		print(str(t.tid) + ' gets scheduled')
		del dep_dict[t]
		arr = []
		arr.append(t.tid)
		t_dep_graph, dep_dict = trim_dep_graph(t_dep_graph, arr, dep_dict)
	else:
		b = []
		arr = []
		# batch of transactions won
		print('Batch:' + jsonpickle.encode(batch))
		for transaction in batch:
			if type(transaction) == tuple:
				b.append(transaction[0].tid)
				del dep_dict[transaction[0]]
			else:
				b.append(transaction.tid)
				del dep_dict[transaction]		
		print(str(b) + ' all get scheduled')
		for temp in b:
			arr.append(temp)
		t_dep_graph, dep_dict = trim_dep_graph(t_dep_graph, arr, dep_dict)
	return dep_dict

def trim_dep_graph(t_dep_graph, transactions, dep_dict):
	# print(transactions)
	for tid in transactions:
		for value in t_dep_graph[tid]:
			t_dep_graph[value].remove(tid)
			# check dep_dict for transaction with tid matching value
			for t in dep_dict:
				if t.tid == value:
					dep_dict[t] = dep_dict[t] - 1

	return t_dep_graph, dep_dict
		


def find_dep_dict(graph, locktable):
	"""
	returns set containing number of transactions dependent on corresponding transaction
	read operations do not depend on other read operations
	"""
	t_dep_graph = {}
	
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
					try:
						t_dep_graph[t.tid].append(transaction.tid)
					except Exception as e:
						t_dep_graph[t.tid] = []
						t_dep_graph[t.tid].append(transaction.tid)
					if not (item.kind or items[i.index(item)].kind):
						dep_dict[transaction] = dep_dict[transaction] - 1
						t_dep_graph[t.tid].remove(transaction.tid)
					break
	return dep_dict, t_dep_graph


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

"""def find_t_dep_graph(graph, locktable):
	print('locktable: ' + str(jsonpickle.encode(locktable)))
	t_dep_graph = {}
	for item in locktable:
		tr_arr = locktable[item]
		for transaction in tr_arr:
			t_dep_graph[transaction] = []
			# checking which other transactions want same item in conflicting mode
			for i in locktable:
				t = locktable[i]
				if t != transaction and i.variable == item.variable and not(i.kind or item.kind):
					# t and transaction have dependency
					t_dep_graph[transaction].append(t)"""