"""
accepts Operation objects and aggregates them into transactions
schedules the transactions to maintain consistency
"""

from transaction import Transaction, Operation, Item
import jsonpickle

class Schedule:
	def __init__(self, transactions, tids):
		super(Schedule, self).__init__()
		self.transactions = transactions
		self.tids = tids


def organise_operations(operations, schedule):
	# operations is a list containing Operation objects
	for operation in operations:
		# extract info from operation and add to relevant transaction
		tid = operation.tid
		if not(tid in schedule.tids):
			# new transaction
			# todo: second arg does not take transaction dependencies into account
			operations = []
			operations.append(operation)
			dset = []
			dset.append(operation.item)
			dset.append(operation.item.transactions)
			transaction = Transaction(operations, dset, tid)
			schedule.transactions.append(transaction)

		# scheduling operations
		print(jsonpickle.encode(schedule))


o1 = Operation('r', Item(variable='x', transactions=[]), '1') # r1(x)
operations = []
operations.append(o1)
organise_operations(operations, Schedule([], []))