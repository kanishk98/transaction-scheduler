class Item:
	"""
	variable is the actual data item being represented
	transactions is the set of items waiting on self
	"""
	def __init__(self, variable, transactions):
		super(Item, self).__init__()
		self.variable = variable
		self.transactions = transactions
		

class Operation:
	"""
	kind represents kind of data operation (read/write)
	item represents data item being altered by Transaction
	tid represents transaction self belongs to
	"""
	def __init__(self, kind, item, tid):
		super(Operation, self).__init__()
		self.kind = kind
		self.item = item
		self.tid = tid

class Transaction:
	"""
	operations represents the set of transaction operations
	dset represents the set of transactions and items self is waiting on
	"""
	def __init__(self, operations, dset, tid):
		super(Transaction, self).__init__()
		self.operations = operations
		self.dset = dset
		self.tid = tid