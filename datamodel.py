class Item:
	def __init__(self, kind, variable):
		super(Item, self).__init__()
		self.variable = variable
		self.kind = kind

class Request:
	def __init__(self, operation, length):
		super(Request, self).__init__()
		self.operation = operation
		self.length = length

class Schedule:
	"""
	operations represents list of ops to be executed as part of self
	locktable represents table of requested data items and transactions in system memory
	tids represents tids of all transactions in system memory
	"""
	def __init__(self, operations, locktable, tids):
		super(Schedule, self).__init__()
		self.locktable = locktable
		self.operations = operations
		self.tids = tids
		
class Operation:
	"""
	kind represents kind of data operation (read/write represented by False/True)
	item represents data item being altered by Transaction
	tid represents transaction self belongs to
	"""
	def __init__(self, kind, item, tid):
		super(Operation, self).__init__()
		self.kind = kind
		self.item = item
		self.tid = tid

class Transaction:
	def __init__(self, tid, kind):
		super(Transaction, self).__init__()
		self.tid = tid
		self.kind = kind