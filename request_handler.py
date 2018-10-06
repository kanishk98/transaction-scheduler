import time
from scheduler import organise_operations
from datamodel import Operation, Schedule, Item
import jsonpickle

operations = []
tids = []

o1 = Operation(False, Item(False, 'x'), 1) # r1(x)
print(jsonpickle.encode(o1))
schedule = organise_operations(o1, Schedule(operations, {}, tids))
print('Goodnight!')

time.sleep(3)

print('Waking up...')
o2 = Operation(True, Item(True, 'x'), 2) # w2 (x)
print(jsonpickle.encode(o2))
organise_operations(o2, schedule)