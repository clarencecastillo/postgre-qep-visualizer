from utils import format, read_json
from analysis import analyze
import re

tests = read_json('tests.json')

print('Available Test Cases:')
for i, test in enumerate(tests):
    print(str(i) + '. ' + test['Test Case'])

test = tests[20]
test_case = test['Test Case']
query = test['Query']
execution_plan = test['Execution Plan']
print('Test Case: \n' + test_case + '\n')
print('Input Query: \n' + query + '\n')
print('Formatted Query: \n' + format(query) + '\n')

format(query)
'SELECT DISTINCT PUBLICATION.PUBKEY\nFROM PUBLICATION'
list(re.finditer("PUBLICATION", format(query)))

analyze(execution_plan, query)

# "Test Case": "Gather Merge, Index Join",
  #
  # {
  #   "Query": "",
  #   "Test Case": "",
  #   "Execution Plan": {
  #
  #   }
  # },
#
