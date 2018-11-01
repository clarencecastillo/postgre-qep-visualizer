from utils import format, read_json
from analysis import analyze
import re

tests = read_json('tests.json')

print('Available Test Cases:')
for i, test in enumerate(tests):
    print(str(i) + '. ' + test['Test Case'])

test = tests[1]
test_case = test['Test Case']
query = test['Query']
execution_plan = test['Execution Plan']
print('Test Case: \n' + test_case + '\n')
print('Input Query: \n' + query + '\n')
print('Formatted Query: \n' + format(query) + '\n')

format(query)

list(re.finditer('PUBLICATION', format(query)))

analyze(execution_plan, query)


  #
  # {
  #   "Query": "",
  #   "Test Case": "",
  #   "Execution Plan": {
  #
  #   }
  # },

#   "Expected Query": [{
# "Index": 0,
# "Query": "PUBLICATION"
# }, {
# "Index": 0,
# "Query": "PUBLICATION.PUBYEAR >= 2000"
# }, {
# "Index": 0,
# "Query": "PUBLICATION.PUBYEAR <= 2017"
# }]
