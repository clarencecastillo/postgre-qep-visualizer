from utils import format, read_json
from analysis import analyze
import re

tests = read_json('tests.json')

print('Available Test Cases:')
for i, test in enumerate(tests):
    print(str(i) + '. ' + test['Test Case'])

test = tests[13]
test_case = test['Test Case']
query = test['Query']
execution_plan = test['Execution Plan']
print('Test Case: \n' + test_case + '\n')
print('Input Query: \n' + query + '\n')
print('Formatted Query: \n' + format(query) + '\n')

format(query)
'SELECT P.PROCEEDBOOKTITLE,\n\tI.INPROBOOKTITLE,\n\tPUBLICATION.PUBTITLE\nFROM INPROCEEDING AS I,\n\tPROCEEDING AS P,\n\tPUBLICATION\nWHERE P.PUBKEY = I.INPROCROSSREF\n\t\tAND I.PUBKEY = PUBLICATION.PUBKEY'
list(re.finditer("I.PUBKEY = PUBLICATION.PUBKEY", format(query)))
analyze(execution_plan, query)

# "Test Case": "Merge Join & Gather Merge",

  #
  # {
  #   "Query": "",
  #   "Test Case": "",
  #   "Execution Plan": {
  #
  #   }
  # },
#
  "Expected Query": [{
"Index": 0,
"Query": "PUBLICATION"
}, {
"Index": 0,
"Query": "PUBLICATION.PUBYEAR >= 2000"
}, {
"Index": 0,
"Query": "PUBLICATION.PUBYEAR <= 2017"
}]
