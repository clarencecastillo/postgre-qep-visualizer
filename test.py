from utils import format
from analysis import *
import json

def read_json(file_path):
    with open(file_path, 'r') as f:
        content = json.load(f)
    return content

tests = read_json('tests.json')

print('Available Test Cases:')
for i, test in enumerate(tests):
    print(str(i) + '. ' + test['Test Case'])

test = tests[17]
test_case = test['Test Case']
query = test['Query']
execution_plan = test['Execution Plan']
print('Test Case: \n' + test_case + '\n')
print('Input Query: \n' + query + '\n')
print('Formatted Query: \n' + format(query) + '\n')

analyze(execution_plan, query)
