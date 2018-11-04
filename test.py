from utils import format, read_json
from analysis import analyze
from operator import itemgetter
import re

def run_tests():

    for i, test in enumerate(tests):
        test_case = test['Test Case']
        print('{0}. {1}'.format(i, test_case))
        e, q = analyze(test['Execution Plan'], test['Query'])
        validate_plan(e['Plan'])

def validate_plan(plan):
    expected, actual = [sorted(l, key=itemgetter('match')) for l in (plan['Expected Query'], plan['Query'])]
    result = 'Passed' if len(expected) == len(actual) and all(x == y for x, y in zip(expected, actual)) else 'Failed'
    print('[{0}] {1}'.format(plan['Node Type'], result))

    if (result == 'Failed'):
        print('Expected:', plan['Expected Query'])
        print('Actual:', plan['Query'])

    if 'Plans' in plan:
        for sub_plan in plan['Plans']:
            validate_plan(sub_plan)

tests = read_json('tests.json')
run_tests()
