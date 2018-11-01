from utils import format
import re

QUERY_LIMIT_REGEX = r"\bLIMIT\s+{0}"
QUERY_SORT_REGEX = r"ORDER BY\s+{0}"
QUERY_SORT_KEY_REGEX = r"{0}(\s+{1})?"
QUERY_SCAN_REGEX = r"(FROM\s+)?{0}(\s+{1})?"

NODE_DESCRIPTIONS = {
    'Seq Scan': 'Scans the entire relation as stored on disk.',
    'Index Scan': 'Performs a B-tree traversal, walks through the leaf nodes to find all matching entries, and fetches the corresponding table data.',
    'Hash Join': 'Loads the candidate records from one side of the join into a hash table which is then probed for each record from the other side of the join.',
    'Aggregate': 'Groups records together based on a key or an aggregate function.',
    'Limit': 'Returns a specified number of rows from a record set.',
    'Sort': 'Sorts a record set based on the specified sort key.',
    'Nested Loop': 'Merges two record sets by looping through every record in the first set and trying to find a match in the second set.',
    'Merge Join': 'Merges two record sets by first sorting them on a join key.',
    'Hash': 'Generates a hash table from the records in the input recordset.',
    'CTE_Scan': 'Performs sequential scan of a Common Table Expression (CTE) query results.',
    'Bitmap Heap Scan': 'Searches through the pages returned by the Bitmap Index Scan for relevant rows.',
    'Index Only Scan': 'Finds relevant records based on an Index. Performs a single read operation from the index and does not read from the corresponding table.'
}

def analyze(execution_plan, query):
    formatted_query = format(query)
    analyze_plan(execution_plan['Plan'], formatted_query)
    execution_plan['Longest Duration'] = get_longest_duration(execution_plan['Plan'])
    return execution_plan, formatted_query

def get_node_description(plan):

    description = ''
    if plan['Node Type'] in NODE_DESCRIPTIONS:
        description = NODE_DESCRIPTIONS[plan['Node Type']]

    return description

def calculate_actual_duration(plan):
    actual_duration = plan['Actual Total Time']
    if 'Plans' in plan.keys():
        for sub_plan in plan['Plans']:
            if (sub_plan['Node Type'] != 'CTE_Scan'):
                actual_duration -= sub_plan['Actual Total Time'];

    # time is reported for an invidual loop
    # actual duration must be adjusted by number of loops
    actual_duration = actual_duration * plan['Actual Loops'];
    return actual_duration;

def get_longest_duration(plan):
    duration = plan['Actual Duration']

    if 'Plans' in plan:
        for sub_plan in plan['Plans']:
            duration = max(duration, get_longest_duration(sub_plan))
        return duration
    return duration


def analyze_plan(plan, query):
    plan['Query'] = get_query_components(plan, query)
    plan['Actual Duration'] = calculate_actual_duration(plan)
    plan['Description'] = get_node_description(plan)

    if 'Plans' in plan.keys():
        for sub_plan in plan['Plans']:
            analyze_plan(sub_plan, query)

def get_query_components(plan, query):

    query_components = []

    if plan['Node Type'] == 'Limit':
        query_components = parse_limit(plan, query)

    elif plan['Node Type'] == 'Sort':
        query_components = parse_sort(plan, query)

    elif plan['Node Type'] == 'Seq Scan':
        query_components = parse_seq_scan(plan, query)

    # elif plan['Node Type'] in ['Index Only Scan', 'Bitmap Index Scan', 'Bitmap Heap Scan']:
    #     query_components = parse_index_only_scan(plan, query)
    # elif plan['Node Type'] == 'Index Scan':
    #     query_components = parse_index_scan(plan, query)
    # elif plan['Node Type'] == 'Hash Join':
    #     query_components = parse_hash_join(plan, query)
    # elif plan['Node Type'] == 'Merge Join':
    #     query_components = parse_merge_join(plan, query)
    # elif plan['Node Type'] == 'Aggregate':
    #     query_components = parse_aggregate(plan, query)
    # elif plan['Node Type'] == 'Sort':
    #     query_components = parse_sort(plan, query)
    # elif plan['Node Type'] == 'Limit':
    #     query_components = parse_limit(plan, query)
    # elif plan['Node Type'] == 'Unique':
    #     query_components = parse_unique(plan, query)
    # else:
    #     query_components = parse_general(plan, query)

    return [c.upper() for c in query_components]

def parse_sort(plan, query):

    if 'Sort Key' not in plan.keys():
        return []

    sort_keys_regex = []

    sort_keys = plan['Sort Key']
    for sort_key in sort_keys:
        # descending keys are surrounded by parenthesis
        desc = sort_key.startswith("(") and sort_key.endswith(")")
        direction = "DESC" if desc else "ASC"

        # remove surrounding parenthesis
        sort_key = sort_key[1:-1] if desc else sort_key

        # escape parenthesis for function calls
        sort_key = sort_key.replace("(", "\(").replace(")", "\)")

        sort_keys_regex.append(QUERY_SORT_KEY_REGEX.format(sort_key, direction))

    # join sort keys regex by comma
    sort_keys_joined = ",\s*".join(sort_keys_regex)
    regex = QUERY_SORT_REGEX.format(sort_keys_joined)

    search = re.search(regex, query, re.IGNORECASE)
    return [search[0]] if search else []

def parse_limit(plan, query):
    regex = QUERY_LIMIT_REGEX.format(plan['Plan Rows'])

    search = re.search(regex, query, re.IGNORECASE)
    return [search[0]] if search else []


def parse_seq_scan(plan, query):
    regex = QUERY_SCAN_REGEX.format(plan['Relation Name'], plan['Alias'])
    if any(key in plan.keys() for key in ['Filter', 'Index Cond']):
        regex += "WHERE(.*\n)*(?<!\t)"
    search = re.search(regex, query, re.IGNORECASE)
    return [search[0]] if search else []



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

test = tests[-2]
test_case = test['Test Case']
query = test['Query']
execution_plan = test['Execution Plan']
print('Test Case: \n' + test_case + '\n')
print('Input Query: \n' + query + '\n')
print('Formatted Query: \n' + format(query) + '\n')

e,q = analyze(execution_plan, query)
e['Longest Duration']
