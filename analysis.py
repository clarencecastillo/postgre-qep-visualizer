from utils import format, read_json, parse_nested
import itertools
import re

QUERY_LIMIT_REGEX = r"\bLIMIT\s+{0}"
QUERY_SORT_REGEX = r"ORDER BY\s+{0}"
QUERY_SORT_KEY_REGEX = r"{0}(\s+{1})?"
QUERY_SCAN_REGEX = r"{0}(\s+{1})?"
QUERY_SCAN_RELATION_REGEX = r"(?<=[\s,]){0}(\s+{1})?"
QUERY_CLAUSE_REGEX = r'\n({0}\s.*\n?(\t+.*\n?)*)(?<!\t)'

FILTERS_PAREN_REGEX = r"\(([\%\w\.\/]+)\)::[a-z]+"
FILTERS_QUOTE_REGEX = r"'([\%\w\.\/]+)'::[a-z]+"

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

    root_plan = execution_plan['Plan']
    analyze_plan(root_plan, formatted_query)

    execution_plan['Longest Duration'] = get_longest_duration(root_plan)
    find_slowest_node(root_plan, execution_plan['Longest Duration'])

    execution_plan['Highest Cost'] = get_longest_duration(root_plan)
    find_costliest_node(root_plan, execution_plan['Highest Cost'])

    execution_plan['Greatest Errors'] = get_greatest_errors(root_plan)
    find_greatest_errors_node(root_plan, execution_plan['Greatest Errors'])

    execution_plan['Largest Size'] = get_largest_nodes(root_plan)
    find_largest_node(root_plan, execution_plan['Largest Size'])

    return execution_plan, formatted_query


def analyze_plan(plan, query):
    if 'Plans' in plan.keys():
        for sub_plan in plan['Plans']:
            analyze_plan(sub_plan, query)

    plan['Query'] = get_query_components(plan, query)
    plan['Actual Duration'] = calculate_actual_duration(plan)
    plan['Actual Cost'] = calculate_actual_cost(plan)
    plan['Estimate Errors'] = calculate_estimate_errors(plan)
    plan['Description'] = get_node_description(plan)

def get_query_components(plan, query):

    query_components = []

    # Limit: Keyword Limit with number
    if plan['Node Type'] == 'Limit':
        query_components = parse_limit(plan, query)

    # Sort: Sort key, Or Inherits the query from its parent (If the node of ’Sort’ is a child of another node with strategy ‘sorter’)
    elif plan['Node Type'] == 'Sort':
        query_components = parse_sort(plan, query)

    # Scan: scanned table, filter condition, index condition, recheck condition (deal with '::text', 'LIKE' --> '~~')
    elif plan['Node Type'] in ['Seq Scan', 'CTE Scan']:
        query_components = parse_scan(plan, query, ['Filter'])

    elif plan['Node Type'] in ['Index Scan', 'Index Only Scan', 'Bitmap Index Scan']:
        query_components = parse_scan(plan, query, ['Filter', 'Index Cond'])

    elif plan['Node Type'] == 'Bitmap Heap Scan':
        query_components = parse_scan(plan, query, ['Recheck Cond'])

    # Hash Join: join condition (might reverse order)
    elif plan['Node Type'] == 'Hash Join':
        query_components = parse_scan(plan, query, ['Hash Cond'])

    # Merge Join: join condition (might reverse order)
    elif plan['Node Type'] == 'Merge Join':
        query_components = parse_scan(plan, query, ['Merge Cond'])

    # Nested Loop: query components from its child nodes
    elif plan['Node Type'] == 'Nested Loop':
        query_components = parse_nested_loop(plan, query)

    # Aggregates: Group Key
    elif plan['Node Type'] in ['Aggregate', 'GroupAggregate', 'HashAggregate']:
        query_components = parse_aggregate(plan, query)

    # Hash: Hashed table (take from child nodes)
    elif plan['Node Type'] == 'Hash':
        query_components = parse_hash(plan, query)

    #Gathers: gathered columns
    elif plan['Node Type'] in ['Gather', 'Gather Merge', 'BitmapOr']:
        query_components = parse_synthesised(plan, query)

    # Unique: Keyword DISTINCT with column(s)
    elif plan['Node Type'] == 'Unique':
        query_components = parse_unique(plan, query)

    else:
        query_components = parse_general(plan, query)

    return query_components

def parse_aggregate(plan, query):
    group_by_clause = next(re.finditer(QUERY_CLAUSE_REGEX.format('GROUP BY'), query))
    return [find_matching_query(group_by_clause.group(), query)]

def parse_unique(plan, query):
    distinct_on = find_matching_query(QUERY_DISTINCT_ON_REGEX, query)
    if distinct_on:
        return [distinct_on]

    return [find_matching_query(QUERY_CLAUSE_REGEX.format('DISTINCT'), query)]

def parse_synthesised(plan, query):
    if 'Plans' not in plan:
        return []

    query_components = []

    for sub_plan in plan['Plans']:
        sub_plan_query = sub_plan['Query']
        for query in sub_plan_query:
            if query not in query_components:
                query_components.append(query)

    return query_components

def parse_sort(plan, query):

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
    return [find_matching_query(regex, query)]

def parse_limit(plan, query):
    regex = QUERY_LIMIT_REGEX.format(plan['Plan Rows'])
    return [find_matching_query(regex, query)]

def parse_scan(plan, query, keys):
    query_components = []

    if 'FROM' in query and all(key in plan for key in ['Relation Name', 'Alias']):
            relation_regex = QUERY_SCAN_RELATION_REGEX.format(plan['Relation Name'], plan['Alias'])
            from_clause = next(re.finditer(QUERY_CLAUSE_REGEX.format('FROM'), query))
            relation_component = find_matching_query(relation_regex, from_clause.group(), from_clause.start())
            query_components.append(relation_component)

    if 'WHERE' in query:
        where_clause = next(re.finditer(QUERY_CLAUSE_REGEX.format('WHERE'), query))
        conditions = []
        for key in keys:
            if key in plan:
                key_conditions = parse_filters(plan[key])
                conditions = conditions + key_conditions

        for condition in conditions:
            condition_regex = re.sub(r"\w+\.", r"(\\w+\.)?", condition)
            condition_component = find_matching_query(condition_regex, where_clause.group(), where_clause.start())
            if condition_component:
                query_components.append(condition_component)
    return query_components

def parse_nested_loop(plan, query):
    query_components = []
    for subplan in plan['Plans']:
        query_components = query_components + subplan['Query']
    return query_components

def parse_hash(plan, query):
    query_components = []
    for subplan in plan['Plans']:
        query_components = query_components + subplan['Query']
    return query_components

def parse_general(plan, query):
    return [plan['Node Type']]

def find_matching_query(regex, query, offset=0):
    search = re.finditer(regex, query, re.IGNORECASE)
    result = next(search, None)
    if result is not None:
        return {
            'start': offset + result.start(),
            'end': offset + result.end(),
            'match': result.group().upper()
        }

def extract_conditions(layers):
    conditions = []
    if len(layers) == 1:
        condition = layers[0]
        if isinstance(condition, str):
            conditions = [condition]
            # adds symmetric representation of the condition i.e. a == b
            if ('=' in condition):
                symmetric = ' '.join(condition.split(' ')[::-1])
                conditions.append(symmetric)
            return conditions
        else:
            return [extract_conditions(condition)]
    for layer in layers:
        if isinstance(layer, list):
            conditions = conditions + extract_conditions(layer)
    return conditions

def parse_filters(filters):
    # parse {0} ~~ {1} -> {0} LIKE {1}
    filters = filters.replace("~~", "LIKE")

    # parse ({0})::{1} -> {0}
    filters = re.sub(FILTERS_PAREN_REGEX, "\g<1>", filters)

    # parse '{0}'::{1} -> '{0}'
    filters = re.sub(FILTERS_QUOTE_REGEX, "'\g<1>'",filters)

    return extract_conditions(parse_nested(filters))

def get_node_description(plan):

    description = ''
    if plan['Node Type'] in NODE_DESCRIPTIONS:
        description = NODE_DESCRIPTIONS[plan['Node Type']]

    return description

def calculate_actual_duration(plan):
    actual_duration = plan['Actual Total Time']
    if 'Plans' in plan.keys():
        for sub_plan in plan['Plans']:
            # since CTE scan duration is already included in its subnodes,
            # it should be be subtracted from the duration of this node
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

def find_slowest_node(plan, longest_duration):
    plan['Is Slowest'] = False
    if plan['Actual Duration'] == longest_duration:
        plan['Is Slowest'] = True

    if 'Plans' in plan:
        for sub_plan in plan['Plans']:
            find_slowest_node(sub_plan, longest_duration)

def calculate_actual_cost(plan):
    actual_cost = plan['Total Cost']
    if 'Plans' in plan.keys():
        for sub_plan in plan['Plans']:
            # since CTE scan duration is already included in its subnodes,
            # it should be be subtracted from the duration of this node
            if (sub_plan['Node Type'] != 'CTE_Scan'):
                actual_cost -= sub_plan['Total Cost'];

    # time is reported for an invidual loop
    # actual duration must be adjusted by number of loops
    actual_cost = actual_cost * plan['Actual Loops'];
    return actual_cost;

def get_highest_cost(plan):
    cost = plan['Actual Cost']

    if 'Plans' in plan:
        for sub_plan in plan['Plans']:
            cost = max(cost, get_highest_cost(sub_plan))
        return cost
    return cost

def find_costliest_node(plan, highest_cost):
    plan['Is Costliest'] = False
    if plan['Actual Cost'] == highest_cost:
        plan['Is Costliest'] = True

    if 'Plans' in plan:
        for sub_plan in plan['Plans']:
            find_costliest_node(sub_plan, highest_cost)

def calculate_estimate_errors(plan):
    return abs(plan['Actual Rows'] - plan['Plan Rows'])

def get_greatest_errors(plan):
    error = plan['Estimate Errors']

    if 'Plans' in plan:
        for sub_plan in plan['Plans']:
            error = max(error, get_greatest_errors(sub_plan))
        return error
    return error

def find_greatest_errors_node(plan, greatest_errors):
    plan['Has Greatest Errors'] = False
    if plan['Estimate Errors'] == greatest_errors:
        plan['Has Greatest Errors'] = True

    if 'Plans' in plan:
        for sub_plan in plan['Plans']:
            find_greatest_errors_node(sub_plan, greatest_errors)

def get_largest_nodes(plan):
    size = plan['Actual Rows']

    if 'Plans' in plan:
        for sub_plan in plan['Plans']:
            size = max(size, get_largest_nodes(sub_plan))
        return size
    return size

def find_largest_node(plan, largest_size):
    plan['Is Largest'] = False
    if plan['Actual Rows'] == largest_size:
        plan['Is Largest'] = True

    if 'Plans' in plan:
        for sub_plan in plan['Plans']:
            find_largest_node(sub_plan, largest_size)

# tests = read_json('tests.json')
# test = tests[0]
# execution_plan = test['Execution Plan']
# query = format(test['Query'])
# e, q = analyze(execution_plan, query)
# e['Plan']['Query']
