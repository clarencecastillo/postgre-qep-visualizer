from utils import format, read_json, parse_nested
import re

QUERY_LIMIT_REGEX = r"\bLIMIT\s+{0}"
QUERY_SORT_REGEX = r"ORDER BY\s+{0}"
QUERY_SORT_KEY_REGEX = r"{0}(\s+{1})?"
QUERY_SCAN_REGEX = r"{0}(\s+{1})?"
QUERY_SCAN_RELATION_REGEX = r"{0}(\s+{1})?"
QUERY_CLAUSE_REGEX = r'\n({0}.*\n(\t+.*\n)+)(?<!\t)'

FILTERS_PAREN_REGEX = r"\(([\w\.]+)\)::[a-z]+"
FILTERS_QUOTE_REGEX = r"'([\w\.]+)'::[a-z]+"

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
    find_slowest_node(execution_plan['Plan'], execution_plan['Longest Duration'])
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

def find_slowest_node(plan, longest_duration):
    plan['Is Slowest'] = False
    if plan['Actual Duration'] == longest_duration:
        plan['Is Slowest'] = True

    if 'Plans' in plan:
        for sub_plan in plan['Plans']:
            find_slowest_node(sub_plan, longest_duration)

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

    elif plan['Node Type'] in ['Seq Scan', 'Index Scan', 'Index Only Scan', 'Bitmap Index Scan', 'Bitmap Heap Scan']:
        query_components = parse_scan(plan, query)

    # elif plan['Node Type'] == 'Hash Join':
    #     query_components = parse_hash_join(plan, query)
    # elif plan['Node Type'] == 'Merge Join':
    #     query_components = parse_merge_join(plan, query)
    # elif plan['Node Type'] == 'Aggregate':
    #     query_components = parse_aggregate(plan, query)
    # elif plan['Node Type'] == 'Unique':
    #     query_components = parse_unique(plan, query)
    # else:
    #     query_components = parse_general(plan, query)

    return [c.upper() for c in query_components]

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

def parse_scan(plan, query):
    query_components = []
    if all(key in plan for key in ['Relation Name', 'Alias']):
        relation_regex = QUERY_SCAN_RELATION_REGEX.format(plan['Relation Name'], plan['Alias'])
        from_clause = next(re.finditer(QUERY_CLAUSE_REGEX.format('FROM'), query))
        relation_component = find_matching_query(relation_regex, from_clause.group(), from_clause.start())
        query_components.append(relation_component)

    where_clause = next(re.finditer(QUERY_CLAUSE_REGEX.format('WHERE'), query))
    conditions = []
    for key in ['Filter', 'Index Cond']:
        if key in plan:
            key_conditions = parse_filters(plan[key])
            conditions = conditions + key_conditions

    for condition in conditions:
        condition_regex = re.sub(r"\w+\.", r"(\\w+)?", condition)
        condition_component = find_matching_query(condition_regex, where_clause.group(), where_clause.start())
        if condition_component:
            query_components.append(condition_component)
    return query_components

def find_matching_query(regex, query, offset=0):
    search = re.finditer(regex, query, re.IGNORECASE)
    result = next(search, None)
    if result is not None:
        return {
            'start': offset + result.start(),
            'end': offset + result.end(),
            'match': result.group()
        }

def extract_conditions(layers):
    conditions = []
    if len(layers) == 1:
        return [layers[0]] if isinstance(layers[0], str) else [extract_conditions(layers[0])]
    for layer in layers:
        if isinstance(layer, list):
            conditions = conditions + extract_conditions(layer)
    return conditions

def parse_filters(filters):
    filters = re.sub(FILTERS_PAREN_REGEX, "\g<1>", filters)
    filters = re.sub(FILTERS_QUOTE_REGEX, "'\g<1>'",filters)
    return extract_conditions(parse_nested(filters))
