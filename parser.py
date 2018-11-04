from utils import format, read_json, parse_nested
import re

QUERY_LIMIT_REGEX = r"\bLIMIT\s+{0}"
QUERY_COL_RELATION_REGEX = r"\w+\."
QUERY_OPTIONAL_COL_RELATION_REGEX = r"(\\w+\.)?"
QUERY_SCAN_RELATION_REGEX = r"(?<=[\s,]){0}(\s+{1})?"
QUERY_CLAUSE_REGEX = r'(?<=[\s]){0}\s.*(\n\t+.*)*(?<!\t)'
QUERY_DISTINCT_ON_REGEX = "DISTINCT ON \(.*\)"

FILTERS_PAREN_REGEX = r"\(([\%\w\.\/]+)\)::[a-z]+"
FILTERS_QUOTE_REGEX = r"'([\%\w\.\/]+)'::[a-z]+"

def parse(execution_plan, query):
    formatted_query = format(query)

    root_plan = execution_plan['Plan']
    parse_plan(root_plan, formatted_query)

    return execution_plan, formatted_query

def parse_plan(plan, query):
    if 'Plans' in plan.keys():
        for sub_plan in plan['Plans']:
            parse_plan(sub_plan, query)

    plan['Query'] = get_query_components(plan, query)

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

        for sub_plan in plan['Plans']:
            if sub_plan['Node Type'] == 'Sort':
                sub_plan['Query'] = parse_merge_sort(sub_plan, query_components[0])

    # Aggregates: Group Key
    elif plan['Node Type'] in ['Aggregate', 'GroupAggregate', 'HashAggregate']:
        query_components = parse_aggregate(plan, query)

        if 'Strategy' in plan and plan['Strategy'] == 'Sorted':
            for sub_plan in plan['Plans']:
                if sub_plan['Node Type'] == 'Sort':
                    sub_plan['Query'] = [component for component in query_components if component['match'].startswith('GROUP BY')]

    #Gathers: gathered columns
    elif plan['Node Type'] in ['Gather', 'Gather Merge', 'BitmapOr', 'Nested Loop', 'Hash']:
        query_components = parse_synthesised(plan, query)

    # Unique: Keyword DISTINCT with column(s)
    elif plan['Node Type'] == 'Unique':
        query_components = parse_unique(plan, query)

    else:
        query_components = parse_general(plan, query)

    return query_components

def parse_merge_sort(plan, parent_query):
    sort_key = plan['Sort Key'][0]
    sort_key_regex = re.sub(QUERY_COL_RELATION_REGEX, QUERY_OPTIONAL_COL_RELATION_REGEX, sort_key)
    return find_matching_query(sort_key_regex, parent_query['match'], parent_query['start'])

def parse_aggregate(plan, query):
    group_by_clause = next(re.finditer(QUERY_CLAUSE_REGEX.format('GROUP BY'), query))
    query_components = find_matching_query(group_by_clause.group(), query)

    if 'Filter' in plan and 'HAVING' in query:
        query_components = query_components + find_matching_query(QUERY_CLAUSE_REGEX.format('HAVING'), query)

    return query_components

def parse_unique(plan, query):
    distinct_on = find_matching_query(QUERY_DISTINCT_ON_REGEX, query)
    if distinct_on:
        return distinct_on

    return find_matching_query(QUERY_CLAUSE_REGEX.format('DISTINCT'), query)

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

    if 'Sort Key' not in plan or 'ORDER BY' not in query:
        return []

    regex = QUERY_CLAUSE_REGEX.format('ORDER BY')
    return find_matching_query(regex, query)

def parse_limit(plan, query):
    regex = QUERY_LIMIT_REGEX.format(plan['Plan Rows'])
    return find_matching_query(regex, query)

def parse_scan(plan, query, keys):
    query_components = []

    if 'FROM' in query and all(key in plan for key in ['Relation Name', 'Alias']):
            relation_regex = QUERY_SCAN_RELATION_REGEX.format(plan['Relation Name'], plan['Alias'])
            from_clause = next(re.finditer(QUERY_CLAUSE_REGEX.format('FROM'), query))
            relation_component = find_matching_query(relation_regex, from_clause.group(), from_clause.start())
            query_components = query_components + relation_component

    if 'WHERE' in query:
        where_clause = next(re.finditer(QUERY_CLAUSE_REGEX.format('WHERE'), query))
        conditions = []
        for key in keys:
            if key in plan:
                key_conditions = parse_filters(plan[key])
                conditions = conditions + key_conditions

        conditions_regex = {re.sub(QUERY_COL_RELATION_REGEX, QUERY_OPTIONAL_COL_RELATION_REGEX, condition) for condition in conditions}
        for condition_regex in conditions_regex:
            condition_component = find_matching_query(condition_regex, where_clause.group(), where_clause.start())
            query_components = query_components + condition_component

    return query_components

def parse_general(plan, query):
    return [plan['Node Type']]

def find_matching_query(regex, query, offset=0):
    search = re.finditer(regex, query, re.IGNORECASE)
    result = next(search, None)
    if result is not None:
        return [{
            'start': offset + result.start(),
            'end': offset + result.end(),
            'match': result.group().upper()
        }]
    else:
        return []

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
