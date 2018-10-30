import psycopg2
import json
# from queries import *

import re

connection = "dbname='dblpDB_quarter' user='anqitu' host='localhost' password='dbpass'"

def print_test_query(query):
    [print(line.strip().upper()) for line in query.split('\n')]

def read_json(file_path):
    with open(file_path, 'r') as f:
        content = json.load(f)
    return content

def get_qep(query, connection = connection):
    try:
        conn = psycopg2.connect(connection)
    except:
        print('Unable to connect to the database')

    cur = conn.cursor()

    try:
        cur.execute(query)
    except Exception as e:
        msg = type(e).__name__
        return None, msg

    try:
        qep = cur.fetchall()
    except Exception as e:
        msg = type(e).__name__
        return None, msg
    cur.close()
    conn.close()

    return qep, 'No Error'


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

def get_query_components(plan, query):

    query_components = []

    if plan['Node Type'] == 'Seq Scan':
        query_components = parse_seq_scan(plan, query)
    elif plan['Node Type'] == 'Index Only Scan' or plan['Node Type'] == 'Bitmap Index Scan' or plan['Node Type'] == 'Bitmap Heap Scan':
        query_components = parse_index_only_scan(plan, query)
    elif plan['Node Type'] == 'Index Scan':
        query_components = parse_index_scan(plan, query)
    elif plan['Node Type'] == 'Hash Join':
        query_components = parse_hash_join(plan, query)
    elif plan['Node Type'] == 'Merge Join':
        query_components = parse_merge_join(plan, query)
    elif plan['Node Type'] == 'Aggregate':
        query_components = parse_aggregate(plan, query)
    elif plan['Node Type'] == 'Sort':
        query_components = parse_sort(plan, query)
    elif plan['Node Type'] == 'Limit':
        query_components = parse_limit(plan, query)
    elif plan['Node Type'] == 'Unique':
        query_components = parse_unique(plan, query)
    else:
        query_components = parse_general(plan, query)

    return query_components

def get_node_description(plan):
    node_descriptions = {
    'Seq Scan': '',
    'Index Scan': '',
    'Hash Join': '',
    'Aggregate': '',
    'Gather': '',
    }

    if plan['Node Type'] in node_descriptions:
        description = node_descriptions[plan['Node Type']]
    else:
        description = ''

    return description

def parse(plan, query):
    plan['Query'] = get_query_components(plan, query)
    plan['Actual Duration'] = calculate_actual_duration(plan)
    plan['Description'] = get_node_description(plan)

    if 'Plans' in plan.keys():
        for sub_plan in plan['Plans']:
            parse(sub_plan, query)

def parse_general(plan, query):
    node_type = plan['Node Type']
    return [node_type.upper()]

# Scans ------------------------------------------------------------------------
def process_filter(filter, query):
    elements = filter.split(' ')
    filters = []

    for i in range(0, len(elements), 4):
        left = elements[i].split('::')[0].replace('(', '').replace(')', '').replace('*', '(*)').upper()
        right = elements[i + 2].split('::')[0].replace('(', '').replace(')', '').replace('*', '(*)').upper()
        sign = elements[i + 1]

        if sign == '~~':
            sign = 'LIKE'

        filters.append(' '.join([left, sign, right]))

        if left in aliases.keys():
            filters.append(' '.join([aliases[left], sign, right]))

    return filters

def process_index_cond(cond, query):
    # remove extra information to match the exact query component
    # Eg. 'Index Cond': "(pubmonth.pubkey = 'books/aw/AhoHU83'::text)"
    index_cond = cond.replace('::text', '').replace('(', '').replace(')', '').upper()

    # split the conditions connencted by 'OR' or 'AND'
    # Eg. 'Filter': '((publisher.publisherid < 10) OR (publisher.publisherid > 100))'
    index_conds = index_cond.replace(' OR ', ' AND ').split(' AND ')

    # remove the whitespace atteched to the condition
    conds = []
    for cond in index_conds:
        left = cond.split('=')[0].strip()
        right = cond.split('=')[1].strip()
        conds.append(' '.join([left, '=', right]))
        conds.append(' '.join([right, '=', left]))

    return conds


def parse_seq_scan(plan, query):
    query_matches = []
    if plan['Node Type'] == 'Seq Scan':
        if 'Filter' in plan.keys():
            filters = process_filter(plan['Filter'], query)
            query_matches = query_matches + filters

        return query_matches
    else:
        print('Not Seq Scan.')

def parse_index_only_scan(plan, query):
    query_matches = []
    if plan['Node Type'] == 'Index Only Scan' or plan['Node Type'] == 'Bitmap Index Scan' or plan['Node Type'] == 'Bitmap Heap Scan':

        if 'Index Cond' in plan.keys():
            index_conds = process_index_cond(plan['Index Cond'], query)
            query_matches = query_matches + index_conds

        if 'Recheck Cond' in plan.keys():
            index_conds = process_index_cond(plan['Recheck Cond'], query)
            query_matches = query_matches + index_conds

        return query_matches
    else:
        print('Not Index Only Scan.')

# A combination of Seq Scan and Index Only Scan
def parse_index_scan(plan, query):
    query_matches = []
    if plan['Node Type'] == 'Index Scan':
        if 'Filter' in plan.keys():
            filters = process_filter(plan['Filter'], query)
            query_matches = query_matches + filters

        if 'Index Cond' in plan.keys():
            index_conds = process_index_cond(plan['Index Cond'], query)
            query_matches = query_matches + index_conds
        return query_matches
    else:
        print('Not Index Scan.')

# Joins ------------------------------------------------------------------------
def process_join_cond(cond, query):
    conds = []
    cond = cond.replace('::text', '').replace('(', '').replace(')', '').upper()
    # Address the reording of the attributes on the left and right of the '=' sign
    left = cond.split('=')[0].strip()
    right = cond.split('=')[1].strip()
    conds.append(' '.join([left, '=', right]))
    conds.append(' '.join([right, '=', left]))
    return conds


def parse_hash_join(plan, query):
    query_matches = []
    if plan['Node Type'] == 'Hash Join':
        if 'Hash Cond' in plan.keys():
            conds = process_join_cond(plan['Hash Cond'], query)
            query_matches = query_matches + conds
        return query_matches
    else:
        print('Not Hash Join.')

def parse_merge_join(plan, query):
    query_matches = []
    if plan['Node Type'] == 'Merge Join':
        if 'Merge Cond' in plan.keys():
            conds = process_join_cond(plan['Merge Cond'], query)
            query_matches = query_matches + conds
        return query_matches
    else:
        print('Not Merge Join.')


def parse_aggregate(plan, query):
    query_matches = []
    if plan['Node Type'] == 'Aggregate':
        if 'Group Key' in plan.keys():
            group_key = ', '.join(plan['Group Key'])
            # Two cases of using Aggregate:
            # One is 'Group By'
            query_match = ' '.join(['GROUP BY', group_key])
            query_matches.append(query_match.upper())
            # Another is 'DISTINCT'
            query_match = ' '.join(['DISTINCT', group_key])
            query_matches.append(query_match.upper())
        return query_matches
    else:
        print('Not Aggregate.')

def parse_sort(plan, query):
    query_matches = []
    if plan['Node Type'] == 'Sort':
        if 'Sort Key' in plan.keys():
            sort_key = sort_keys = ', '.join(plan['Sort Key']).replace('(', '').replace(')', '').replace('*', '(*)')
            query_match = ' '.join(['ORDER BY', sort_key])
            query_matches.append(query_match.upper())
        return query_matches
    else:
        print('Not Sort.')

def parse_limit(plan, query):
    query_matches = []
    if plan['Node Type'] == 'Limit':
        query_match = re.findall('LIMIT \d+', query)[0]
        query_matches.append(query_match.upper())
        return query_matches
    else:
        print('Not Limit.')

def parse_unique(plan, query):
    query_matches = []
    if plan['Node Type'] == 'Unique':
        query_matches.append('DISTINCT')
        return query_matches
    else:
        print('Not Unique.')


def find_aliases(query):
    aliases = re.findall(r'[^\s]+\sAS\s[^\s,]+', query)
    aliases_dict = {alias.split(' AS ')[0]: alias.split(' AS ')[1] for alias in aliases}
    return aliases_dict

aliases = find_aliases(query)

def get_test_query(node_type):
    if node_type in queries:
        return queries[node_type]
    else:
        return None

queries = {
    "Seq Scan": """
                SELECT PUBLICATION.pubTitle FROM PUBLICATION
                WHERE PUBLICATION.pubYear >= 2000
                AND PUBLICATION.pubYear <= 2017
                """,

    "Index Only Scan (text)": """
                      SELECT PUBMONTH.pubKey FROM PUBMONTH
                      WHERE PUBMONTH.pubKey = 'books/aw/AhoHU83'
                      """,

    "Index Only Scan (number)": """
                       SELECT PUBLISHER.publisherid FROM PUBLISHER
                       WHERE PUBLISHER.publisherid < 10
                       OR PUBLISHER.publisherid > 100
                       """,

    "Index Scan": """
                  SELECT PUBLICATION.pubTitle FROM PUBLICATION
                  WHERE PUBLICATION.pubKey = 'books/aw/AhoHU83'
                  AND PUBLICATION.pubYear <= 2017
                  """,

    "Bitmap Index Scan": """
                       SELECT PUBLICATION.pubKey FROM PUBLICATION
                       WHERE PUBLICATION.pubKey = 'books/aw/AhoHU83'
                       OR PUBLICATION.pubKey = 'books/aw/ArnoldG99'
                       """,
    "Bitmap Heap Scan": """
                      SELECT PUBLICATION.pubKey FROM PUBLICATION
                      WHERE PUBLICATION.pubKey = 'books/aw/AhoHU83'
                      OR PUBLICATION.pubKey = 'books/aw/ArnoldG99'
                      """,

    "Hash Join": """
                  SELECT PROCEEDING.proceedbooktitle, INPROCEEDING.inprobooktitle FROM INPROCEEDING, PROCEEDING
                  WHERE PROCEEDING.pubKey = INPROCEEDING.inproCrossRef;
                  """,

    "Aggregate": """
            SELECT PUBLICATION.pubType, PUBLICATION.pubYear, COUNT(*) FROM PUBLICATION
            GROUP BY PUBLICATION.pubType, PUBLICATION.pubYear;
            """,
    "Sort": """
            SELECT PUBLICATION.pubYear, PUBLICATION.pubType, COUNT(*) FROM PUBLICATION
            GROUP BY PUBLICATION.pubType, PUBLICATION.pubYear
            ORDER BY COUNT(*) DESC, PUBLICATION.pubYear;
            """,

    "Unique": """
            SELECT DISTINCT PUBLICATION.pubKey
            FROM PUBLICATION;
            """,
}


query = """
        SELECT DISTINCT PUBLICATION.pubYear
        FROM PUBLICATION;
        """

query = get_test_query('Seq Scan')
query = get_test_query('Index Only Scan (text)')
query = get_test_query('Index Only Scan (number)')
query = get_test_query('Index Scan')
query = get_test_query('Bitmap Index Scan')
query = get_test_query('Bitmap Heap Scan')
query = get_test_query('Hash Join')
query = get_test_query('Aggregate')
query = get_test_query('Sort')
print_test_query(query)

qep, msg = get_qep('EXPLAIN (ANALYZE, VERBOSE)' + query)
# qep, msg = get_qep(query)
# qep, msg = get_qep('EXPLAIN (FORMAT JSON)' + query)

# Get QEP from postgre
qep, msg = get_qep('EXPLAIN (ANALYZE, COSTS, VERBOSE, FORMAT JSON)' + query)
plan = qep[0][0][0]['Plan']

# Get QEP from json file
# qep = read_qep_from_json('tests/seq_scan.json')
# plan = qep[0]['Plan']

parse(plan, query)
qep
plan
