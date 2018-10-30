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

def get_query_components(plan):

    query_components = []

    if plan['Node Type'] == 'Seq Scan':
        query_components = parse_seq_scan(plan)
    elif plan['Node Type'] == 'Index Only Scan' or plan['Node Type'] == 'Bitmap Index Scan' or plan['Node Type'] == 'Bitmap Heap Scan':
        query_components = parse_index_only_scan(plan)
    elif plan['Node Type'] == 'Index Scan':
        query_components = parse_index_scan(plan)
    elif plan['Node Type'] == 'Hash Join' or plan['Node Type'] == 'Merge Join':
        query_components = parse_join(plan)
    elif plan['Node Type'] == 'Aggregate':
        query_components = parse_aggregate(plan)
    elif plan['Node Type'] == 'Gather':
        query_components = parse_general(plan)
    else:
        query_components = parse_general(plan)

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

def parse(plan):
    plan['Query'] = get_query_components(plan)
    plan['Actual Duration'] = calculate_actual_duration(plan)
    plan['Description'] = get_node_description(plan)

    if 'Plans' in plan.keys():
        for sub_plan in plan['Plans']:
            parse(sub_plan)

def parse_general(plan):
    node_type = plan['Node Type']
    return [node_type.upper()]

# Scans ------------------------------------------------------------------------
def process_filter(filter):
    elements = filter.split(' ')
    filters = []

    for i in range(0, len(elements), 4):
        left = elements[i].split('::')[0].replace('(', '').replace(')', '').replace('*', '(*)').upper()
        right = elements[i + 2].split('::')[0].replace('(', '').replace(')', '').replace('*', '(*)').upper()
        sign = elements[i + 1]

        if sign == '~~':
            sign = 'LIKE'

        filters.append(' '.join([left, sign, right]))

        if sign == '=':
            filters.append(' '.join([right, sign, left]))

        if left in aliases.keys():
            filters.append(' '.join([aliases[left], sign, right]))

    return filters

def process_index_cond(cond):
    # remove extra information to match the exact query component
    # Eg. 'Index Cond': "(pubmonth.pubkey = 'books/aw/AhoHU83'::text)"
    index_cond = cond.replace('::text', '').replace('(', '').replace(')', '').upper()

    # split the conditions connencted by 'OR' or 'AND'
    # Eg. 'Filter': '((publisher.publisherid < 10) OR (publisher.publisherid > 100))'
    index_conds = index_cond.replace('OR', 'AND').split('AND')

    # remove the whitespace atteched to the condition
    index_conds = [cond.strip() for cond in index_conds]

    return index_conds


def parse_seq_scan(plan):
    query_matches = []
    if plan['Node Type'] == 'Seq Scan':
        if 'Filter' in plan.keys():
            filters = process_filter(plan['Filter'])
            query_matches = query_matches + filters

        return query_matches
    else:
        print('Not Seq Scan.')

def parse_index_only_scan(plan):
    query_matches = []
    if plan['Node Type'] == 'Index Scan' or plan['Node Type'] == 'Bitmap Index Scan' or plan['Node Type'] == 'Bitmap Heap Scan':

        if 'Index Cond' in plan.keys():
            index_conds = process_index_cond(plan['Index Cond'])
            query_matches = query_matches + index_conds

        if 'Recheck Cond' in plan.keys():
            index_conds = process_index_cond(plan['Recheck Cond'])
            query_matches = query_matches + index_conds

        return query_matches
    else:
        print('Not Index Only Scan.')

# A combination of Seq Scan and Index Only Scan
def parse_index_scan(plan):
    query_matches = []
    if plan['Node Type'] == 'Index Scan':
        if 'Filter' in plan.keys():
            filters = process_filter(plan['Filter'])
            query_matches = query_matches + filters

        if 'Index Cond' in plan.keys():
            index_conds = process_index_cond(plan['Index Cond'])
            query_matches = query_matches + index_conds
        return query_matches
    else:
        print('Not Index Scan.')

# Joins ------------------------------------------------------------------------
def process_join_cond(cond):
    conds = []
    cond = cond.replace('::text', '').replace('(', '').replace(')', '').upper()
    # Address the reording of the attributes on the left and right of the '=' sign
    left = cond.split('=')[0].strip()
    right = cond.split('=')[1].strip()
    conds.append(' '.join([left, '=', right]))
    conds.append(' '.join([right, '=', left]))
    return conds


def parse_join(plan):
    query_matches = []
    if plan['Node Type'] == 'Hash Join' or plan['Node Type'] == 'Merge Join':
        if 'Hash Cond' in plan.keys():
            conds = process_join_cond(plan['Hash Cond'])
            query_matches = query_matches + conds
        return query_matches
    else:
        print('Not Join.')

# Aggregate --------------------------------------------------------------------
def parse_aggregate(plan):
    query_matches = []
    if plan['Node Type'] == 'Aggregate':
        if 'Group Key' in plan.keys():
            group_key = ', '.join(plan['Group Key'])
            query_match = ' '.join(['GROUP BY', group_key])
            query_matches.append(query_match.upper())
        return query_matches
    else:
        print('Not Aggregate.')

def parse_gather(plan):
    query_matches = []
    if plan['Node Type'] == 'Gather':
        if 'Output' in plan.keys():
            outputs = ', '.join(plan['Output']).replace('PARTIAL ', '').replace('::text', '')
            query_match = ' '.join([outputs])
            query_matches.append(query_match.upper())
        return query_matches
    else:
        print('Not Index Scan.')




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
            WHERE PUBLICATION.pubYear >= 2000 AND PUBLICATION.pubYear <= 2017
            GROUP BY PUBLICATION.pubType, PUBLICATION.pubYear;
            """,
}

query = """
        SELECT PUBLICATION.pubType, COUNT(*) FROM PUBLICATION
        WHERE PUBLICATION.pubYear >= 2000 AND PUBLICATION.pubYear <= 2017
        GROUP BY PUBLICATION.pubType;
        """

query = """
        SELECT PUBLICATION.pubType, PUBLICATION.pubYear, COUNT(*) FROM PUBLICATION
        WHERE PUBLICATION.pubYear >= 2000 AND PUBLICATION.pubYear <= 2017
        GROUP BY PUBLICATION.pubType, PUBLICATION.pubYear;
        """

query = get_test_query('Seq Scan')
query = get_test_query('Index Only Scan (text)')
query = get_test_query('Index Only Scan (number)')
query = get_test_query('Index Scan')
query = get_test_query('Bitmap Index Scan')
query = get_test_query('Bitmap Heap Scan')
query = get_test_query('Hash Join')
query = get_test_query('Aggregate')
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

parse(plan)
qep
plan







# import sqlparse
#
# parsed = sqlparse.parse(query)
# stmt = parsed[0]
#
# for token in stmt.tokens:
#     print(type(token), token.ttype, token.value)
#
# identifier_list = stmt.tokens[4]
# for identifier in identifier_list.get_identifiers():
#     print(type(identifier), identifier.ttype, identifier.value)
#
# for identifier in identifier_list.get_identifiers():
#     print(type(identifier), identifier.ttype, identifier.value, identifier.get_real_name())
#
# where = stmt.tokens[-1]
# for token in where.tokens:
#     print(type(token), token.ttype, token.value)
#
# for id, item in enumerate(where.flatten()):
#     print(id, item.value)
#
# for item in stmt.flatten():
#     print(item.ttype)
#     print(item.parent.parent)
#     if item.ttype is sqlparse.tokens.Keyword.DML and item.value.upper() == 'SELECT':
#         print(item.parent)



# Startup cost is a tricky concept. It doesn't just represent the amount of time before that component starts. It represents the amount of time between when the component starts executing (reading in data) and when the component outputs its first row.
# Total cost is the entire execution time of the component, from when it begins reading in data to when it finishes writing its output.




# query = """
#         SELECT PUBLICATION.pubType, COUNT(*) FROM PUBLICATION
#         WHERE PUBLICATION.pubYear >= 2000 AND PUBLICATION.pubYear <= 2017
#         GROUP BY PUBLICATION.pubType;
#         """
