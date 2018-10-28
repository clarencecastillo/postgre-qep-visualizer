import psycopg2
import json

connection = "dbname='dblpDB_quarter' user='anqitu' host='localhost' password='dbpass'"

def read_qep_from_json(file_path):
    with open('test.json', 'r') as f:
        qep = json.load(f)
    return qep

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

def get_query_component(plan):
    query_component = []
    if plan['Node Type'] == 'Seq Scan':
        query_component = parse_seq_scan(plan)
    elif plan['Node Type'] == 'Index Scan' or plan['Node Type'] == 'Index Only Scan':
        query_component = parse_index_scan(plan)
    elif plan['Node Type'] == 'Hash Join':
        query_component = parse_hash_join(plan)
    elif plan['Node Type'] == 'Aggregate':
        query_component = parse_aggregate(plan)
    elif plan['Node Type'] == 'Gather':
        query_component = parse_gather(plan)
    else:
        query_component = parse_general(plan)

    return query_component

def parse(plan):
    plan['Query'] = get_query_component(plan)
    plan['Actual Duration'] = calculate_actual_duration(plan)

    if 'Plans' in plan.keys():
        for sub_plan in plan['Plans']:
            parse(sub_plan)



def parse_general(plan):
    node_type = plan['Node Type']
    return [node_type.upper()]

def parse_aggregate(plan):
    query_matches = []
    if plan['Node Type'] == 'Aggregate':
        group_key = ', '.join(plan['Group Key'])
        query_match = ' '.join(['GROUP BY', group_key])
        query_matches.append(query_match.upper())
        if 'Filter' in plan.keys():
            filters = process_filter(plan['Filter'])
            query_matches = query_matches + filters

            # filter = plan['Filter'].replace('::text', '').replace('(', '').replace(')', '').replace('~~', 'LIKE').replace('*', '(*)')
            # query_matches.append(filter.upper())

        if 'Output' in plan.keys():
            outputs = ', '.join(plan['Output']).replace('PARTIAL ', '').replace('::text', '')
            query_match = ' '.join([outputs])
            query_matches.append(query_match.upper())

        return query_matches
    else:
        print('Not Aggregate.')

def parse_seq_scan(plan):
    query_matches = []
    if plan['Node Type'] == 'Seq Scan':
        if 'Filter' in plan.keys():
            filters = process_filter(plan['Filter'])
            query_matches = query_matches + filters

            # filter = plan['Filter'].replace('::text', '').replace('(', '').replace(')', '').replace('~~', 'LIKE')
            # query_match = ' '.join([filter])
            # query_matches.append(query_match.upper())
        return query_matches
    else:
        print('Not Seq Scan.')

def parse_index_scan(plan):
    query_matches = []
    if plan['Node Type'] == 'Index Scan' or plan['Node Type'] == 'Index Only Scan':
        if 'Filter' in plan.keys():
            filters = process_filter(plan['Filter'])
            query_matches = query_matches + filters

            # filter = plan['Filter'].replace('::text', '').replace('(', '').replace(')', '')
            # relation_name = plan['Relation Name'].upper()
            # query_match = ' '.join([relation_name + '.' + filter])
            # query_matches.append(query_match.upper())
        if 'Index Cond' in plan.keys():
            filters = process_filter(plan['Index Cond'])
            query_matches = query_matches + filters

            # index_cond = plan['Index Cond'].replace('::text', '').replace('(', '').replace(')', '')
            # query_match = ' '.join([index_cond])
            # query_matches.append(query_match.upper())
        return query_matches
    else:
        print('Not Index Scan.')

def parse_hash_join(plan):
    query_matches = []
    if plan['Node Type'] == 'Hash Join':
        if 'Hash Cond' in plan.keys():
            filters = process_filter(plan['Hash Cond'])
            query_matches = query_matches + filters
        return query_matches
    else:
        print('Not Index Scan.')

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

def find_aliases(query):
    aliases = re.findall(r'[^\s]+\sAS\s[^\s,]+', query)
    aliases_dict = {alias.split(' AS ')[0]: alias.split(' AS ')[1] for alias in aliases}
    return aliases_dict

aliases = find_aliases(query)
aliases



query = """
        SELECT PUBLICATION.pubTitle
        FROM PUBLICATION, PROCEEDING
        WHERE PUBLICATION.pubKey = PROCEEDING.pubKey
        AND PROCEEDING.proceedType = 'conf'
        """
query = """
        SELECT PUBLICATION.pubType, COUNT(*) FROM PUBLICATION
        WHERE PUBLICATION.pubYear >= 2000 AND PUBLICATION.pubYear <= 2017
        GROUP BY PUBLICATION.pubType;
        """


# To makes it simpler to find the items we're looing for,
# Make the query upper case and ensuring that all comma separated values have a space
query = query.upper()

qep, msg = get_qep('EXPLAIN (ANALYZE, VERBOSE)' + query)
qep, msg = get_qep(query)
qep, msg = get_qep('EXPLAIN (FORMAT JSON)' + query)
qep, msg = get_qep('EXPLAIN (ANALYZE, COSTS, VERBOSE, FORMAT JSON)' + query)

plan = qep[0][0][0]['Plan']

qep = read_qep_from_json('test.json')
plan = qep[0]['Plan']
parse(plan)
qep




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

#
# {
#     'Plan':{'Mapping': "query",
#             'Plans': [{'Mapping': 'query',
#                        'Plans': [{'Mapping': 'query',
#                                   'Plans': [{'Mapping': 'query'}]}]},
#                       {'Mapping': 'query'}, ]
#     }
# }
#
# query_error = """
#         EXPLAIN ANALYZE \
#         SELECT pubType, COUNT(*) FROM PUBLICATION \
#         WHERE pubYear >= 2000 AND pubYear <= 2017 \
#         GROUP BY pubYear
#         """
# print_qep(query_error)


# query = """
#         SELECT PUBLICATION.pubTitle
#         FROM PUBLICATION, PROCEEDING
#         WHERE PUBLICATION.pubKey = PROCEEDING.pubKey
#         AND PROCEEDING.proceedType = 'conf'
#         """
