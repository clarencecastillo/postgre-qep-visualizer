import psycopg2

connection = "dbname='dblpDB_quarter' user='anqitu' host='localhost' password='dbpass'"

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

def parse(plan):
    map = {}
    query_match = None

    if plan['Node Type'] == 'Seq Scan':
        query_match = parse_seq_scan(plan)
    elif plan['Node Type'] == 'Index Scan':
        query_match = parse_index_scan(plan)
    elif plan['Node Type'] == 'Hash Join':
        query_match = parse_hash_join(plan)
    elif plan['Node Type'] == 'Aggregate':
        query_match = parse_aggregate(plan)
    elif plan['Node Type'] == 'Gather':
        query_match = parse_gather(plan)
    else:
        query_match = parse_general(plan)

    map['Mapping'] = query_match
    if 'Plans' in plan.keys():
        map['Plans'] = parse_sub_plans(plan)

    return map

def parse_sub_plans(plan):
    plans = []
    for sub_plan in plan['Plans']:
        plans.append(parse(sub_plan))
    return plans

def parse_general(plan):
    node_type = plan['Node Type']
    return [node_type.upper()]

def parse_aggregate(plan):
    query_matches = []
    if plan['Node Type'] == 'Aggregate':
        group_key = ', '.join(plan['Group Key'])
        query_match = ' '.join(['GROUP BY', group_key])
        query_matches.append(query_match.upper())
        return query_matches
    else:
        print('Not Aggregate.')

def parse_seq_scan(plan):
    query_matches = []
    if plan['Node Type'] == 'Seq Scan':
        if 'Filter' in plan.keys():
            filter = plan['Filter'].replace('::text', '').replace('(', '').replace(')', '')
            query_match = ' '.join([filter])
            query_matches.append(query_match.upper())
        return query_matches
    else:
        print('Not Seq Scan.')

def parse_index_scan(plan):
    query_matches = []
    if plan['Node Type'] == 'Index Scan':
        if 'Filter' in plan.keys():
            filter = plan['Filter'].replace('::text', '').replace('(', '').replace(')', '')
            relation_name = plan['Relation Name'].upper()
            query_match = ' '.join([relation_name + '.' + filter])
            query_matches.append(query_match.upper())
        if 'Index Cond' in plan.keys():
            index_cond = plan['Index Cond'].replace('::text', '').replace('(', '').replace(')', '')
            query_match = ' '.join([index_cond])
            query_matches.append(query_match.upper())
        return query_matches
    else:
        print('Not Index Scan.')

def parse_hash_join(plan):
    query_matches = []
    if plan['Node Type'] == 'Hash Join':
        if 'Hash Cond' in plan.keys():
            hash_cond = plan['Hash Cond'].replace('::text', '').replace('(', '').replace(')', '')
            query_match = ' '.join([hash_cond])
            query_matches.append(query_match.upper())
        return query_matches
    else:
        print('Not Index Scan.')

def parse_gather(plan):
    query_matches = []
    if plan['Node Type'] == 'Gather':
        if 'Output' in plan.keys():
            outputs = ', '.join(plan['Output']).replace('PARTIAL ', '')
            query_match = ' '.join(['SELECT', outputs])
            query_matches.append(query_match.upper())
        return query_matches
    else:
        print('Not Index Scan.')

query = """
        SELECT
        AUTHORSHIP.personKey,
        CONCAT(PERSON.personFirstName, ' ', PERSON.personLastName) as personName,
        COUNT(*) AS pubCount
        FROM PUBLICATION, INPROCEEDING, AUTHORSHIP, PROCEEDING, PERSON
        WHERE INPROCEEDING.pubKey = PUBLICATION.pubKey
        AND INPROCEEDING.pubKey = AUTHORSHIP.pubKey
        AND PROCEEDING.pubKey = INPROCEEDING.inproCrossRef
        AND PERSON.personKey = AUTHORSHIP.personKey
        AND PUBLICATION.pubYear = 2014
        AND PROCEEDING.pubkey LIKE 'conf/sigmod/%'
        GROUP BY AUTHORSHIP.personKey, PERSON.personFirstName, PERSON.personLastName
        HAVING COUNT(*) >= 2;
        """.upper()

qep, msg = get_qep('EXPLAIN ANALYZE' + query)
qep, msg = get_qep(query)
qep, msg = get_qep('EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON)' + query)
qep
plan = qep[0][0][0]['Plan']
plan
parse(plan)





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
