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


def print_qep(query):
    qep, msg = get_qep(query)

    if qep is not None:
        for execution in qep:
            print(execution)
    else:
        print(msg)

def get_action_cost(action_str):
    action = action_str.split('  ')[0]

    cost_estimate = qep[0][0].replace(') (', ')  (').split('  ')[1].replace('(', '').replace(')', '').split(' ')
    times = cost_estimate[0].split('=')[1]
    time_list = times.split('..')
    time = round(float(time_list[1]) - float(time_list[0]), 3)
    cost_estimate[0] = cost_estimate[0].replace(times, str(time))
    cost_estimate = {element.split('=')[0]: element.split('=')[1] for element in cost_estimate}

    cost_actual = qep[0][0].replace(') (', ')  (').split('  ')[2].replace('(', '').replace(')', '').replace('actual time', 'cost').split(' ')
    times = cost_actual[0].split('=')[1]
    time_list = times.split('..')
    time = round(float(time_list[1]) - float(time_list[0]), 3)
    cost_actual[0] = cost_actual[0].replace(times, str(time))
    cost_actual
    cost_actual = {element.split('=')[0]: element.split('=')[1] for element in cost_actual}

    return {'action': action, 'cost_estimate': cost_estimate, 'cost_actual': cost_actual}

query_error = """
        EXPLAIN ANALYZE \
        SELECT pubType, COUNT(*) FROM PUBLICATION \
        WHERE pubYear >= 2000 AND pubYear <= 2017 \
        GROUP BY pubYear
        """
print_qep(query_error)

query = """
        SELECT pubType, COUNT(*) FROM PUBLICATION \
        WHERE pubYear >= 2000 AND pubYear <= 2017 \
        GROUP BY pubType
        """
query = """
        SELECT pubType FROM PUBLICATION
        """
query = """
        SELECT CONCAT(PERSON.personFirstName, ' ', PERSON.personLastName) AS pubAuthor,
        PVLDB.count AS pvldbCount,
        SIGMOD.count AS sigmodCount
        FROM
        (
        	SELECT COUNT(*) AS count, AUTHORSHIP.personKey AS personKey FROM PUBLICATION, AUTHORSHIP
        	WHERE PUBLICATION.pubKey = AUTHORSHIP.pubKey
        	AND PUBLICATION.pubKey LIKE '%pvldb%'
        	GROUP BY AUTHORSHIP.personKey
        ) AS PVLDB,
        (
        	SELECT COUNT(*) AS count, AUTHORSHIP.personKey AS personKey FROM PUBLICATION, AUTHORSHIP
        	WHERE PUBLICATION.pubKey = AUTHORSHIP.pubKey
        	AND PUBLICATION.pubKey LIKE '%sigmod%'
        	GROUP BY AUTHORSHIP.personKey
        ) AS SIGMOD,
        PERSON
        WHERE PERSON.personKey = PVLDB.personKey
        AND PVLDB.personKey = SIGMOD.personKey
        AND PVLDB.count >= 10
        AND SIGMOD.count >= 10
        """

qep, msg = get_qep('EXPLAIN (FORMAT JSON)' + query)
qep, msg = get_qep('EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON)' + query)


data = [{'Plan': {'Node Type': 'Seq Scan',
   'Relation Name': 'publication',
   'Schema': 'public',
   'Alias': 'publication',
   'Startup Cost': 0.0,
   'Total Cost': 30385.53,
   'Plan Rows': 1070653,
   'Plan Width': 9,
   'Actual Startup Time': 0.071,
   'Actual Total Time': 242.622,
   'Actual Rows': 1070653,
   'Actual Loops': 1,
   'Output': ['pubtype'],
   'Shared Hit Blocks': 11167,
   'Shared Read Blocks': 8512,
   'Shared Dirtied Blocks': 0,
   'Shared Written Blocks': 0,
   'Local Hit Blocks': 0,
   'Local Read Blocks': 0,
   'Local Dirtied Blocks': 0,
   'Local Written Blocks': 0,
   'Temp Read Blocks': 0,
   'Temp Written Blocks': 0},
  'Planning Time': 0.442,
  'Triggers': [],
  'Execution Time': 304.371}]


len(qep[0][0][0]['Plan']['Plans'])
qep[0][0][0].keys()
qep[0][0][0]['Plan']

qep, msg = get_qep('EXPLAIN (ANALYZE)' + query)
qep




# action_indices_start = [0]
# for i in range(len(qep)):
#     if '->' in qep[i][0]:
#         action_indices_start.append(i)
# action_indices_end = action_indices_start[1:] + [len(qep) - 2]
# action_indices_end = [i-1 for i in action_indices_end]
# action_indice_ranges = [[action_indices_start[i], action_indices_end[i]]for i in range(len(action_indices_start))]
# action_indice_ranges
#
# qep_dict = []
# for action_indice_range in action_indice_ranges:
#     desc = qep[action_indice_range[0]][0]
#     desc = desc.split('->')[-1].strip()
#     action_plan = get_action_cost(desc)
#     for i in range(action_indice_range[0] + 1, action_indice_range[1]):
#         key = qep[i][0].split(': ')[0].strip()
#         value = qep[i][0].split(': ')[1].strip()
#         action_plan[key] = value
#     qep_dict.append(action_plan)
# qep_dict


# Startup cost is a tricky concept. It doesn't just represent the amount of time before that component starts. It represents the amount of time between when the component starts executing (reading in data) and when the component outputs its first row.
# Total cost is the entire execution time of the component, from when it begins reading in data to when it finishes writing its output.
