import psycopg2

def get_qep(query, connection = "dbname='dblpDB_quarter' user='anqitu' host='localhost' password='dbpass'"):
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


query = """
        EXPLAIN \
        SELECT pubType, COUNT(*) FROM PUBLICATION \
        WHERE pubYear >= 2000 AND pubYear <= 2017 \
        GROUP BY pubType
        """
query = """
        EXPLAIN \
        SELECT pubType, COUNT(*) FROM PUBLICATION \
        WHERE pubYear >= 2000 AND pubYear <= 2017 \
        GROUP BY pubYear
        """

print_qep(query)
