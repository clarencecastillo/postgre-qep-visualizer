queries = {
  "Seq Scan": """
              SELECT PUBLICATION.pubTitle FROM PUBLICATION
              WHERE PUBLICATION.pubYear >= 2000
              AND PUBLICATION.pubYear <= 2017
              """

  "Index Only Scan" : """
                      SELECT PUBLICATION.pubTitle FROM PUBLICATION
                      WHERE PUBLICATION.pubKey = 'books/aw/AhoHU83'
                      AND PUBLICATION.pubYear <= 2017
                      """

  "Index Scan" : """
                 SELECT PUBLICATION.pubKey FROM PUBLICATION
                 WHERE PUBLICATION.pubKey = 'books/aw/AhoHU83'
                 """
}

def get_test_query(node_type):
    if node_type in queries:
        return queries[node_type]
    else:
        return None


# query = """
#         SELECT PUBLICATION.pubTitle
#         FROM PUBLICATION, PROCEEDING
#         WHERE PUBLICATION.pubKey = PROCEEDING.pubKey
#         AND PROCEEDING.proceedType = 'conf'
#         """
# query = """
#         SELECT PUBLICATION.pubType, COUNT(*) FROM PUBLICATION
#         WHERE PUBLICATION.pubYear >= 2000 AND PUBLICATION.pubYear <= 2017
#         GROUP BY PUBLICATION.pubType;
#         """
#
# query = """
#         SELECT PUBLICATION.pubType, COUNT(*) FROM PUBLICATION
#         WHERE PUBLICATION.pubYear >= 2000 AND PUBLICATION.pubYear <= 2017
#         GROUP BY PUBLICATION.pubType;
#         """
#
# query = """
#         SELECT PUBLICATION.pubTitle FROM PUBLICATION
#         WHERE PUBLICATION.pubYear >= 2000 AND PUBLICATION.pubYear <= 2017
#         """
