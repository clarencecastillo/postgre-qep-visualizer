import sqlparse

def format(query):
    return sqlparse.format(query, reindent=True, indent_tabs=True).upper()
