import sqlparse
import json

def format(query):
    return sqlparse.format(query, reindent=True, indent_tabs=True).upper()

def read_json(file_path):
    with open(file_path, 'r') as f:
        content = json.load(f)
    return content
