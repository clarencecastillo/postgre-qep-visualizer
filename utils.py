import sqlparse
import json
import re

def format(query):
    return sqlparse.format(query, reindent=True, indent_tabs=True).upper()

def read_json(file_path):
    with open(file_path, 'r') as f:
        content = json.load(f)
    return content

def parse_nested(text, left=r'[(]', right=r'[)]', sep=r','):
    # Based on https://stackoverflow.com/a/17141899/190597 (falsetru)
    pat = r'({}|{}|{})'.format(left, right, sep)
    tokens = re.split(pat, text)
    stack = [[]]
    for x in tokens:
        if not x or re.match(sep, x): continue
        if re.match(left, x):
            stack[-1].append([])
            stack.append(stack[-1][-1])
        elif re.match(right, x):
            stack.pop()
            if not stack:
                raise ValueError('error: opening bracket is missing')
        else:
            stack[-1].append(x)
    if len(stack) > 1:
        print(stack)
        raise ValueError('error: closing bracket is missing')
    return stack.pop()[0]
