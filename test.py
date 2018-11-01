from utils import format, read_json
from analysis import analyze

tests = read_json('tests.json')

print('Available Test Cases:')
for i, test in enumerate(tests):
    print(str(i) + '. ' + test['Test Case'])

test = tests[11]
test_case = test['Test Case']
query = test['Query']
execution_plan = test['Execution Plan']
print('Test Case: \n' + test_case + '\n')
print('Input Query: \n' + query + '\n')
print('Formatted Query: \n' + format(query) + '\n')

format(query)

"SELECT CONCAT(PERSON.PERSONFIRSTNAME, ' ', PERSON.PERSONLASTNAME) AS PUBAUTHOR,\n\tPVLDB.COUNT AS PVLDBCOUNT,\n\tSIGMOD.COUNT AS SIGMODCOUNT\nFROM\n\t\t(SELECT COUNT(*) AS COUNT,\n\t\t\t\tAUTHORSHIP.PERSONKEY AS PERSONKEY\n\t\t\tFROM PUBLICATION,\n\t\t\t\tAUTHORSHIP\n\t\t\tWHERE PUBLICATION.PUBKEY = AUTHORSHIP.PUBKEY\n\t\t\t\t\tAND PUBLICATION.PUBKEY LIKE '%PVLDB%'\n\t\t\tGROUP BY AUTHORSHIP.PERSONKEY) AS PVLDB,\n\n\t\t(SELECT COUNT(*) AS COUNT,\n\t\t\t\tAUTHORSHIP.PERSONKEY AS PERSONKEY\n\t\t\tFROM PUBLICATION,\n\t\t\t\tAUTHORSHIP\n\t\t\tWHERE PUBLICATION.PUBKEY = AUTHORSHIP.PUBKEY\n\t\t\t\t\tAND PUBLICATION.PUBKEY LIKE '%SIGMOD%'\n\t\t\tGROUP BY AUTHORSHIP.PERSONKEY) AS SIGMOD,\n\tPERSON\nWHERE PERSON.PERSONKEY = PVLDB.PERSONKEY\n\t\tAND PVLDB.PERSONKEY = SIGMOD.PERSONKEY\n\t\tAND PVLDB.COUNT >= 10\n\t\tAND SIGMOD.COUNT >= 10"

analyze(execution_plan, query)


  #
  # {
  #   "Query": "",
  #   "Test Case": "",
  #   "Execution Plan": {
  #
  #   }
  # },
