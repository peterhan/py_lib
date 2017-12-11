import json
from collections import defaultdict

nested_dict = lambda: defaultdict(nested_dict)
nest = nested_dict()

nest[0][1][2][3][4][5] = 6
print nest[0][1][2][3][6]
print  json.dumps(nest,indent=2)


class NestedDict(dict):
    def __missing__(self, key):
        self[key] = NestedDict()
        return self[key]

table = NestedDict()
table['A']['B1']['C1'] = 1
print  table['A']['aB1']
print  json.dumps(table,indent=2)