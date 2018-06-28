import toml
import json

print json.dumps(toml.load(open('test.toml')),indent=2)