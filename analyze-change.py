import json
import sys

before = sys.argv[1]
after = sys.argv[2]

before_distribution = json.loads(open(before, 'r').read())
after_distribution = json.loads(open(after, 'r').read())

for node, item in before_distribution.items():
    before = set(item)
    after = set(after_distribution.get(node, []))
    removed = before.difference(after)
    added = after.difference(before)
    if len(added) > 0 or len(removed) > 0:
        print(json.dumps({node: {'removed': len(list(removed)), 'added': len(list(added))}}, indent=4, sort_keys=True))
