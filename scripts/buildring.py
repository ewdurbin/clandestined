if __name__ == '__main__':

    import sys

    from collections import defaultdict

    from clandestine import Cluster

    nodes = {
        '1': {'name': 'dn1.kissmetrics.com', 'zone': 'us-east-1d'},
        '2': {'name': 'dn2.kissmetrics.com', 'zone': 'us-east-1e'},
        '3': {'name': 'dn3.kissmetrics.com', 'zone': 'us-east-1e'},
        '4': {'name': 'dn4.kissmetrics.com', 'zone': 'us-east-1e'},
        '5': {'name': 'dn5.kissmetrics.com', 'zone': 'us-east-1c'},
        '6': {'name': 'dn6.kissmetrics.com', 'zone': 'us-east-1c'},
        '7': {'name': 'dn7.kissmetrics.com', 'zone': 'us-east-1d'},
        '8': {'name': 'dn8.kissmetrics.com', 'zone': 'us-east-1d'},
        '9': {'name': 'dn9.kissmetrics.com', 'zone': 'us-east-1d'},
        '10': {'name': 'dn10.kissmetrics.com', 'zone': 'us-east-1c'},
        '11': {'name': 'dn14.kissmetrics.com', 'zone': 'us-east-1c'},
        '12': {'name': 'dn15.kissmetrics.com', 'zone': 'us-east-1d'},
        '13': {'name': 'dn12.kissmetrics.com', 'zone': 'us-east-1c'},
        '14': {'name': 'dn13.kissmetrics.com', 'zone': 'us-east-1c'},
        '15': {'name': 'dn11.kissmetrics.com', 'zone': 'us-east-1e'},
        '16': {'name': 'dn20.kissmetrics.com', 'zone': 'us-east-1e'},
        '17': {'name': 'dn19.kissmetrics.com', 'zone': 'us-east-1d'},
        '18': {'name': 'dn18.kissmetrics.com', 'zone': 'us-east-1c'},
        '19': {'name': 'dn17.kissmetrics.com', 'zone': 'us-east-1d'},
        '20': {'name': 'dn16.kissmetrics.com', 'zone': 'us-east-1e'},
        '21': {'name': 'dn25.kissmetrics.com', 'zone': 'us-east-1e'},
        '22': {'name': 'dn23.kissmetrics.com', 'zone': 'us-east-1d'},
        '23': {'name': 'dn24.kissmetrics.com', 'zone': 'us-east-1e'},
        '24': {'name': 'dn21.kissmetrics.com', 'zone': 'us-east-1d'},
        '25': {'name': 'dn22.kissmetrics.com', 'zone': 'us-east-1c'},
        '26': {'name': 'dn30.kissmetrics.com', 'zone': 'us-east-1c'},
        '27': {'name': 'dn28.kissmetrics.com', 'zone': 'us-east-1e'},
        '28': {'name': 'dn29.kissmetrics.com', 'zone': 'us-east-1e'},
        '29': {'name': 'dn27.kissmetrics.com', 'zone': 'us-east-1d'},
        '30': {'name': 'dn26.kissmetrics.com', 'zone': 'us-east-1c'},
        '68': {'name': 'dn31.kissmetrics.com', 'zone': 'us-east-1c'},
        '69': {'name': 'dn32.kissmetrics.com', 'zone': 'us-east-1d'},
        '70': {'name': 'dn33.kissmetrics.com', 'zone': 'us-east-1e'},
        '71': {'name': 'dn34.kissmetrics.com', 'zone': 'us-east-1c'},
        '72': {'name': 'dn35.kissmetrics.com', 'zone': 'us-east-1d'},
        '73': {'name': 'dn36.kissmetrics.com', 'zone': 'us-east-1e'},
        '74': {'name': 'dn37.kissmetrics.com', 'zone': 'us-east-1c'},
        '75': {'name': 'dn38.kissmetrics.com', 'zone': 'us-east-1d'},
        '76': {'name': 'dn39.kissmetrics.com', 'zone': 'us-east-1e'},
        '77': {'name': 'dn40.kissmetrics.com', 'zone': 'us-east-1c'},
        '78': {'name': 'dn41.kissmetrics.com', 'zone': 'us-east-1d'},
        '79': {'name': 'dn42.kissmetrics.com', 'zone': 'us-east-1e'},
        '80': {'name': 'dn43.kissmetrics.com', 'zone': 'us-east-1c'},
        '81': {'name': 'dn44.kissmetrics.com', 'zone': 'us-east-1d'},
        '82': {'name': 'dn45.kissmetrics.com', 'zone': 'us-east-1e'},
        '83': {'name': 'dn46.kissmetrics.com', 'zone': 'us-east-1c'},
        '84': {'name': 'dn47.kissmetrics.com', 'zone': 'us-east-1d'},
        '85': {'name': 'dn48.kissmetrics.com', 'zone': 'us-east-1e'},
        '86': {'name': 'dn49.kissmetrics.com', 'zone': 'us-east-1c'},
        '87': {'name': 'dn50.kissmetrics.com', 'zone': 'us-east-1d'},
        '88': {'name': 'dn51.kissmetrics.com', 'zone': 'us-east-1e'},
        '89': {'name': 'dn52.kissmetrics.com', 'zone': 'us-east-1c'},
        '90': {'name': 'dn53.kissmetrics.com', 'zone': 'us-east-1d'},
        '91': {'name': 'dn54.kissmetrics.com', 'zone': 'us-east-1e'},
        '92': {'name': 'dn55.kissmetrics.com', 'zone': 'us-east-1c'},
        '93': {'name': 'dn56.kissmetrics.com', 'zone': 'us-east-1d'},
        '94': {'name': 'dn57.kissmetrics.com', 'zone': 'us-east-1e'},
        '95': {'name': 'dn58.kissmetrics.com', 'zone': 'us-east-1c'},
        '96': {'name': 'dn59.kissmetrics.com', 'zone': 'us-east-1d'},
        '97': {'name': 'dn60.kissmetrics.com', 'zone': 'us-east-1e'},
    }

    product = int(sys.argv[1])
    count = int(sys.argv[2])
    dist_type = sys.argv[3]

    if dist_type not in ['total', 'layout']:
        print (dist_type)
        raise Exception

    cluster = Cluster(nodes)
    layout = defaultdict(list)
    total_blocks = {}
    for i in range(count):
        nodes = cluster.find_nodes(product, i)
        for node in nodes:
            total_blocks[node] = total_blocks.get(node, 0) + 1
            layout[node].append(i)

    import json
    if dist_type == 'total':
        #print json.dumps(total_blocks, sort_keys=True, indent=4)
        print json.dumps(total_blocks)
    if dist_type == 'layout':
        print json.dumps(layout, sort_keys=True, indent=4)
