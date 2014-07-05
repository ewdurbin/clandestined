
from collections import defaultdict
from sets import Set

from murmur3 import murmur3_x86_32

class RendezvousHash(object):

    def __init__(self, nodes=None, hash_function=murmur3_x86_32):
        self.nodes = nodes
        self.hash_function = hash_function

    def find_node(self, key):
        hash_string = "%s-%s"
        return max(self.nodes, key=lambda x: self.hash_function(hash_string % (str(x), str(key))))

class Cluster(object):

    def __init__(self, cluster, hash_function=murmur3_x86_32, replicas=2):
        self.replicas = replicas
        self.hash_function = hash_function
        self.nodes = {}
        zones = Set([])
        self.zone_members = defaultdict(list)
        self.rings = {}

        for node in cluster:
            (node_id, node_hostname, node_zone) = node
            zones.add(node_zone)
            self.zone_members[node_zone].append(node_id)
            self.nodes[node_id] = node_hostname

        self.zones = sorted(list(zones))

        for zone, nodes in self.zone_members.iteritems():
            self.rings[zone] = RendezvousHash(nodes=nodes, hash_function=self.hash_function)

    def node_name_by_id(self, node_id):
        return self.nodes[node_id]

    def find_nodes(self, product_id, block_index):
        nodes = []
        offset = int(product_id) + int(block_index) % len(self.zones)
        for i in range(self.replicas):
            zone = self.zones[(i + offset) % len(self.zones)]
            ring = self.rings[zone]
            key = "%s-%s" % (product_id, block_index)
            nodes.append(ring.find_node(key))
        return nodes


if __name__ == '__main__':

    import sys

    nodes = [
        ('1', 'dn1.kissmetrics.com', 'us-east-1d'),
        ('2', 'dn2.kissmetrics.com', 'us-east-1e'),
        ('3', 'dn3.kissmetrics.com', 'us-east-1e'),
        ('4', 'dn4.kissmetrics.com', 'us-east-1e'),
        ('5', 'dn5.kissmetrics.com', 'us-east-1c'),
        ('6', 'dn6.kissmetrics.com', 'us-east-1c'),
        ('7', 'dn7.kissmetrics.com', 'us-east-1d'),
        ('8', 'dn8.kissmetrics.com', 'us-east-1d'),
        ('9', 'dn9.kissmetrics.com', 'us-east-1d'),
        ('10', 'dn10.kissmetrics.com', 'us-east-1c'),
        ('11', 'dn14.kissmetrics.com', 'us-east-1c'),
        ('12', 'dn15.kissmetrics.com', 'us-east-1d'),
        ('13', 'dn12.kissmetrics.com', 'us-east-1c'),
        ('14', 'dn13.kissmetrics.com', 'us-east-1c'),
        ('15', 'dn11.kissmetrics.com', 'us-east-1e'),
        ('16', 'dn20.kissmetrics.com', 'us-east-1e'),
        ('17', 'dn19.kissmetrics.com', 'us-east-1d'),
        ('18', 'dn18.kissmetrics.com', 'us-east-1c'),
        ('19', 'dn17.kissmetrics.com', 'us-east-1d'),
        ('20', 'dn16.kissmetrics.com', 'us-east-1e'),
        ('21', 'dn25.kissmetrics.com', 'us-east-1e'),
        ('22', 'dn23.kissmetrics.com', 'us-east-1d'),
        ('23', 'dn24.kissmetrics.com', 'us-east-1e'),
        ('24', 'dn21.kissmetrics.com', 'us-east-1d'),
        ('25', 'dn22.kissmetrics.com', 'us-east-1c'),
        ('26', 'dn30.kissmetrics.com', 'us-east-1c'),
        ('27', 'dn28.kissmetrics.com', 'us-east-1e'),
        ('28', 'dn29.kissmetrics.com', 'us-east-1e'),
        ('29', 'dn27.kissmetrics.com', 'us-east-1d'),
        ('30', 'dn26.kissmetrics.com', 'us-east-1c'),
        ('68', 'dn31.kissmetrics.com', 'us-east-1c'),
        ('69', 'dn32.kissmetrics.com', 'us-east-1d'),
        ('70', 'dn33.kissmetrics.com', 'us-east-1e'),
        ('71', 'dn34.kissmetrics.com', 'us-east-1c'),
        ('72', 'dn35.kissmetrics.com', 'us-east-1d'),
        ('73', 'dn36.kissmetrics.com', 'us-east-1e'),
        ('74', 'dn37.kissmetrics.com', 'us-east-1c'),
        ('75', 'dn38.kissmetrics.com', 'us-east-1d'),
        ('76', 'dn39.kissmetrics.com', 'us-east-1e'),
        ('77', 'dn40.kissmetrics.com', 'us-east-1c'),
        ('78', 'dn41.kissmetrics.com', 'us-east-1d'),
        ('79', 'dn42.kissmetrics.com', 'us-east-1e'),
        ('80', 'dn43.kissmetrics.com', 'us-east-1c'),
        ('81', 'dn44.kissmetrics.com', 'us-east-1d'),
        ('82', 'dn45.kissmetrics.com', 'us-east-1e'),
        ('83', 'dn46.kissmetrics.com', 'us-east-1c'),
        ('84', 'dn47.kissmetrics.com', 'us-east-1d'),
        ('85', 'dn48.kissmetrics.com', 'us-east-1e'),
        ('86', 'dn49.kissmetrics.com', 'us-east-1c'),
        ('87', 'dn50.kissmetrics.com', 'us-east-1d'),
        ('88', 'dn51.kissmetrics.com', 'us-east-1e'),
        ('89', 'dn52.kissmetrics.com', 'us-east-1c'),
        ('90', 'dn53.kissmetrics.com', 'us-east-1d'),
        ('91', 'dn54.kissmetrics.com', 'us-east-1e'),
        ('92', 'dn55.kissmetrics.com', 'us-east-1c'),
        ('93', 'dn56.kissmetrics.com', 'us-east-1d'),
        ('94', 'dn57.kissmetrics.com', 'us-east-1e'),
        ('95', 'dn58.kissmetrics.com', 'us-east-1c'),
        ('96', 'dn59.kissmetrics.com', 'us-east-1d'),
        ('97', 'dn60.kissmetrics.com', 'us-east-1e'),
    ]
    
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
