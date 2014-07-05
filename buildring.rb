require './murmur3'
require 'set'

class RendezvousHash

  def initialize(nodes=nil, hash_function=method(:murmur3_32_str_hash))
    @nodes = nodes
    @hash_function = hash_function
  end

  def find_node(key)
    @nodes.max {|a,b| @hash_function.call("#{a}-#{key}") <=> @hash_function.call("#{b}-#{key}")}
  end

end

class Cluster

  def initialize(cluster, hash_function=method(:murmur3_32_str_hash), replicas=2)
    @replicas = replicas
    @hash_function = hash_function
    @nodes = Hash[]
    zones = Set.new([])
    @zone_members = Hash[]
    rings = Hash[]

    cluster.each do |node|
      members = []
      (node_id, node_hostname, node_zone) = node
      zones.add(node_zone)
      members = @zone_members[node_zone] || []
      @zone_members[node_zone] = members << node_id
      @nodes[node_id] = node_hostname
    end

    @zones = zones.to_a.sort!

    @zone_members.each do |zone, zone_nodes|
      rings[zone] = RendezvousHash.new(zone_nodes, @hash_function)
    end
    @rings = rings
  end

  def node_name_by_id(node_id)
    @nodes[node_id]
  end

  def find_nodes(product_id, block_index)
    nodes = []
    offset = (product_id.to_i + block_index.to_i) % @zones.length
    for i in (0...@replicas)
      zone = @zones[(i + offset) % @zones.length]
      key = "#{product_id}-#{block_index}"
      nodes << @rings[zone].find_node(key)
    end
    nodes
  end

end

nodes = [
    ['1', 'dn1.kissmetrics.com', 'us-east-1d'],
    ['2', 'dn2.kissmetrics.com', 'us-east-1e'],
    ['3', 'dn3.kissmetrics.com', 'us-east-1e'],
    ['4', 'dn4.kissmetrics.com', 'us-east-1e'],
    ['5', 'dn5.kissmetrics.com', 'us-east-1c'],
    ['6', 'dn6.kissmetrics.com', 'us-east-1c'],
    ['7', 'dn7.kissmetrics.com', 'us-east-1d'],
    ['8', 'dn8.kissmetrics.com', 'us-east-1d'],
    ['9', 'dn9.kissmetrics.com', 'us-east-1d'],
    ['10', 'dn10.kissmetrics.com', 'us-east-1c'],
    ['11', 'dn14.kissmetrics.com', 'us-east-1c'],
    ['12', 'dn15.kissmetrics.com', 'us-east-1d'],
    ['13', 'dn12.kissmetrics.com', 'us-east-1c'],
    ['14', 'dn13.kissmetrics.com', 'us-east-1c'],
    ['15', 'dn11.kissmetrics.com', 'us-east-1e'],
    ['16', 'dn20.kissmetrics.com', 'us-east-1e'],
    ['17', 'dn19.kissmetrics.com', 'us-east-1d'],
    ['18', 'dn18.kissmetrics.com', 'us-east-1c'],
    ['19', 'dn17.kissmetrics.com', 'us-east-1d'],
    ['20', 'dn16.kissmetrics.com', 'us-east-1e'],
    ['21', 'dn25.kissmetrics.com', 'us-east-1e'],
    ['22', 'dn23.kissmetrics.com', 'us-east-1d'],
    ['23', 'dn24.kissmetrics.com', 'us-east-1e'],
    ['24', 'dn21.kissmetrics.com', 'us-east-1d'],
    ['25', 'dn22.kissmetrics.com', 'us-east-1c'],
    ['26', 'dn30.kissmetrics.com', 'us-east-1c'],
    ['27', 'dn28.kissmetrics.com', 'us-east-1e'],
    ['28', 'dn29.kissmetrics.com', 'us-east-1e'],
    ['29', 'dn27.kissmetrics.com', 'us-east-1d'],
    ['30', 'dn26.kissmetrics.com', 'us-east-1c'],
    ['68', 'dn31.kissmetrics.com', 'us-east-1c'],
    ['69', 'dn32.kissmetrics.com', 'us-east-1d'],
    ['70', 'dn33.kissmetrics.com', 'us-east-1e'],
    ['71', 'dn34.kissmetrics.com', 'us-east-1c'],
    ['72', 'dn35.kissmetrics.com', 'us-east-1d'],
    ['73', 'dn36.kissmetrics.com', 'us-east-1e'],
    ['74', 'dn37.kissmetrics.com', 'us-east-1c'],
    ['75', 'dn38.kissmetrics.com', 'us-east-1d'],
    ['76', 'dn39.kissmetrics.com', 'us-east-1e'],
    ['77', 'dn40.kissmetrics.com', 'us-east-1c'],
    ['78', 'dn41.kissmetrics.com', 'us-east-1d'],
    ['79', 'dn42.kissmetrics.com', 'us-east-1e'],
    ['80', 'dn43.kissmetrics.com', 'us-east-1c'],
    ['81', 'dn44.kissmetrics.com', 'us-east-1d'],
    ['82', 'dn45.kissmetrics.com', 'us-east-1e'],
    ['83', 'dn46.kissmetrics.com', 'us-east-1c'],
    ['84', 'dn47.kissmetrics.com', 'us-east-1d'],
    ['85', 'dn48.kissmetrics.com', 'us-east-1e'],
    ['86', 'dn49.kissmetrics.com', 'us-east-1c'],
    ['87', 'dn50.kissmetrics.com', 'us-east-1d'],
    ['88', 'dn51.kissmetrics.com', 'us-east-1e'],
    ['89', 'dn52.kissmetrics.com', 'us-east-1c'],
    ['90', 'dn53.kissmetrics.com', 'us-east-1d'],
    ['91', 'dn54.kissmetrics.com', 'us-east-1e'],
    ['92', 'dn55.kissmetrics.com', 'us-east-1c'],
    ['93', 'dn56.kissmetrics.com', 'us-east-1d'],
    ['94', 'dn57.kissmetrics.com', 'us-east-1e'],
    ['95', 'dn58.kissmetrics.com', 'us-east-1c'],
    ['96', 'dn59.kissmetrics.com', 'us-east-1d'],
    ['97', 'dn60.kissmetrics.com', 'us-east-1e'],
]

product = ARGV[0]
count = ARGV[1]
dist_type = ARGV[2]

cluster = Cluster.new(nodes)
total_blocks = Hash[]
layout = Hash[]

for i in (0...count.to_i)
  nodes = cluster.find_nodes(product, i)
  nodes.each do |node|
    node_count = total_blocks[node] || 0
    total_blocks[node] = node_count + 1
    node_blocks = layout[node] || []
    layout[node] = node_blocks << i
  end
end

require 'json'   
if dist_type == 'total'
  puts JSON.dump(total_blocks)
elsif dist_type == 'layout'
  puts JSON.dump(layout)
end
