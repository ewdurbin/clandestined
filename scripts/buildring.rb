
require 'json'
require 'clandestine'

nodes = Hash[
    '1' => Hash["name" => 'dn1.kissmetrics.com', "zone" => 'us-east-1d'],
    '2' => Hash["name" => 'dn2.kissmetrics.com', "zone" => 'us-east-1e'],
    '3' => Hash["name" => 'dn3.kissmetrics.com', "zone" => 'us-east-1e'],
    '4' => Hash["name" => 'dn4.kissmetrics.com', "zone" => 'us-east-1e'],
    '5' => Hash["name" => 'dn5.kissmetrics.com', "zone" => 'us-east-1c'],
    '6' => Hash["name" => 'dn6.kissmetrics.com', "zone" => 'us-east-1c'],
    '7' => Hash["name" => 'dn7.kissmetrics.com', "zone" => 'us-east-1d'],
    '8' => Hash["name" => 'dn8.kissmetrics.com', "zone" => 'us-east-1d'],
    '9' => Hash["name" => 'dn9.kissmetrics.com', "zone" => 'us-east-1d'],
    '10' => Hash["name" => 'dn10.kissmetrics.com', "zone" => 'us-east-1c'],
    '11' => Hash["name" => 'dn14.kissmetrics.com', "zone" => 'us-east-1c'],
    '12' => Hash["name" => 'dn15.kissmetrics.com', "zone" => 'us-east-1d'],
    '13' => Hash["name" => 'dn12.kissmetrics.com', "zone" => 'us-east-1c'],
    '14' => Hash["name" => 'dn13.kissmetrics.com', "zone" => 'us-east-1c'],
    '15' => Hash["name" => 'dn11.kissmetrics.com', "zone" => 'us-east-1e'],
    '16' => Hash["name" => 'dn20.kissmetrics.com', "zone" => 'us-east-1e'],
    '17' => Hash["name" => 'dn19.kissmetrics.com', "zone" => 'us-east-1d'],
    '18' => Hash["name" => 'dn18.kissmetrics.com', "zone" => 'us-east-1c'],
    '19' => Hash["name" => 'dn17.kissmetrics.com', "zone" => 'us-east-1d'],
    '20' => Hash["name" => 'dn16.kissmetrics.com', "zone" => 'us-east-1e'],
    '21' => Hash["name" => 'dn25.kissmetrics.com', "zone" => 'us-east-1e'],
    '22' => Hash["name" => 'dn23.kissmetrics.com', "zone" => 'us-east-1d'],
    '23' => Hash["name" => 'dn24.kissmetrics.com', "zone" => 'us-east-1e'],
    '24' => Hash["name" => 'dn21.kissmetrics.com', "zone" => 'us-east-1d'],
    '25' => Hash["name" => 'dn22.kissmetrics.com', "zone" => 'us-east-1c'],
    '26' => Hash["name" => 'dn30.kissmetrics.com', "zone" => 'us-east-1c'],
    '27' => Hash["name" => 'dn28.kissmetrics.com', "zone" => 'us-east-1e'],
    '28' => Hash["name" => 'dn29.kissmetrics.com', "zone" => 'us-east-1e'],
    '29' => Hash["name" => 'dn27.kissmetrics.com', "zone" => 'us-east-1d'],
    '30' => Hash["name" => 'dn26.kissmetrics.com', "zone" => 'us-east-1c'],
    '68' => Hash["name" => 'dn31.kissmetrics.com', "zone" => 'us-east-1c'],
    '69' => Hash["name" => 'dn32.kissmetrics.com', "zone" => 'us-east-1d'],
    '70' => Hash["name" => 'dn33.kissmetrics.com', "zone" => 'us-east-1e'],
    '71' => Hash["name" => 'dn34.kissmetrics.com', "zone" => 'us-east-1c'],
    '72' => Hash["name" => 'dn35.kissmetrics.com', "zone" => 'us-east-1d'],
    '73' => Hash["name" => 'dn36.kissmetrics.com', "zone" => 'us-east-1e'],
    '74' => Hash["name" => 'dn37.kissmetrics.com', "zone" => 'us-east-1c'],
    '75' => Hash["name" => 'dn38.kissmetrics.com', "zone" => 'us-east-1d'],
    '76' => Hash["name" => 'dn39.kissmetrics.com', "zone" => 'us-east-1e'],
    '77' => Hash["name" => 'dn40.kissmetrics.com', "zone" => 'us-east-1c'],
    '78' => Hash["name" => 'dn41.kissmetrics.com', "zone" => 'us-east-1d'],
    '79' => Hash["name" => 'dn42.kissmetrics.com', "zone" => 'us-east-1e'],
    '80' => Hash["name" => 'dn43.kissmetrics.com', "zone" => 'us-east-1c'],
    '81' => Hash["name" => 'dn44.kissmetrics.com', "zone" => 'us-east-1d'],
    '82' => Hash["name" => 'dn45.kissmetrics.com', "zone" => 'us-east-1e'],
    '83' => Hash["name" => 'dn46.kissmetrics.com', "zone" => 'us-east-1c'],
    '84' => Hash["name" => 'dn47.kissmetrics.com', "zone" => 'us-east-1d'],
    '85' => Hash["name" => 'dn48.kissmetrics.com', "zone" => 'us-east-1e'],
    '86' => Hash["name" => 'dn49.kissmetrics.com', "zone" => 'us-east-1c'],
    '87' => Hash["name" => 'dn50.kissmetrics.com', "zone" => 'us-east-1d'],
    '88' => Hash["name" => 'dn51.kissmetrics.com', "zone" => 'us-east-1e'],
    '89' => Hash["name" => 'dn52.kissmetrics.com', "zone" => 'us-east-1c'],
    '90' => Hash["name" => 'dn53.kissmetrics.com', "zone" => 'us-east-1d'],
    '91' => Hash["name" => 'dn54.kissmetrics.com', "zone" => 'us-east-1e'],
    '92' => Hash["name" => 'dn55.kissmetrics.com', "zone" => 'us-east-1c'],
    '93' => Hash["name" => 'dn56.kissmetrics.com', "zone" => 'us-east-1d'],
    '94' => Hash["name" => 'dn57.kissmetrics.com', "zone" => 'us-east-1e'],
    '95' => Hash["name" => 'dn58.kissmetrics.com', "zone" => 'us-east-1c'],
    '96' => Hash["name" => 'dn59.kissmetrics.com', "zone" => 'us-east-1d'],
    '97' => Hash["name" => 'dn60.kissmetrics.com', "zone" => 'us-east-1e'],
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

if dist_type == 'total'
  puts JSON.dump(total_blocks)
elsif dist_type == 'layout'
  puts JSON.dump(layout)
end
