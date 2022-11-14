#!/usr/local/bin/python3
from typing import List, Dict
import json, random, sys, getopt

usage = '''
-b        --broker-list : Broker id list seperated by comma with no space.  
-t              --topic : Topic name.
-p         --partitions : The number of partitions for the topic.
-r --replication-factor : The replication factor for each partition in the topic being created. 
-o             --output : [optional] The filename of output. If not specified, output to stdout.
-f             --format : [optional] Format output json with 2 space indent. 
'''

def parse_args():
  broker_ids: List[int] = None 
  topic: str = None
  partitions: int = None
  replicas: int = None
  output_file: str = None
  format: bool = False
  
  short_opts: str = 'b:t:p:r:o:fh'
  long_opts: List[str] = ['topic=','partitions=', 'replication-factor=', 'output=', 'format', 'help']
  try:
    opts, args = getopt.getopt(sys.argv[1:], short_opts, long_opts)
  except getopt.GetoptError as err:
      print(err)  
      sys.exit(2)

  for opt, arg in opts:
    if opt in ['-h', '--help']:
      print(usage)
      sys.exit(0)
    elif opt in ['-b', '--broker-list']:
      broker_ids = list(map(lambda b: int(b), arg.split(',')))
    elif opt in ['-t', '--topic']:
      topic = arg
    elif opt in ['-p', '--partitions']:
      partitions = int(arg)
    elif opt in ['-r', '--replication-factor']:
      replicas = int(arg)
    elif opt in ['-o', '--output']:
      output_file = arg
    elif opt in ['-f', '--format']:
      format = True
    else:
      pass
  return broker_ids, topic, partitions, replicas, output_file, format

def check_args(
    broker_ids: List[int], 
    topic: str, 
    partitions: int, 
    replicas: int ) -> str:
  if broker_ids==None or len(broker_ids) == 0:
    return 'broker-list needed. '
  if topic == None or len(topic) == 0:
    return 'topic needed. '
  if partitions == None:
    return 'partitions needed. '
  if partitions == 0:
    return 'invalid partition. '
  if replicas == None:
    return 'replication-factor needed. '
  if replicas == 0:
    return 'invalid replication-factor. '
  return None

def assign(
    broker_ids: List[int], 
    partitions: int, 
    replicas: int) -> Dict[int, List[int]]:

  assignment: Dict[int, List[int]] = {}
  step: int = 0
  broker_size: int = len(broker_ids)
  first_partition_index: int = random.randrange(0, broker_size)

  def nextIndex(rep):
    shift: int = 1 + (step+rep) % (broker_size - 1)
    return broker_ids[(first_partition_index+shift) % broker_size]

  for p in range(0, partitions):
    if p > 0 and p % (broker_size) == 0:
      step += 1
    assignment[p] = [broker_ids[first_partition_index]]
    for r in range(1, replicas):
      assignment[p].append(nextIndex(r-1))
    first_partition_index = (first_partition_index+1) % broker_size
  return assignment

def convert_json_object(topic: str, assignment: Dict[int, List[int]]) -> Dict:
  assign_json = {'version': 1, 'partitions': []}
  for a in assignment:
    assign_json['partitions'].append({
        'topic': topic,
        'partition': a,
        'replicas': assignment[a],
        })
  return assign_json

def output(topic: str, assignment: Dict, output_file:str, format:bool):
  if output_file!=None:
    with open(output_file, 'w') as f:
      if format:
        f.write(json.dumps(convert_json_object(topic, assignment), indent=2))
      else:
        f.write(json.dumps(convert_json_object(topic, assignment)))
  else:
    if format:
      print(json.dumps(convert_json_object(topic, assignment), indent=2))
    else:
      print(json.dumps(convert_json_object(topic, assignment)))

def main():
  broker_ids, topic, partitions, replicas, output_file, format = parse_args()
  err:str = check_args(broker_ids, topic, partitions, replicas)
  if err != None:
    print(err)
    sys.exit(2)
  assignment = assign(broker_ids, partitions, replicas)
  output(topic, assignment, output_file, format)

if __name__ == "__main__":
  main()
