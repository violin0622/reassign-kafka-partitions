# reassign-kafka-partitions
A tool of generate partition assignment json file.   
Similar with kafka's `kafka-reassign-partitions.sh`, but this tool can generate json that changed replication-factor.   

usage:
```
-b        --broker-list : Broker id list seperated by comma with no space.  
-t              --topic : Topic name.
-p         --partitions : The number of partitions for the topic.
-r --replication-factor : The replication factor for each partition in the topic being created. 
-o             --output : [optional] The filename of output. If not specified, output to stdout.
-f             --format : [optional] Format output json with 2 space indent. 
```
