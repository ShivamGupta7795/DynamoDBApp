# simpleamazondynamoapp
Implementation of a simple Amazon Dynamo Simulation.
For this implementation I considered 5 fixed avd's which are connected in a ring/chord. 
Every avd can communicate with any other avd in the ring.
Each object is stored according to DHT principle i.e. if hash(obejct)>hash(prednode) && hash(object)<=hash(currentnode) then insert the object in the current node.
The degree of replication is 3 i.e. a node and it's two successors in the ring will have a copy of the object.
This implementation can also handle concurrent insert/query operation and can handle multiple avd failures during operation.
During recovery the recovered node communicate with it's predecessor and successor for recovering missing insertions and updated.
