# Replication for the Global Data Plane

## Distributed Replication for a Specialized Database 
## Scott Numamoto - Tony Yang - Steven Wu

There has been an explosion in the number of smart devices connected via the Internet to augment people's everyday life. The Global Data Plane (GDP) is a new infrastructure design which tries to address the challenges presented by such a vast and broad array of devices by introducing a higher layer of abstraction. We present an anti-entropy mechanism which ensures that data is replicated on multiple GDP log servers and user APIs. We modeled many of our decision to support a highly write available system. Evaluations show that our replication system can recover from large loss of log servers and has significantly better usage of bandwidth than a naive system. 

Replication for the Global Data Plane is the result of a course paper for [CS 262: Advanced Topics in Computer Systems](https://people.eecs.berkeley.edu/~kubitron/courses/cs262a-F18/index.html). 
