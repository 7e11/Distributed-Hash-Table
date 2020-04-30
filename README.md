# Distributed-Hash-Table
CSE 403: Advanced Operating Systems, Assignment 1.


I didn't add much documentation for the assignment 1 charts, as they were made in excel, 
but I'll link the assignment 2 charts here for posterity.

Throughput (See raw PNG for zoom)
![](assignment_2/throughput.png)

Latency
![](assignment_2/latency.png)

Raw operation counts (95% Confidence Interval)
- Notice the high number of put_aborts for low key ranges
- And spikes in get_negack, as a client tries to get a key on its own node, which is locked by a different client.

![](assignment_2/op_counts_95CI.png)
