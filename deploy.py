# Should build for release
# Copy over the executable to the servers
# Run it and take all of the results, save to a csv where columns are parameters and rows are nodes.
#   I'll need a different approach if I end up having a monitoring thread which takes statistics every 50ms.
#   For those results, I'll need to save a CSV for every statistic which is monitored. Columns are monitoring threads,
#   Rows are time. (Time won't be exactly the same for all of the nodes, as there are no guarentees when the threads
#   wake up.
#   Stuff to monitor in real time: #messages which have been processed since the last wakeup (throughput).


# Perhaps there could also be a version which runs it with different combinations of key ranges and num ops.
