# Paxos

A simplified implementation of the Paxos consensus algorithm. Paxos is a algorithm designed for distributed systems by Leslie Lamport for ensuring consistancy across a group of nodes despite unreliable network conditions. To reduce complexity, this implementation collapses all the roles into one.

## Potential Future Improvements

1. Add ability to batch multiple values into each Accept, Accepted, and Confirm message to reduce required network traffic.

2. Add ability to write confirmed values to a file for quicker fault recovery.

3. Experiment with modifications to protocol to allow progress without a majority (like described here: https://padhye.org/raft-minority/)

