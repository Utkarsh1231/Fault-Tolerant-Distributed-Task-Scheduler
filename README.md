# Fault-Tolerant Distributed Task Scheduler

A resilient, distributed task scheduling system implemented in pure Python. The system features automatic master failover and guarantees no in-flight work is lost across node crashes.

## Key Features
- **Hybrid Leader Election:** Utilizes a Bully-Raft hybrid algorithm for rapid O(n) leader election and control plane management.
- **Global State Snapshots:** Implements the Chandy-Lamport algorithm for consistent global state capture without pausing task execution.
- **Durable Recovery:** Snapshots are replicated across all nodes. If the leader fails, the newly elected leader seamlessly restores the last consistent snapshot and resumes scheduling.
- **Two-Plane Architecture:** Separates ephemeral control messages (elections, heartbeats) from persistent data plane connections (task assignment, worker monitoring).

## Setup and Execution
This system is entirely self-contained and uses only Python standard libraries.

1. Open three separate terminals.
2. Run the following commands, one in each terminal:
   ```bash
   python hybrid_node.py 1
   python hybrid_node.py 2
   python hybrid_node.py 3
