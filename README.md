# Peer-to-Peer Distributed Task Execution System

## Overview
This project implements a peer-to-peer (P2P) distributed system where nodes collaborate to execute tasks in parallel. The system features automatic peer discovery, task distribution, and execution coordination between master and worker nodes.

## Key Features
- **Automatic Peer Discovery**: Nodes automatically find each other using multicast UDP
- **Task Distribution**: Master node partitions and assigns tasks to worker nodes
- **Parallel Execution**: Workers execute tasks concurrently based on capability
- **Fault-Tolerant Communication**: Hybrid UDP/TCP protocol with error handling
- **Execution Tracking**: Master node monitors task completion across the network

## Installation
1. Ensure Python 3.6+ is installed
2. Clone this repository
3. No additional dependencies required (uses standard library only)

## Usage

### Starting Nodes
```python
python ScreenTest2.py
```

### Custom Configuration
Modify the example section at the bottom of the file to:
- Change port numbers
- Adjust the instruction set
- Add more worker nodes

Example node configuration:
```python
# Master node (handles 'a' type instructions)
start_node("a1", 10001, instructions=["a1", "a2", "b1", "c1", "a3"])

# Worker nodes
start_node("b1", 10002)  # Handles 'b' type instructions
start_node("c1", 10003)  # Handles 'c' type instructions
```

## System Architecture

### Message Types
The system uses 12 message types for communication:
- Discovery: `DISCOVER`, `HELLO`
- Connection: `CONNECT_REQUEST`, `CONNECT_ACK`, `HANDSHAKE`, `HANDSHAKE_RESPONSE`
- Coordination: `PEER_EXCHANGE`
- Task Management: `INSTRUCTION_ASSIGNMENT`, `EXECUTION_CONFIRMATION`, `TASK_COMPLETE`
- Termination: `DISCONNECT_REQUEST`, `DISCONNECT_ACK`

### Node Roles
- **Master Node**: 
  - Partitions and assigns tasks
  - Tracks execution progress
  - Coordinates worker nodes
- **Worker Nodes**: 
  - Execute assigned tasks
  - Report completion status
  - Participate in peer network

## Implementation Details

### Network Protocols
- **UDP Multicast**: Used for peer discovery
- **TCP**: Used for reliable task assignment and execution tracking

### Threading Model
- Separate threads for:
  - UDP listener
  - TCP listener
  - Peer exchange
  - Connection handling

## Example Output
```
Node a1 started on port 10001
Node a1 sent DISCOVER
Node b1 started on port 10002
Node b1 sent DISCOVER
Node a1 discovered peer b1
Node b1 discovered peer a1
Node a1 assigning tasks to peer b1
Node b1 received instructions: ['b1']
Node b1 executing: b1
Master received confirmation for b1 from b1
```

## Limitations
- Currently designed for local testing (uses localhost)
- Simple master election (first node with tasks becomes master)
- Basic error handling and recovery

## Future Enhancements
- Dynamic master election
- Network topology visualization
- Enhanced fault tolerance
- Load balancing
- Security features

## License
MIT License - Free for modification and distribution
