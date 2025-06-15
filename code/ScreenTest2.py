import socket
import threading
import json
import time
import random
from collections import defaultdict
from enum import Enum

# Message Types
class MessageType(Enum):
    DISCOVER = "DISCOVER"
    HELLO = "HELLO"
    PEER_EXCHANGE = "PEER_EXCHANGE"
    CONNECT_REQUEST = "CONNECT_REQUEST"
    CONNECT_ACK = "CONNECT_ACK"
    HANDSHAKE = "HANDSHAKE"
    HANDSHAKE_RESPONSE = "HANDSHAKE_RESPONSE"
    INSTRUCTION_ASSIGNMENT = "INSTRUCTION_ASSIGNMENT"
    EXECUTION_CONFIRMATION = "EXECUTION_CONFIRMATION"
    TASK_COMPLETE = "TASK_COMPLETE"
    DISCONNECT_REQUEST = "DISCONNECT_REQUEST"
    DISCONNECT_ACK = "DISCONNECT_ACK"

class Node:
    def __init__(self, node_id, port):
        self.node_id = node_id
        self.port = port
        self.peers = {}  # {peer_id: (ip, port)}
        self.connections = {}  # Active connections {peer_id: socket}
        self.instruction_types = set()
        self.assigned_instructions = []
        self.execution_order = []
        self.is_master = False
        self.sequence_counter = 0
        self.lock = threading.Lock()
        
        # Multicast setup for discovery
        self.multicast_group = ('224.3.29.71', 10000)
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.settimeout(0.2)
        self.udp_socket.bind(('0.0.0.0', port))
        
        # TCP server for connections
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.bind(('0.0.0.0', port))
        self.tcp_socket.listen(5)
        
        print(f"Node {node_id} started on port {port}")

    def start(self):
        # Start listening threads
        threading.Thread(target=self.listen_udp, daemon=True).start()
        threading.Thread(target=self.listen_tcp, daemon=True).start()
        
        # Start periodic peer exchange
        threading.Thread(target=self.periodic_peer_exchange, daemon=True).start()
        
        # Initial discovery
        self.broadcast_discovery()

    def broadcast_discovery(self):
        """Broadcast discovery message to find other nodes"""
        message = {
            "type": MessageType.DISCOVER.name,
            "sender_id": self.node_id,
            "sender_port": self.port
        }
        self.udp_socket.sendto(json.dumps(message).encode(), self.multicast_group)
        print(f"Node {self.node_id} sent DISCOVER")

    def listen_udp(self):
        """Listen for UDP multicast messages"""
        while True:
            try:
                data, addr = self.udp_socket.recvfrom(1024)
                message = json.loads(data.decode())
                self.handle_message(message, addr)
            except socket.timeout:
                continue
            except Exception as e:
                print(f"Error in UDP listener: {e}")

    def listen_tcp(self):
        """Listen for TCP connections"""
        while True:
            try:
                conn, addr = self.tcp_socket.accept()
                threading.Thread(target=self.handle_tcp_connection, args=(conn,)).start()
            except Exception as e:
                print(f"Error in TCP listener: {e}")

    def handle_tcp_connection(self, conn):
        """Handle incoming TCP connection"""
        try:
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                message = json.loads(data.decode())
                self.handle_message(message)
        except Exception as e:
            print(f"Error handling TCP connection: {e}")
        finally:
            conn.close()

    def handle_message(self, message, addr=None):
        """Handle incoming messages based on type"""
        msg_type = MessageType[message["type"]]
        
        if msg_type == MessageType.DISCOVER:
            self.handle_discover(message, addr)
        elif msg_type == MessageType.HELLO:
            self.handle_hello(message)
        elif msg_type == MessageType.PEER_EXCHANGE:
            self.handle_peer_exchange(message)
        elif msg_type == MessageType.CONNECT_REQUEST:
            self.handle_connect_request(message)
        elif msg_type == MessageType.CONNECT_ACK:
            self.handle_connect_ack(message)
        elif msg_type == MessageType.HANDSHAKE:
            self.handle_handshake(message)
        elif msg_type == MessageType.HANDSHAKE_RESPONSE:
            self.handle_handshake_response(message)
        elif msg_type == MessageType.INSTRUCTION_ASSIGNMENT:
            self.handle_instruction_assignment(message)
        elif msg_type == MessageType.EXECUTION_CONFIRMATION:
            self.handle_execution_confirmation(message)
        elif msg_type == MessageType.TASK_COMPLETE:
            self.handle_task_complete()
        elif msg_type == MessageType.DISCONNECT_REQUEST:
            self.handle_disconnect_request(message)
        elif msg_type == MessageType.DISCONNECT_ACK:
            self.handle_disconnect_ack(message)

    # Protocol handlers
    def handle_discover(self, message, addr):
        """Respond to discovery message"""
        if message["sender_id"] != self.node_id:
            response = {
                "type": MessageType.HELLO.name,
                "sender_id": self.node_id,
                "sender_port": self.port,
                "instruction_types": list(self.instruction_types)
            }
            self.udp_socket.sendto(json.dumps(response).encode(), addr)
            print(f"Node {self.node_id} responded to DISCOVER from {message['sender_id']}")

    def handle_hello(self, message):
        """Add discovered peer to list"""
        with self.lock:
            if message["sender_id"] not in self.peers:
                self.peers[message["sender_id"]] = ('localhost', message["sender_port"])
                print(f"Node {self.node_id} discovered peer {message['sender_id']}")

    def periodic_peer_exchange(self):
        """Periodically exchange peer lists with known peers"""
        while True:
            time.sleep(10)
            with self.lock:
                if self.peers:
                    message = {
                        "type": MessageType.PEER_EXCHANGE.name,
                        "sender_id": self.node_id,
                        "peers": list(self.peers.keys())
                    }
                    for peer_id, (ip, port) in self.peers.items():
                        try:
                            self.send_tcp_message(ip, port, message)
                        except:
                            continue

    def handle_peer_exchange(self, message):
        """Update peer list from received peer exchange"""
        with self.lock:
            for peer_id in message["peers"]:
                if peer_id not in self.peers and peer_id != self.node_id:
                    # We don't know the port of these peers, so we can't connect directly
                    # In a real implementation, we'd include connection info
                    self.peers[peer_id] = ('localhost', random.randint(10000, 20000))
                    print(f"Node {self.node_id} learned about peer {peer_id} from peer exchange")

    def connect_to_peer(self, peer_id):
        """Establish connection to another peer"""
        if peer_id in self.connections:
            return True
            
        ip, port = self.peers.get(peer_id, (None, None))
        if not ip or not port:
            return False
            
        try:
            # Step 1: Send CONNECT_REQUEST
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip, port))
            
            connect_request = {
                "type": MessageType.CONNECT_REQUEST.name,
                "sender_id": self.node_id
            }
            sock.send(json.dumps(connect_request).encode())
            
            # Step 2: Wait for CONNECT_ACK
            data = sock.recv(1024)
            if not data:
                return False
                
            ack = json.loads(data.decode())
            if MessageType[ack["type"]] != MessageType.CONNECT_ACK:
                return False
                
            # Step 3: Send HANDSHAKE
            handshake = {
                "type": MessageType.HANDSHAKE.name,
                "sender_id": self.node_id,
                "instruction_types": list(self.instruction_types)
            }
            sock.send(json.dumps(handshake).encode())
            
            # Step 4: Wait for HANDSHAKE_RESPONSE
            data = sock.recv(1024)
            if not data:
                return False
                
            response = json.loads(data.decode())
            if MessageType[response["type"]] != MessageType.HANDSHAKE_RESPONSE:
                return False
                
            # Connection established
            self.connections[peer_id] = sock
            threading.Thread(target=self.listen_for_messages, args=(sock, peer_id)).start()
            return True
            
        except Exception as e:
            print(f"Connection to {peer_id} failed: {e}")
            return False

    def handle_connect_request(self, message):
        """Handle incoming connection request"""
        peer_id = message["sender_id"]
        if peer_id in self.connections:
            return
            
        try:
            # Create new socket for this connection
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(('0.0.0.0', 0))
            sock.listen(1)
            
            # Send CONNECT_ACK
            ack = {
                "type": MessageType.CONNECT_ACK.name,
                "sender_id": self.node_id
            }
            self.send_tcp_message('localhost', message.get('sender_port', self.port), ack)
            
            # Wait for HANDSHAKE
            conn, addr = sock.accept()
            data = conn.recv(1024)
            handshake = json.loads(data.decode())
            
            if MessageType[handshake["type"]] == MessageType.HANDSHAKE:
                # Send HANDSHAKE_RESPONSE
                response = {
                    "type": MessageType.HANDSHAKE_RESPONSE.name,
                    "sender_id": self.node_id
                }
                conn.send(json.dumps(response).encode())
                
                # Connection established
                self.connections[peer_id] = conn
                threading.Thread(target=self.listen_for_messages, args=(conn, peer_id)).start()
                
        except Exception as e:
            print(f"Error handling connection request: {e}")

    def listen_for_messages(self, sock, peer_id):
        """Listen for messages on an established connection"""
        try:
            while True:
                data = sock.recv(1024)
                if not data:
                    break
                message = json.loads(data.decode())
                self.handle_message(message)
        except:
            pass
        finally:
            with self.lock:
                if peer_id in self.connections:
                    del self.connections[peer_id]

    def send_tcp_message(self, ip, port, message):
        """Send a TCP message to a specific address"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip, port))
            sock.send(json.dumps(message).encode())
            sock.close()
            return True
        except:
            return False

    # Task execution handlers
    def handle_instruction_assignment(self, message):
        """Handle instructions assigned by master"""
        instructions = message.get("instructions", [])
        self.assigned_instructions = instructions
        print(f"Node {self.node_id} received instructions: {instructions}")
        
        # Execute instructions in order
        for instr in instructions:
            time.sleep(1)  # Simulate work
            print(f"Node {self.node_id} executing: {instr}")
            
            # Send confirmation to master
            confirmation = {
                "type": MessageType.EXECUTION_CONFIRMATION.name,
                "sender_id": self.node_id,
                "instruction": instr,
                "sequence": message.get("sequence", 0)
            }
            self.send_to_peer(message["sender_id"], confirmation)

    def handle_execution_confirmation(self, message):
        """Master node tracks execution confirmations"""
        if self.is_master:
            self.execution_order.append({
                "instruction": message["instruction"],
                "node": message["sender_id"],
                "sequence": message["sequence"]
            })
            print(f"Master received confirmation for {message['instruction']} from {message['sender_id']}")

    def handle_task_complete(self):
        """Handle task completion notification"""
        print(f"Node {self.node_id} received TASK_COMPLETE")
        # Clean up resources
        self.assigned_instructions = []

    # Connection termination handlers
    def handle_disconnect_request(self, message):
        """Handle disconnect request"""
        peer_id = message["sender_id"]
        if peer_id in self.connections:
            self.connections[peer_id].close()
            del self.connections[peer_id]
            
        # Send acknowledgment
        ack = {
            "type": MessageType.DISCONNECT_ACK.name,
            "sender_id": self.node_id
        }
        self.send_to_peer(peer_id, ack)

    def handle_disconnect_ack(self, message):
        """Handle disconnect acknowledgment"""
        peer_id = message["sender_id"]
        if peer_id in self.connections:
            self.connections[peer_id].close()
            del self.connections[peer_id]

    def send_to_peer(self, peer_id, message):
        """Send message to a specific peer"""
        if peer_id in self.connections:
            try:
                self.connections[peer_id].send(json.dumps(message).encode())
                return True
            except:
                del self.connections[peer_id]
                return False
        return False

    # Master node functions
    def become_master(self, instructions):
        """This node becomes the master and coordinates execution"""
        self.is_master = True
        self.assigned_instructions = instructions
        
        # Parse instructions and group by type
        instruction_groups = defaultdict(list)
        for i, instr in enumerate(instructions):
            instr_type = instr[0]  # First character is type (a1 -> 'a')
            instruction_groups[instr_type].append((i, instr))
            if instr_type not in self.instruction_types:
                self.instruction_types.add(instr_type)
        
        # Find peers for each instruction type
        for instr_type, group in instruction_groups.items():
            if instr_type == self.node_id[0]:  # If we can handle this type
                continue
                
            # Find a peer that can handle this type
            for peer_id in self.peers:
                if peer_id[0] == instr_type:  # Simple type matching by first char
                    if self.connect_to_peer(peer_id):
                        # Send instructions
                        assignment = {
                            "type": MessageType.INSTRUCTION_ASSIGNMENT.name,
                            "sender_id": self.node_id,
                            "instructions": [instr for (seq, instr) in group],
                            "sequence": self.sequence_counter
                        }
                        self.sequence_counter += 1
                        self.send_to_peer(peer_id, assignment)
                        break
        
        # Handle our own instruction type
        our_type = self.node_id[0]
        if our_type in instruction_groups:
            our_instructions = [instr for (seq, instr) in instruction_groups[our_type]]
            self.handle_instruction_assignment({
                "type": MessageType.INSTRUCTION_ASSIGNMENT.name,
                "sender_id": self.node_id,
                "instructions": our_instructions,
                "sequence": self.sequence_counter
            })
            self.sequence_counter += 1

    def print_execution_summary(self):
        """Print execution order summary"""
        if self.is_master and self.execution_order:
            print("\nExecution Summary:")
            for item in sorted(self.execution_order, key=lambda x: x["sequence"]):
                print(f"{item['instruction']} (by {item['node']})")

# Helper function to start nodes
def start_node(node_id, port, instructions=None):
    node = Node(node_id, port)
    node.instruction_types.add(node_id[0])  # First char is instruction type
    
    # Simple way to determine if this node is the master
    if instructions:
        node.become_master(instructions)
    
    node.start()
    return node

# Example usage
if __name__ == "__main__":
    # Example instructions
    instructions = ["a1", "a2", "b1", "c1", "a3", "b2", "c2", "b3", "a4", "c3", "b4"]
    
    # Start nodes in separate threads (in reality these would be separate processes)
    import threading
    
    # Master node (can handle 'a' type instructions)
    master_thread = threading.Thread(target=start_node, args=("a1", 10001, instructions))
    master_thread.daemon = True
    master_thread.start()
    
    # Worker nodes
    time.sleep(1)
    worker_b = threading.Thread(target=start_node, args=("b1", 10002))
    worker_b.daemon = True
    worker_b.start()
    
    worker_c = threading.Thread(target=start_node, args=("c1", 10003))
    worker_c.daemon = True
    worker_c.start()
    
    # Let the nodes run for a while
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")