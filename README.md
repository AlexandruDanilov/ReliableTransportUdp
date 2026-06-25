# Reliable Transport over UDP
## General information
This project implements a custom protocol on top of UDP, which targets the issue of UDP's lack 
of reliability. It is designed to handle file and image transfer across a really bad network, 
with 5% packet loss. The protocol ensures that packets are delivered in order, without loss 
while operating on multiple threads.

## Main aspects of the protocol
### Connections:
The protocol uses the three-way handshake method to handle the connection between the client
and the server. 
The client sends a SYN connection request to the server port 8032.
To support multiple clients without collision the server does UDP socket binding to an available
port and sends a SYN-ACK response to the client with the new port number. 
The client extracts the new port from the payload of the SYN-ACK response and sends another ACK
to the server to confirm the connection. After this, all the file transfer will be done on the 
new port, while the server can still listen for new clients on port 8032.

### Sliding window
In order to use the network efficiently, the protocol implements a sliding window mechanism. The 
sender can send multiple packets without waiting for an acknowledgment for each one, up to a certain 
window size. This allows for better throughput, especially in high-latency networks. The receiver 
acknowledges the packets it receives, and the sender slides the window forward as ACK messages are 
received. This mechanism helps to keep the network full and reduces the idle time of the sender.
The maximum window size is dynamically calculated based on BDP to ensure the link is used in an optimal
way.

### Selective repeat ARQ
In order for the packet loss to not cause performance issues, the protocol implements selective repeat ARQ.
When a packet arrives late (out of order), the receiver buffers it inside a map. Once the sequence 
is complete, the buffered data is reassembled in the correct order and written to the output.
The sender keeps track of the packets with no ACK, and when an ACK is received, it removes the 
packet from the respective map, allowing the sliding window to advance regardless of the order of 
the packets.

### Retransmission
To fix the issues raised by the highly unstable mininet network, the protocol uses a rather fast retransmission
mechanism, where a dedicated thread monitors the map of the packets with no ACK. The respective thread
checks the time elapsed since the packet was sent, using gettimeofday. If the packet has no ACK for more than
10ms, it is retransmitted. This allows the protocol to quickly recover from packet loss and maintain a 
good throughput.

### Memory safety
To fix most of the bugs, everything is initialized to zero, all structures are zeroed using ={} and
all new allocations are made using calloc. This ensures that there are no uninitialized variables that
could cause undefined behavior.

### Conclusion
Overall, the protocol is designed to provide reliable data transfer over UDP, even in the presence of
packet loss. By implementing a three-way handshake, sliding window, selective repeat ARQ, and a 
fast retransmission mechanism, the protocol can efficiently handle file and image transfer across a 
bad network.

### Known issues
Mininet simulates a really bad network, with 5% loss, which is quite high. This causes a lot of debugging
issues, since the checker does not perform twice the same. Timeouts are really harsh and the run depends 
on the machine so sometimes it passes all tests, sometimes it fails some or most of them. This made it really
difficult to tell if small changes were improvements. In real life, the protocol can be adapted to the network
by changing the timeout values until the performance is good.
Furthermore, the task requirement for the protocol had certain aspects that were not very clear (for example,
the task recommends Cumulative ACK, and further down the line it recommends Selective Repeat ARQ, which is 
not compatible with Cumulative ACK).
Due to the high instability of the checker itself, there might be multiple uploads with the same archive just
so that I can get the same result I get locally, which is not ideal.

