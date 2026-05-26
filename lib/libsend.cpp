#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <cassert>
#include <poll.h>
#include <sys/timerfd.h>
#include <cstring>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;
int global_speed = 0;
int global_delay = 0;

int send_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    pthread_mutex_lock(&cons[conn_id]->con_lock);

    /* We will write code here as to not have sync problems with sender_handler */

    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return size;
}

void *sender_handler(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];

    while (1) {

        if (cons.size() == 0) {
            continue;
        }
        int conn_id = -1;
        do {
            res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* Handle segment received from the receiver. We use this between locks
        as to not have synchronization issues with the send_data calls which are
        on the main thread */

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
}

int setup_connection(uint32_t ip, uint16_t port)
{
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */

    struct connection *con = (struct connection *)malloc(sizeof(struct connection));
    int conn_id = 0;
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    /* // This can be used to set a timer on a socket 
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    } */

    /* We will send the SYN on 8031. Then we will receive a SYN-ACK with the connection
     * port. We can use con->sockfd for both cases, but we will need to update server_addr
     * with the port received via SYN-ACK */

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = port;
    server_addr.sin_addr.s_addr = ip;

    poli_tcp_ctrl_hdr header;
    header.type = 1; /* SYN */
    header.protocol_id = POLI_PROTOCOL_ID;
    sendto(con->sockfd, &header, sizeof(header), MSG_CONFIRM, (const struct sockaddr *) &server_addr, sizeof(server_addr));

    DEBUG_PRINT("SYN sent\n");

    char buffer[MAX_SEGMENT_SIZE];
    struct sockaddr_in from_addr;
    socklen_t from_len = sizeof(from_addr);
    uint16_t new_port;

    while (true) {
        int n = recvfrom(con->sockfd, buffer, MAX_SEGMENT_SIZE, 0, (struct sockaddr *)&from_addr, &from_len);
        if (n > 0) {
            poli_tcp_ctrl_hdr *header = (poli_tcp_ctrl_hdr *)buffer;
            if (header->type == 2) { 
                conn_id = header->conn_id;
                con->recv_window_bytes = header->recv_window;
                memcpy(&new_port, buffer + sizeof(poli_tcp_ctrl_hdr), sizeof(uint16_t));
                DEBUG_PRINT("Received SYN-ACK, new port is %d\n", new_port);
                break;
            }
        }
    }

    server_addr.sin_port = htons(new_port);
    con->servaddr = server_addr;
    poli_tcp_ctrl_hdr syn_ack_header;
    syn_ack_header.protocol_id = POLI_PROTOCOL_ID;
    syn_ack_header.conn_id = conn_id;
    syn_ack_header.type = 3;
    sendto(con->sockfd, &syn_ack_header, sizeof(syn_ack_header), MSG_CONFIRM, (const struct sockaddr *) &server_addr, sizeof(server_addr));

    DEBUG_PRINT("ACK sent, connection established\n");

    con->conn_id = conn_id;
    con->state = 2;

    int sliding_window_bytes = (global_speed * 1000000 / 8) * (global_delay * 2) / 1000;
    con->max_window_seq = sliding_window_bytes / MAX_SEGMENT_SIZE;
    if (con->max_window_seq < 10) {
        con->max_window_seq = 10;
    }

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    
    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on our connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
    struct itimerspec spec;     
    spec.it_value.tv_sec = 1;    
    spec.it_value.tv_nsec = 0;    
    spec.it_interval.tv_sec = 1;    
    spec.it_interval.tv_nsec = 0;    
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
    fdmax++;


    pthread_mutex_init(&con->con_lock, NULL);
    cons.insert({conn_id, con});

    DEBUG_PRINT("Connection established!");

    return conn_id;
}

void init_sender(int speed, int delay)
{
    pthread_t thread1;
    int ret;

    global_speed = speed;
    global_delay = delay;

    /* Create a thread that will*/
    ret = pthread_create( &thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}
