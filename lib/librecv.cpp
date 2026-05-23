#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <poll.h>
#include <cassert>
#include <sys/timerfd.h>
#include <cstring>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;
int listen_sockfd = -1;
int max_recv_buffer_bytes = 0;
int next_connection_id = 0;

int recv_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    pthread_mutex_lock(&cons[conn_id]->con_lock);
    
    /* We will write code here as to not have sync problems with recv_handler */

    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return size;
}

void *receiver_handler(void *arg)
{

    char segment[MAX_SEGMENT_SIZE];
    int res;
    DEBUG_PRINT("Starting recviver handler\n");

    while (1) {

        int conn_id = -1;
        do {
            res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* Handle segment received from the sender. We use this between locks
        as to not have synchronization issues with the recv_data calls which are
        on the main thread */

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }

    
}

int wait4connect(uint32_t ip, uint16_t port)
{
    /* TODO: Implement the Three Way Handshake on the receiver part. This blocks
     * until a connection is established. */

    struct connection *con = (struct connection *)malloc(sizeof(struct connection));
    int conn_id = next_connection_id++;

    /* This can be used to set a timer on a socket, useful once we received a
     * SYN. You may want to disable by setting the time to 0 (tv_sec = 0,
     * tv_usec = 0)
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    } */

    char buffer[MAX_SEGMENT_SIZE];
    struct sockaddr_in cliaddr;
    socklen_t clilen = sizeof(cliaddr);

    DEBUG_PRINT("Waiting for SYN\n");

    while (true) {
        int n = recvfrom(listen_sockfd, buffer, MAX_SEGMENT_SIZE, MSG_WAITALL, (struct sockaddr *) &cliaddr , &clilen);
        if (n > 0) {
            poli_tcp_ctrl_hdr *header = (poli_tcp_ctrl_hdr *)buffer;
            if (header->type == 1) {
                DEBUG_PRINT("Received SYN\n");
                break;
            }
            
        }
    }

    /* Receive SYN on the connection socket. Create a new socket and bind it to
     * the chosen port. Send the data port number via SYN-ACK to the client */
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in new_server_addr;
    new_server_addr.sin_family = AF_INET;
    new_server_addr.sin_port = htons(0);
    new_server_addr.sin_addr.s_addr = INADDR_ANY;
    bind(con->sockfd, (struct sockaddr *)&new_server_addr, sizeof(new_server_addr));

    socklen_t len = sizeof(new_server_addr);
    getsockname(con->sockfd, (struct sockaddr *)&new_server_addr, &len);
    int new_port = ntohs(new_server_addr.sin_port);

    char syn_ack[MAX_SEGMENT_SIZE];
    poli_tcp_ctrl_hdr *syn_ack_header = (poli_tcp_ctrl_hdr *)syn_ack;
    syn_ack_header->protocol_id = POLI_PROTOCOL_ID;
    syn_ack_header->conn_id = conn_id;
    syn_ack_header->type = 2;
    syn_ack_header->ack_num = 0;
    syn_ack_header->recv_window = max_recv_buffer_bytes;

    memcpy(syn_ack + sizeof(poli_tcp_ctrl_hdr), &new_port, sizeof(uint16_t));
    sendto(listen_sockfd, syn_ack, sizeof(poli_tcp_ctrl_hdr) + sizeof(uint16_t), MSG_CONFIRM, (const struct sockaddr *) &cliaddr, clilen);

    while (true) {
        int n = recvfrom(con->sockfd, buffer, MAX_SEGMENT_SIZE, MSG_WAITALL, (struct sockaddr *) &cliaddr , &clilen);
        if (n > 0) {
            poli_tcp_ctrl_hdr *header = (poli_tcp_ctrl_hdr *)buffer;
            if (header->type == 3) {
                DEBUG_PRINT("Received ACK, connection established\n");
                break;
            }
        }
    }

    con->servaddr = cliaddr;
    con->conn_id = conn_id;
    con->state = 2;
    con->expected_sequence_number = 0;
    con->recv_window_bytes = max_recv_buffer_bytes;

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    
    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on a connection */
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

void init_receiver(int recv_buffer_bytes)
{
    pthread_t thread1;
    int ret;

    /* TODO: Create the connection socket and bind it to 8031 */

    max_recv_buffer_bytes = recv_buffer_bytes;
    listen_sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    assert(listen_sockfd >= 0);
    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(8031);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    ret = bind(listen_sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr));
    assert(ret == 0);

    ret = pthread_create( &thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}
