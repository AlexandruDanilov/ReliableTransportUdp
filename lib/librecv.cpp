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
#include <unistd.h>
#include <string>

using namespace std;

std::map<int, struct connection *> cons;
std::map<int, std::string> recv_buffers[MAX_CONNECTIONS];

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

    /* We will write code here as to not have sync problems with recv_handler */
    while (size == 0) {
        pthread_mutex_lock(&cons[conn_id]->con_lock);

        int expected_seq = cons[conn_id]->expected_sequence_number;

        if (recv_buffers[conn_id].count(expected_seq)) {
            std::string payload = recv_buffers[conn_id][expected_seq];

            int to_copy = len;
            if ((int)payload.size() < to_copy) {
                to_copy = payload.size();
            }

            memcpy(buffer, payload.data(), to_copy);
            size = to_copy;

            recv_buffers[conn_id].erase(expected_seq);
            cons[conn_id]->expected_sequence_number++;
        }

        pthread_mutex_unlock(&cons[conn_id]->con_lock);

        if (size == 0) {
            usleep(1000);
        }
    }

    return size;
}

void *receiver_handler(void *arg)
{

    char segment[MAX_SEGMENT_SIZE];
    int res;
    DEBUG_PRINT("Starting receiver handler\n");

    while (1) {

        int conn_id = -1;
        do {
            res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        if (res > 0 && conn_id >= 0 && cons.find(conn_id) != cons.end()) {
            
            pthread_mutex_lock(&cons[conn_id]->con_lock);

            /* Handle segment received from the sender. */
            poli_tcp_data_hdr *hdr = (poli_tcp_data_hdr *)segment;

            if (hdr->type == 4) {
                if (hdr->seq_num >= cons[conn_id]->expected_sequence_number) {
                    std::string payload(segment + sizeof(poli_tcp_data_hdr), hdr->len);
                    recv_buffers[conn_id][hdr->seq_num] = payload;
                }

                poli_tcp_ctrl_hdr ack_hdr = {};
                ack_hdr.protocol_id = POLI_PROTOCOL_ID;
                ack_hdr.conn_id = conn_id;
                ack_hdr.type = 3;
                ack_hdr.ack_num = hdr->seq_num;

                sendto(cons[conn_id]->sockfd, &ack_hdr, sizeof(ack_hdr), 0, 
                       (struct sockaddr *)&cons[conn_id]->servaddr, sizeof(cons[conn_id]->servaddr));
            }

            pthread_mutex_unlock(&cons[conn_id]->con_lock);
        }
    }
    
    return NULL;
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
        int n = recvfrom(listen_sockfd, buffer, MAX_SEGMENT_SIZE, 0, (struct sockaddr *) &cliaddr , &clilen);
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

    while (true) {
        sendto(listen_sockfd, syn_ack, sizeof(poli_tcp_ctrl_hdr) + sizeof(uint16_t), 0, (const struct sockaddr *) &cliaddr, clilen);

        struct pollfd pfd;
        pfd.fd = con->sockfd;
        pfd.events = POLLIN;
        int p = poll(&pfd, 1, 10);

        if (p > 0) {
            int n = recvfrom(con->sockfd, buffer, MAX_SEGMENT_SIZE, 0, (struct sockaddr *) &cliaddr , &clilen);
            if (n > 0) {
                poli_tcp_ctrl_hdr *header = (poli_tcp_ctrl_hdr *)buffer;
                if (header->type == 3) {
                    break;
                } else if (header->type == 4) {
                    poli_tcp_data_hdr *dhdr = (poli_tcp_data_hdr *)buffer;
                    std::string payload(buffer + sizeof(poli_tcp_data_hdr), dhdr->len);
                    recv_buffers[conn_id][dhdr->seq_num] = payload;

                    poli_tcp_ctrl_hdr ack_hdr = {};
                    ack_hdr.protocol_id = POLI_PROTOCOL_ID;
                    ack_hdr.conn_id = conn_id;
                    ack_hdr.type = 3;
                    ack_hdr.ack_num = dhdr->seq_num;
                    sendto(con->sockfd, &ack_hdr, sizeof(ack_hdr), 0, (struct sockaddr *)&cliaddr, clilen);
                    break;
                }
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
    spec.it_value.tv_sec = 0;    
    spec.it_value.tv_nsec = 10000000;    
    spec.it_interval.tv_sec = 0;    
    spec.it_interval.tv_nsec = 10000000;    
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
    server_addr.sin_port = htons(8032);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    ret = bind(listen_sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr));
    assert(ret == 0);

    ret = pthread_create( &thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}
