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
#include <sys/time.h>
#include <unistd.h>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;
int global_speed = 0;
int global_delay = 0;

struct packet_info {
    char data[MAX_SEGMENT_SIZE];
    int size;
    long long send_time_ms;
};

std::map<int, std::map<int, packet_info>> unacked_packets;
std::map<int, int> base_seq; 
std::map<int, int> next_seq; 

long long get_current_time_ms() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec * 1000LL) + (tv.tv_usec / 1000LL);
}

int send_data(int conn_id, char *buffer, int len)
{
    struct connection *con = cons[conn_id];
    int bytes_sent = 0;

    if (next_seq.find(conn_id) == next_seq.end()) {
        next_seq[conn_id] = 0;
        base_seq[conn_id] = 0;
    }

    while (bytes_sent < len) {
        pthread_mutex_lock(&con->con_lock);

        /* We will write code here as to not have sync problems with sender_handler */
        if (next_seq[conn_id] >= base_seq[conn_id] + con->max_window_seq) {
            pthread_mutex_unlock(&con->con_lock);
            usleep(1000); 
            continue;
        }

        int chunk_size = len - bytes_sent;
        if (chunk_size > MAX_DATA_SIZE) {
            chunk_size = MAX_DATA_SIZE;
        }

        packet_info pkt;
        pkt.size = sizeof(poli_tcp_data_hdr) + chunk_size;
        
        poli_tcp_data_hdr *hdr = (poli_tcp_data_hdr *)pkt.data;
        hdr->protocol_id = POLI_PROTOCOL_ID;
        hdr->conn_id = conn_id;
        hdr->type = 4;
        hdr->seq_num = next_seq[conn_id];
        hdr->len = chunk_size;

        memcpy(pkt.data + sizeof(poli_tcp_data_hdr), buffer + bytes_sent, chunk_size);

        pkt.send_time_ms = get_current_time_ms();
        unacked_packets[conn_id][next_seq[conn_id]] = pkt;

        sendto(con->sockfd, pkt.data, pkt.size, 0, (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));

        next_seq[conn_id]++;
        bytes_sent += chunk_size;

        pthread_mutex_unlock(&con->con_lock);
    }

    return bytes_sent;
}

void *sender_handler(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];

    while (1) {

        if (cons.size() == 0) {
            usleep(1000);
            continue;
        }
        int conn_id = -1;

        /* Handle segment received from the receiver. We use this between locks
        as to not have synchronization issues with the send_data calls which are
        on the main thread */
        res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);

        if (res > 0 && conn_id >= 0 && cons.find(conn_id) != cons.end()) {
            pthread_mutex_lock(&cons[conn_id]->con_lock);

            poli_tcp_ctrl_hdr *hdr = (poli_tcp_ctrl_hdr *)buf;
            if (hdr->type == 3) { 
                int ack_num = hdr->ack_num;
                unacked_packets[conn_id].erase(ack_num);

                while (next_seq[conn_id] > base_seq[conn_id] && 
                       unacked_packets[conn_id].find(base_seq[conn_id]) == unacked_packets[conn_id].end()) {
                    base_seq[conn_id]++;
                }
            }
            pthread_mutex_unlock(&cons[conn_id]->con_lock);
        }

        long long now = get_current_time_ms();
        for (auto const& [cid, con] : cons) {
            pthread_mutex_lock(&con->con_lock);
            
            for (auto& [seq, pkt] : unacked_packets[cid]) {
                // RTO FORȚAT LA 10ms - Metoda originală care funcționa
                if (now - pkt.send_time_ms > 10) {
                    sendto(con->sockfd, pkt.data, pkt.size, 0,
                           (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
                    pkt.send_time_ms = now;
                }
            }
            pthread_mutex_unlock(&con->con_lock);
        }
    }
    return NULL;
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

    /* We will send the SYN on 8032. Then we will receive a SYN-ACK with the connection
     * port. We can use con->sockfd for both cases, but we will need to update server_addr
     * with the port received via SYN-ACK */

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = port;
    server_addr.sin_addr.s_addr = ip;

    poli_tcp_ctrl_hdr header;
    header.type = 1; 
    header.protocol_id = POLI_PROTOCOL_ID;

    char buffer[MAX_SEGMENT_SIZE];
    struct sockaddr_in from_addr;
    socklen_t from_len = sizeof(from_addr);
    uint16_t new_port = 0;

    while (true) {

        sendto(con->sockfd, &header, sizeof(header), 0, (const struct sockaddr *) &server_addr, sizeof(server_addr));
        
        struct pollfd pfd;
        pfd.fd = con->sockfd;
        pfd.events = POLLIN;
        int p = poll(&pfd, 1, 10);
        if (p > 0) {
            int n = recvfrom(con->sockfd, buffer, MAX_SEGMENT_SIZE, 0, (struct sockaddr *)&from_addr, &from_len);
            if (n > 0) {
                poli_tcp_ctrl_hdr *recv_hdr = (poli_tcp_ctrl_hdr *)buffer;
                if (recv_hdr->type == 2) { 
                    conn_id = recv_hdr->conn_id;
                    con->recv_window_bytes = recv_hdr->recv_window;
                    memcpy(&new_port, buffer + sizeof(poli_tcp_ctrl_hdr), sizeof(uint16_t));
                    break;
                }
            }
        }
    }

    server_addr.sin_port = htons(new_port);
    con->servaddr = server_addr;
    poli_tcp_ctrl_hdr syn_ack_header;
    syn_ack_header.protocol_id = POLI_PROTOCOL_ID;
    syn_ack_header.conn_id = conn_id;
    syn_ack_header.type = 3;

    sendto(con->sockfd, &syn_ack_header, sizeof(syn_ack_header), 0, (const struct sockaddr *) &server_addr, sizeof(server_addr));

    con->conn_id = conn_id;
    con->state = 2;

    int sliding_window_bytes = (global_speed * 1000000 / 8) * (global_delay * 2) / 1000;
    con->max_window_seq = sliding_window_bytes / MAX_SEGMENT_SIZE;
    
    if (con->max_window_seq < 10) {
        con->max_window_seq = 10;
    }
    // FĂRĂ LIMITA DE 50 AICI (era fix motivul pentru care te limita la 35 de puncte!)

    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    
    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on our connection */
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
