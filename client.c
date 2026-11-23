/*
 * Chloe McGrigg
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>
#include <poll.h>
#include <signal.h>
#include <netdb.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#define MAXPEND 1024
#define MAXID 8192
#define MAX_TEXT 1024
#define DL_DIR "client_downloads"

typedef struct {
    char type;               /* R request, P put, G get */
    int id;
    char text[MAX_TEXT];     /* request text, file path, or file name */
} item_t;

static item_t pend[MAXPEND];     /* eequests waiting to be sent */
static int p_head, p_count;
static int next_id = 1;
static unsigned char state[MAXID];  /* 1 sent, 2 acked, 3 done */
static item_t sent_copy[MAXID];     /* saved requests can be added back if rejected */

static int sock = -1;
static int server_space = -1;    /* most recent free-space value from the server */
static int awaiting_ack, awaiting_resp;
static int maint, want_quit, bye_sent, stdin_eof;
static long sleep_until;

static long now_ms(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec * 1000L + t.tv_nsec / 1000000L;
}

static int write_all(int fd, const void *b, size_t n) {
    const char *p = b;
    while (n > 0) {
        ssize_t w = send(fd, p, n, MSG_NOSIGNAL);
        if (w < 0) { if (errno == EINTR) continue; return -1; }
        p += w; n -= (size_t)w;
    }
    return 0;
}

static int read_exact(int fd, void *b, size_t n) {
    char *p = b;
    while (n > 0) {
        ssize_t r = recv(fd, p, n, 0);
        if (r <= 0) { if (r < 0 && errno == EINTR) continue; return -1; }
        p += r; n -= (size_t)r;
    }
    return 0;
}

static int recv_line(int fd, char *buf, size_t max) {
    size_t n = 0;
    while (n + 1 < max) {
        char c; ssize_t r = recv(fd, &c, 1, 0);
        if (r <= 0) { if (r < 0 && errno == EINTR) continue; return -1; }
        if (c == '\n') break;
        buf[n++] = c;
    }
    buf[n] = 0;
    return 0;
}

/* account for requests that have not been acknowledged yet */
static int effective_space(void) {
    if (server_space < 0) return 0;
    return server_space - awaiting_ack;
}

static void enqueue(char type, const char *text) {
    if (p_count >= MAXPEND) { printf("[client] local buffer full, dropped\n"); return; }
    item_t *it = &pend[(p_head + p_count) % MAXPEND];
    it->type = type;
    it->id = next_id++;
    snprintf(it->text, sizeof it->text, "%s", text);
    p_count++;
    if (it->id < MAXID) state[it->id] = 0;
}

static void push_front(const item_t *it) {
    p_head = (p_head + MAXPEND - 1) % MAXPEND;
    pend[p_head] = *it;
    p_count++;
}

static const char *base_name(const char *p) {
    const char *b = strrchr(p, '/');
    return b ? b + 1 : p;
}

static int send_item(const item_t *it) {
    char hdr[600];
    if (it->type == 'R') {
        int h = snprintf(hdr, sizeof hdr, "REQ %d - %zu\n", it->id, strlen(it->text));
        if (write_all(sock, hdr, (size_t)h) || write_all(sock, it->text, strlen(it->text))) return -1;
    } else if (it->type == 'G') {
        int h = snprintf(hdr, sizeof hdr, "GET %d %s 0\n", it->id, it->text);
        if (write_all(sock, hdr, (size_t)h)) return -1;
    } else {
        FILE *f = fopen(it->text, "rb");
        if (!f) { printf("[client] cannot open '%s' - upload %d skipped\n", it->text, it->id); return 1; }
        fseek(f, 0, SEEK_END);
        long sz = ftell(f);
        fseek(f, 0, SEEK_SET);
        char name[300];
        snprintf(name, sizeof name, "%s", base_name(it->text));
        for (char *c = name; *c; c++) if (*c == ' ') *c = '_';
        int h = snprintf(hdr, sizeof hdr, "PUT %d %s %ld\n", it->id, name, sz);
        if (write_all(sock, hdr, (size_t)h)) { fclose(f); return -1; }
        char chunk[65536];
        size_t n;
        while ((n = fread(chunk, 1, sizeof chunk, f)) > 0)
            if (write_all(sock, chunk, n)) { fclose(f); return -1; }
        fclose(f);
    }
    return 0;
}

/* send queued requests in order while the server has room */
static void try_flush(void) {
    while (!maint && !bye_sent && p_count > 0 && effective_space() > 0) {
        item_t it = pend[p_head];
        p_head = (p_head + 1) % MAXPEND;
        p_count--;
        int r = send_item(&it);
        if (r < 0) { printf("[client] send failed\n"); return; }
        if (r > 0) continue;
        if (it.id < MAXID) { state[it.id] = 1; sent_copy[it.id] = it; }
        awaiting_ack++;
        printf("[client] sent %s #%d (server buffer space was %d, %d unacked)\n",
               it.type == 'R' ? "REQ" : it.type == 'P' ? "PUT" : "GET", it.id, server_space, awaiting_ack);
    }
    if (p_count > 0 && !maint)
        printf("[client] %d request(s) held in local buffer (server buffer space %d, %d unacked)\n",
               p_count, server_space, awaiting_ack);
}

static void save_file(const char *name, const char *data, size_t len) {
    mkdir(DL_DIR, 0755);
    char path[600];
    snprintf(path, sizeof path, "%s/%s", DL_DIR, name);
    FILE *f = fopen(path, "wb");
    if (f) { fwrite(data, 1, len, f); fclose(f); }
    printf("[client] saved file %s (%zu bytes)\n", path, len);
}

/* read a server message, including its header and data */
static int handle_server(void) {
    char l1[128], l2[512];
    if (recv_line(sock, l1, sizeof l1) < 0) return -1;
    if (recv_line(sock, l2, sizeof l2) < 0) return -1;
    int sp = -1;
    if (sscanf(l1, "SPACE %d", &sp) == 1) server_space = sp;

    char kind[16], name[300]; int id; size_t len;
    if (sscanf(l2, "%15s %d %299s %zu", kind, &id, name, &len) != 4) return -1;
    char *body = malloc(len + 1);
    if (!body) return -1;
    if (len && read_exact(sock, body, len) < 0) { free(body); return -1; }
    body[len] = 0;

    printf("[client] <- %s %d  (server buffer space = %d)\n", kind, id, server_space);

    if (!strcmp(kind, "ACK")) {
        if (id > 0 && id < MAXID && state[id] == 1) { state[id] = 2; awaiting_ack--; awaiting_resp++; }
        printf("[client]    request %d accepted by server (%s)\n", id, name);
    } else if (!strcmp(kind, "REJECT")) {
        if (id > 0 && id < MAXID && state[id] == 1) { state[id] = 0; awaiting_ack--; }
        printf("[client]    request %d rejected (%s) - kept in local buffer\n", id, name);
        /* retry this request before sending newer ones */
        if (id > 0 && id < MAXID) push_front(&sent_copy[id]);
    } else if (!strcmp(kind, "RESP")) {
        if (id > 0 && id < MAXID && state[id] == 2) { state[id] = 3; awaiting_resp--; }
        printf("[client]    response to %d: %s\n", id, body);
    } else if (!strcmp(kind, "FILE")) {
        if (id > 0 && id < MAXID && state[id] == 2) { state[id] = 3; awaiting_resp--; }
        printf("[client]    response to %d is a file '%s'\n", id, name);
        save_file(name, body, len);
    } else if (!strcmp(kind, "ERR")) {
        if (id > 0 && id < MAXID && state[id] == 2) { state[id] = 3; awaiting_resp--; }
        printf("[client]    error for %d: %s\n", id, body);
    } else if (!strcmp(kind, "MAINT")) {
        maint = 1;
        printf("[client]    server announced MAINTENANCE: no more requests will be sent\n");
    } else if (!strcmp(kind, "CLOSE")) {
        printf("[client]    server finished all work and asks to close; sending BYE\n");
        char h[64];
        int n = snprintf(h, sizeof h, "BYE %d - 0\n", next_id++);
        write_all(sock, h, (size_t)n);
        bye_sent = 1;
    } else if (!strcmp(kind, "BYEACK")) {
        free(body);
        return 1;
    }
    free(body);
    return 0;
}

/* input from the user */
static char inbuf[8192];
static size_t inlen;

static void handle_command(char *line) {
    while (*line == ' ') line++;
    size_t n = strlen(line);
    while (n > 0 && (line[n - 1] == '\r' || line[n - 1] == ' ')) line[--n] = 0;
    if (!*line) return;
    char cmd[16] = "";
    sscanf(line, "%15s", cmd);
    char *rest = line + strlen(cmd);
    while (*rest == ' ') rest++;

    if (!strcmp(cmd, "req") && *rest) enqueue('R', rest);
    else if (!strcmp(cmd, "put") && *rest) enqueue('P', rest);
    else if (!strcmp(cmd, "get") && *rest) enqueue('G', rest);
    else if (!strcmp(cmd, "sleep")) { sleep_until = now_ms() + atol(rest) * 1000L; return; }
    else if (!strcmp(cmd, "quit")) { want_quit = 1; return; }
    else { printf("[client] unknown command: %s\n", line); return; }

    if (maint) printf("[client] server is in maintenance; request stored locally only\n");
    try_flush();
}

static void process_input(void) {
    for (;;) {
        if (now_ms() < sleep_until) return;
        char *nl = memchr(inbuf, '\n', inlen);
        if (!nl) {
            if (stdin_eof && inlen > 0) { inbuf[inlen] = 0; inlen = 0; handle_command(inbuf); continue; }
            return;
        }
        *nl = 0;
        char line[8192];
        snprintf(line, sizeof line, "%s", inbuf);
        size_t used = (size_t)(nl - inbuf) + 1;
        memmove(inbuf, inbuf + used, inlen - used);
        inlen -= used;
        handle_command(line);
    }
}

int main(int argc, char **argv) {
    const char *host = argc > 1 ? argv[1] : "127.0.0.1";
    const char *port = argc > 2 ? argv[2] : "5050";
    setvbuf(stdout, NULL, _IOLBF, 0);
    signal(SIGPIPE, SIG_IGN);

    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host, port, &hints, &res) != 0) { fprintf(stderr, "cannot resolve %s\n", host); return 1; }
    sock = socket(res->ai_family, res->ai_socktype, 0);
    if (sock < 0 || connect(sock, res->ai_addr, res->ai_addrlen) < 0) { perror("connect"); return 1; }
    freeaddrinfo(res);
    printf("[client] connected to %s:%s\n", host, port);

    int graceful = 0;
    for (;;) {
        process_input();

        if (want_quit && !bye_sent && p_count == 0 && awaiting_ack == 0 && awaiting_resp == 0) {
            char h[64];
            int n = snprintf(h, sizeof h, "BYE %d - 0\n", next_id++);
            write_all(sock, h, (size_t)n);
            bye_sent = 1;
            printf("[client] all work finished; sent BYE\n");
        }
        if (stdin_eof && inlen == 0 && now_ms() >= sleep_until) want_quit = 1;

        struct pollfd p[2];
        p[0].fd = sock; p[0].events = POLLIN;
        p[1].fd = stdin_eof ? -1 : 0; p[1].events = POLLIN;
        int r = poll(p, 2, 100);
        if (r < 0) { if (errno == EINTR) continue; break; }

        if (p[0].revents & (POLLIN | POLLHUP)) {
            int h = handle_server();
            if (h < 0) { printf("[client] connection lost\n"); break; }
            if (h == 1) { graceful = 1; break; }
            try_flush();
        }
        if (!stdin_eof && (p[1].revents & (POLLIN | POLLHUP))) {
            ssize_t n = read(0, inbuf + inlen, sizeof inbuf - inlen - 1);
            if (n <= 0) stdin_eof = 1;
            else inlen += (size_t)n;
        }
    }

    if (graceful) {
        /* finish the client's side of the TCP connection */
        shutdown(sock, SHUT_WR);
        char tmp[256];
        struct pollfd q = { sock, POLLIN, 0 };
        for (int i = 0; i < 25; i++)
            if (poll(&q, 1, 200) > 0 && recv(sock, tmp, sizeof tmp, 0) <= 0) break;
        printf("[client] connection closed gracefully");
        if (p_count > 0) printf(" (%d unsent request(s) were still in the local buffer)", p_count);
        printf("\n");
    }
    close(sock);
    return 0;
}
