/*
 * Chloe McGrigg
 * Student ID: 11612794
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <poll.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define MAXQ 64
#define MAX_TEXT 1024
#define MAX_NAME 256
#define MAX_FILE (64L * 1024 * 1024)
#define FILE_DIR "server_files"

typedef struct {
    int id;
    char kind[8];          /* REQ, PUT or GET */
    char arg[MAX_NAME];    /* safe file name for PUT/GET */
    char text[MAX_TEXT];   /* request text, or temp path of an uploaded file */
} job_t;

static int port = 5050, num_workers = 2, buf_cap = 4, proc_secs = 2, maint_after = 0;

/* shared worker and queue info */
static pthread_mutex_t st_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t st_cv = PTHREAD_COND_INITIALIZER;
static job_t buf_q[MAXQ];   /* requests waiting for a worker */
static job_t dir_q[MAXQ];   /* requests assigned to available workers */
static int buf_head, buf_count, dir_head, dir_count;
static int free_workers;    /* num of workers available */
static int outstanding;     /* accepted requests still being processed */
static int shutting_down;

/* protect writes so messages do not overlap */
static pthread_mutex_t wr_mu = PTHREAD_MUTEX_INITIALIZER;
static int client_fd = -1;

static volatile sig_atomic_t maint_flag = 0;
static void on_usr1(int s) { (void)s; maint_flag = 1; }

/* socket read and write helpers */
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
        if (r == 0) return -1;
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        p += r; n -= (size_t)r;
    }
    return 0;
}

/* returns 1 = line read, 0 = EOF, -1 = error */
static int recv_line(int fd, char *buf, size_t max) {
    size_t n = 0;
    while (n + 1 < max) {
        char c; ssize_t r = recv(fd, &c, 1, 0);
        if (r == 0) return n == 0 ? 0 : -1;
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        if (c == '\n') break;
        buf[n++] = c;
    }
    buf[n] = 0;
    return 1;
}

static int current_space(void) {
    pthread_mutex_lock(&st_mu);
    int s = buf_cap - buf_count;
    pthread_mutex_unlock(&st_mu);
    return s;
}

/*
 * every server message has the same shape:
 *   SPACE <free buffer slots>\n
 *   <KIND> <id> <name-or-dash> <body length>\n
 *   <body bytes>
 * so the free buffer space is always the first line of every response
 */
static void send_frame(const char *kind, int id, const char *arg, const char *body, size_t len) {
    char hdr[512];
    pthread_mutex_lock(&wr_mu);
    int sp = current_space();
    int h = snprintf(hdr, sizeof hdr, "SPACE %d\n%s %d %s %zu\n", sp, kind, id,
                     (arg && arg[0]) ? arg : "-", len);
    if (client_fd >= 0) {
        write_all(client_fd, hdr, (size_t)h);
        if (len) write_all(client_fd, body, len);
    }
    pthread_mutex_unlock(&wr_mu);
}

static void make_safe_name(const char *in, char *out, size_t max) {
    const char *b = strrchr(in, '/');
    b = b ? b + 1 : in;
    size_t i = 0;
    for (; *b && i + 1 < max; b++) {
        char c = *b;
        out[i++] = (isalnum((unsigned char)c) || c == '.' || c == '_' || c == '-') ? c : '_';
    }
    out[i] = 0;
    if (i == 0 || strcmp(out, ".") == 0 || strcmp(out, "..") == 0) strcpy(out, "unnamed");
}

static char *read_whole_file(const char *path, size_t *len) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *b = malloc((size_t)sz + 1);
    if (!b) { fclose(f); return NULL; }
    size_t got = fread(b, 1, (size_t)sz, f);
    b[got] = 0;
    fclose(f);
    *len = got;
    return b;
}

/* process one request on a worker thread */
static void run_job(job_t *j, long wid, const char **kind, char *rname, char **body, size_t *len) {
    printf("[server] worker %ld started %s request %d\n", wid, j->kind, j->id);
    sleep((unsigned)proc_secs);              /* simulate real processing time */
    rname[0] = 0;
    *kind = "RESP";

    if (strcmp(j->kind, "REQ") == 0) {
        const char *t = j->text;
        while (*t == ' ') t++;
        char cmd[64] = "";
        sscanf(t, "%63s", cmd);
        const char *rest = t + strlen(cmd);
        while (*rest == ' ') rest++;
        char out[MAX_TEXT + 128];
        if (strcasecmp(cmd, "time") == 0) {
            time_t now = time(NULL);
            char ts[64];
            strftime(ts, sizeof ts, "%Y-%m-%d %H:%M:%S", localtime(&now));
            snprintf(out, sizeof out, "Server time: %s", ts);
        } else if (strcasecmp(cmd, "echo") == 0) {
            snprintf(out, sizeof out, "%s", rest);
        } else if (strcasecmp(cmd, "upper") == 0) {
            size_t i = 0;
            for (; rest[i] && i < sizeof out - 1; i++) out[i] = (char)toupper((unsigned char)rest[i]);
            out[i] = 0;
        } else if (strcasecmp(cmd, "reverse") == 0) {
            size_t n = strlen(rest);
            if (n > sizeof out - 1) n = sizeof out - 1;
            for (size_t i = 0; i < n; i++) out[i] = rest[n - 1 - i];
            out[n] = 0;
        } else if (strcasecmp(cmd, "count") == 0) {
            int words = 0, inw = 0;
            for (const char *p = rest; *p; p++) {
                if (isspace((unsigned char)*p)) inw = 0;
                else if (!inw) { inw = 1; words++; }
            }
            snprintf(out, sizeof out, "%d words, %zu characters", words, strlen(rest));
        } else {
            snprintf(out, sizeof out, "Processed: %s", t);
        }
        *body = strdup(out);
        *len = strlen(out);
    } else if (strcmp(j->kind, "PUT") == 0) {
        /* save the uploaded file and create a short report */
        char final[MAX_NAME + 32];
        snprintf(final, sizeof final, "%s/%s", FILE_DIR, j->arg);
        rename(j->text, final);
        size_t flen = 0;
        char *data = read_whole_file(final, &flen);
        long lines = 0, words = 0; int inw = 0;
        for (size_t i = 0; data && i < flen; i++) {
            if (data[i] == '\n') lines++;
            if (isspace((unsigned char)data[i])) inw = 0;
            else if (!inw) { inw = 1; words++; }
        }
        free(data);
        char rep[1024];
        int n = snprintf(rep, sizeof rep,
                         "SkyisSmall upload report\nFile name : %s\nSize      : %zu bytes\n"
                         "Lines     : %ld\nWords     : %ld\nProcessed by worker %ld\n",
                         j->arg, flen, lines, words, wid);
        snprintf(rname, 300, "report_%s.txt", j->arg);
        char rpath[600];
        snprintf(rpath, sizeof rpath, "%s/%s", FILE_DIR, rname);
        FILE *rf = fopen(rpath, "wb");
        if (rf) { fwrite(rep, 1, (size_t)n, rf); fclose(rf); }
        *kind = "FILE";
        *body = malloc((size_t)n + 1);
        memcpy(*body, rep, (size_t)n);
        *len = (size_t)n;
    } else { /* GET */
        char path[MAX_NAME + 32];
        snprintf(path, sizeof path, "%s/%s", FILE_DIR, j->arg);
        size_t flen = 0;
        char *data = read_whole_file(path, &flen);
        if (data) {
            snprintf(rname, 300, "%s", j->arg);
            *kind = "FILE";
            *body = data;
            *len = flen;
        } else {
            char out[MAX_NAME + 64];
            snprintf(out, sizeof out, "File not found: %s", j->arg);
            *kind = "ERR";
            *body = strdup(out);
            *len = strlen(out);
        }
    }
}

static void *worker(void *arg) {
    long wid = (long)arg;
    for (;;) {
        job_t j;
        pthread_mutex_lock(&st_mu);
        while (dir_count == 0 && !shutting_down) pthread_cond_wait(&st_cv, &st_mu);
        if (dir_count == 0 && shutting_down) { pthread_mutex_unlock(&st_mu); break; }
        j = dir_q[dir_head];
        dir_head = (dir_head + 1) % MAXQ;
        dir_count--;
        pthread_mutex_unlock(&st_mu);

        for (;;) {
            const char *kind; char rname[320]; char *body = NULL; size_t len = 0;
            run_job(&j, wid, &kind, rname, &body, &len);

            /* take the next waiting request before sending the reply */
            job_t nx; int have_next = 0;
            pthread_mutex_lock(&st_mu);
            if (buf_count > 0) {
                nx = buf_q[buf_head];
                buf_head = (buf_head + 1) % MAXQ;
                buf_count--;
                have_next = 1;
            } else {
                free_workers++;
            }
            pthread_mutex_unlock(&st_mu);

            send_frame(kind, j.id, rname, body, len);
            printf("[server] worker %ld finished request %d\n", wid, j.id);
            free(body);

            pthread_mutex_lock(&st_mu);
            outstanding--;
            pthread_cond_broadcast(&st_cv);
            pthread_mutex_unlock(&st_mu);

            if (!have_next) break;
            printf("[server] worker %ld picked buffered request %d\n", wid, nx.id);
            j = nx;
        }
    }
    return NULL;
}

/* return 0 for direct, 1 for buffered, or -1 if the buffer is full */
static int submit(job_t *j) {
    int r;
    pthread_mutex_lock(&st_mu);
    if (free_workers > 0) {
        free_workers--;
        dir_q[(dir_head + dir_count) % MAXQ] = *j;
        dir_count++;
        outstanding++;
        r = 0;
    } else if (buf_count < buf_cap) {
        buf_q[(buf_head + buf_count) % MAXQ] = *j;
        buf_count++;
        outstanding++;
        r = 1;
    } else {
        r = -1;
    }
    pthread_cond_broadcast(&st_cv);
    pthread_mutex_unlock(&st_mu);
    return r;
}

static int wait_drained(int max_ms) {
    for (int waited = 0; waited < max_ms; waited += 100) {
        pthread_mutex_lock(&st_mu);
        int done = (outstanding == 0);
        pthread_mutex_unlock(&st_mu);
        if (done) return 1;
        usleep(100000);
    }
    return 0;
}

/* handle one client connection */
static int serve_client(int fd) {
    int maint_started = 0, close_sent = 0;
    time_t start = time(NULL), close_time = 0;

    pthread_mutex_lock(&wr_mu);
    client_fd = fd;
    pthread_mutex_unlock(&wr_mu);
    send_frame("HELLO", 0, "-", "", 0);

    for (;;) {
        time_t now = time(NULL);

        /* during maintenance, finish accepted work before closing */
        if (!maint_started && (maint_flag || (maint_after > 0 && now - start >= maint_after))) {
            maint_started = 1;
            printf("[server] MAINTENANCE: no new requests accepted; finishing accepted work\n");
            send_frame("MAINT", 0, "-", "", 0);
        }
        if (maint_started && !close_sent) {
            pthread_mutex_lock(&st_mu);
            int idle = (outstanding == 0);
            pthread_mutex_unlock(&st_mu);
            if (idle) {
                send_frame("CLOSE", 0, "-", "", 0);
                printf("[server] all work done, CLOSE sent; waiting for client BYE\n");
                close_sent = 1;
                close_time = now;
            }
        }
        if (close_sent && now - close_time > 10) { printf("[server] client did not answer CLOSE\n"); break; }

        struct pollfd p = { fd, POLLIN, 0 };
        int r = poll(&p, 1, 200);
        if (r < 0) { if (errno == EINTR) continue; break; }
        if (r == 0) continue;

        char line[512];
        int lr = recv_line(fd, line, sizeof line);
        if (lr <= 0) { printf("[server] client disconnected\n"); break; }

        char kind[8], arg[MAX_NAME]; int id; size_t len;
        if (sscanf(line, "%7s %d %255s %zu", kind, &id, arg, &len) != 4) {
            send_frame("ERR", 0, "-", "bad request", 11);
            continue;
        }

        if (strcmp(kind, "BYE") == 0) {
            wait_drained(30000);
            send_frame("BYEACK", id, "-", "", 0);
            shutdown(fd, SHUT_WR);                 /* close the server's sending side */
            struct pollfd q = { fd, POLLIN, 0 };
            char tmp[256];
            for (int i = 0; i < 25; i++)           /* wait for the client to close its sending side */
                if (poll(&q, 1, 200) > 0 && recv(fd, tmp, sizeof tmp, 0) <= 0) break;
            printf("[server] connection closed gracefully\n");
            break;
        }

        job_t j;
        memset(&j, 0, sizeof j);
        j.id = id;
        snprintf(j.kind, sizeof j.kind, "%s", kind);
        char tmpath[MAX_NAME + 64] = "";

        if (strcmp(kind, "REQ") == 0) {
            if (len >= MAX_TEXT) { send_frame("ERR", id, "-", "too long", 8); break; }
            if (read_exact(fd, j.text, len) < 0) break;
            j.text[len] = 0;
        } else if (strcmp(kind, "GET") == 0) {
            make_safe_name(arg, j.arg, sizeof j.arg);
        } else if (strcmp(kind, "PUT") == 0) {
            if ((long)len > MAX_FILE) { send_frame("ERR", id, "-", "too big", 7); break; }
            make_safe_name(arg, j.arg, sizeof j.arg);
            snprintf(tmpath, sizeof tmpath, "%s/.upload_%d", FILE_DIR, id);
            FILE *f = fopen(tmpath, "wb");
            char chunk[65536];
            size_t left = len; int bad = 0;
            while (left > 0) {                     /* always consume the body */
                size_t n = left < sizeof chunk ? left : sizeof chunk;
                if (read_exact(fd, chunk, n) < 0) { bad = 1; break; }
                if (f) fwrite(chunk, 1, n, f);
                left -= n;
            }
            if (f) fclose(f);
            if (bad) { unlink(tmpath); break; }
            snprintf(j.text, sizeof j.text, "%s", tmpath);
            printf("[server] received file '%s' (%zu bytes) for request %d\n", j.arg, len, id);
        } else {
            send_frame("ERR", id, "-", "unknown request type", 20);
            continue;
        }

        int res = maint_started ? -2 : submit(&j);
        if (res < 0) {
            if (tmpath[0]) unlink(tmpath);
            send_frame("REJECT", id, res == -2 ? "maintenance" : "buffer_full", "", 0);
            printf("[server] request %d REJECTED (%s)\n", id, res == -2 ? "maintenance" : "buffer full");
        } else {
            send_frame("ACK", id, res == 0 ? "DIRECT" : "BUFFERED", "", 0);
            printf("[server] request %d accepted: %s\n", id,
                   res == 0 ? "given to an idle worker" : "all workers busy -> placed in server buffer");
        }
    }

    pthread_mutex_lock(&wr_mu);
    client_fd = -1;
    pthread_mutex_unlock(&wr_mu);
    close(fd);
    return maint_started;
}

int main(int argc, char **argv) {
    int opt;
    while ((opt = getopt(argc, argv, "p:w:b:t:m:")) != -1) {
        switch (opt) {
        case 'p': port = atoi(optarg); break;
        case 'w': num_workers = atoi(optarg); break;
        case 'b': buf_cap = atoi(optarg); break;
        case 't': proc_secs = atoi(optarg); break;
        case 'm': maint_after = atoi(optarg); break;
        default:
            fprintf(stderr, "usage: %s [-p port] [-w workers] [-b buffer] [-t secs] [-m maint_secs]\n", argv[0]);
            return 1;
        }
    }
    if (num_workers < 1 || num_workers > MAXQ || buf_cap < 1 || buf_cap > MAXQ) {
        fprintf(stderr, "workers and buffer size must be between 1 and %d\n", MAXQ);
        return 1;
    }
    setvbuf(stdout, NULL, _IOLBF, 0);
    mkdir(FILE_DIR, 0755);
    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_usr1;
    sigaction(SIGUSR1, &sa, NULL);

    free_workers = num_workers;
    pthread_t th[MAXQ];
    for (long i = 0; i < num_workers; i++) pthread_create(&th[i], NULL, worker, (void *)(i + 1));

    int ls = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    a.sin_port = htons((uint16_t)port);
    if (bind(ls, (struct sockaddr *)&a, sizeof a) < 0 || listen(ls, 5) < 0) { perror("bind/listen"); return 1; }
    printf("[server] listening on port %d: %d workers, buffer of %d requests, %d s per job\n",
           port, num_workers, buf_cap, proc_secs);

    int exit_for_maint = 0;
    while (!exit_for_maint && !maint_flag) {
        struct pollfd lp = { ls, POLLIN, 0 };
        if (poll(&lp, 1, 500) <= 0) continue;
        int fd = accept(ls, NULL, NULL);
        if (fd < 0) continue;
        printf("[server] client connected\n");
        exit_for_maint = serve_client(fd);
    }

    pthread_mutex_lock(&st_mu);
    shutting_down = 1;
    pthread_cond_broadcast(&st_cv);
    pthread_mutex_unlock(&st_mu);
    for (int i = 0; i < num_workers; i++) pthread_join(th[i], NULL);
    close(ls);
    printf("[server] shut down for maintenance\n");
    return 0;
}
