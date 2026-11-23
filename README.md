# Multithreaded TCP Client-Server Application

## Overview

This project is a TCP client-server application written in C. It demonstrates how a client and server can exchange text requests and transfer files over a network connection.

The server uses multiple worker threads to process requests. When all workers are busy, incoming work can wait in a limited server-side buffer. The client also maintains its own queue and uses the available-space updates sent by the server to decide when to send more work.

The application supports text-processing commands, file uploads, file downloads, and a controlled shutdown process.

## Features

- TCP socket communication between a client and server
- Multiple server worker threads using POSIX threads (`pthreads`)
- A configurable first-in, first-out request buffer
- Client-side queuing and retry handling when the server cannot accept a request
- Text commands for time, echo, uppercase conversion, reversing text, and word/character counting
- File uploads and downloads
- Upload reports containing file size, line count, and word count
- File-name sanitization on the server
- Graceful shutdown after outstanding work is completed
- Server maintenance mode triggered by a timer or `SIGUSR1`

## Project Files

| File | Description |
|---|---|
| `client.c` | Connects to the server, accepts commands, queues requests, and handles responses and downloaded files. |
| `server.c` | Listens for client connections, schedules requests, runs worker threads, and handles file operations. |
| `README.md` | Project overview and build/run instructions. |

## Requirements

- GCC
- Linux or another Unix-like environment
- POSIX threads (`pthread`)
- Two terminal windows for a local test

The client connects to `127.0.0.1` on port `5050` by default.

## Compile

Open a terminal in the project directory.

Compile the server:

```bash
gcc -Wall -Wextra -pthread -o server server.c
```

Compile the client:

```bash
gcc -Wall -Wextra -o client client.c
```

## Run

Start the server in the first terminal:

```bash
./server
```

Start the client in a second terminal:

```bash
./client
```

The default settings are:

| Option | Purpose | Default |
|---|---|---:|
| `-p` | Server listening port | `5050` |
| `-w` | Number of worker threads | `2` |
| `-b` | Number of waiting buffer slots | `4` |
| `-t` | Simulated processing time per job, in seconds | `2` |
| `-m` | Seconds after connection before maintenance starts; `0` disables the timer | `0` |

For example, start the server with one worker, two waiting slots, and a one-second processing delay:

```bash
./server -w 1 -b 2 -t 1
```

The client accepts an optional host and port:

```bash
./client 127.0.0.1 5050
```

## Client Commands

Enter these commands in the client terminal:

| Command | What it does |
|---|---|
| `req time` | Returns the server's current local time. |
| `req echo hello there` | Returns the text after `echo`. |
| `req upper hello there` | Converts the text to uppercase. |
| `req reverse hello` | Reverses the text. |
| `req count hello there` | Counts words and characters. |
| `put path/to/file.txt` | Uploads a file to the server. |
| `get file.txt` | Downloads a file from the server. |
| `sleep 5` | Pauses processing of later keyboard commands for five seconds. |
| `quit` | Waits for queued and outstanding requests to finish, then closes the connection. |

### File Transfers

Uploaded files are stored by the server in the `server_files/` directory. After an upload, the server creates a report with information about the uploaded file and sends the report back to the client.

Downloaded files are saved by the client in the `client_downloads/` directory.

To test an upload, create a small text file in the project directory and enter:

```text
put sample.txt
```

Then request it back:

```text
get sample.txt
```

## How Request Processing Works

1. The client connects to the server over TCP.
2. The client sends a request when the server reports available buffer capacity.
3. The server assigns work to an available worker thread.
4. If all workers are busy, the server places the request in its waiting buffer, if space is available.
5. If the server buffer is full, the server rejects the request and the client keeps it queued for another attempt.
6. A worker processes the request and sends the result to the client.
7. The client updates its queue and sends additional work when capacity becomes available.

The server uses mutexes and a condition variable to coordinate shared queue and worker state. A separate mutex protects writes to the client connection so worker threads do not interleave response data.

## Maintenance and Shutdown

Maintenance can be started automatically by setting the server's `-m` option, or manually by sending `SIGUSR1` to the server process.

For example:

```bash
./server -m 10
```

To trigger maintenance manually, find the server process ID and run:

```bash
kill -USR1 <server_pid>
```

When maintenance begins, the server stops accepting new work, finishes requests it has already accepted, and then coordinates a clean connection shutdown with the client.

## Concepts Demonstrated

- C programming
- TCP/IP and socket programming
- Concurrent programming with POSIX threads
- Mutexes and condition variables
- Producer-consumer style request queues
- Client-server protocols and message framing
- File I/O and file transfer
- Graceful shutdown and signal handling

## Notes

- This is an educational project intended for local testing and learning.
- The server creates `server_files/` and the client creates `client_downloads/` when needed.
- The server's processing delay is simulated to make worker scheduling and buffering easier to observe.
- Uploaded files are limited to 64 MB by the server.
