# Multithreaded Web Server — Java

A hands-on Java networking project that implements a TCP server in **three progressively advanced concurrency models** — single-threaded, multi-threaded (one thread per connection), and thread pool — demonstrating the evolution from a blocking server to a production-grade concurrent design.

---

## Table of Contents

- [Project Overview](#-project-overview)
- [Project Structure](#-project-structure)
- [Architecture & Concurrency Models](#-architecture--concurrency-models)
  - [Model 1 — Single-Threaded Server](#model-1--single-threaded-server)
  - [Model 2 — Multi-Threaded Server](#model-2--multi-threaded-server-thread-per-connection)
  - [Model 3 — Thread Pool Server](#model-3--thread-pool-server)
- [How It Works — Communication Flow](#-how-it-works--communication-flow)
- [Code Walkthrough](#-code-walkthrough)
  - [SingleThreaded/Server.java](#singlethreadedserverjava)
  - [SingleThreaded/Client.java](#singlethreadedclientjava)
  - [MultiThreaded/Server.java](#multithreadedserverjava)
  - [MultiThreaded/Client.java](#multithreadedclientjava)
  - [ThreadPool/Server.java](#threadpoolserverjava)
- [Key Java Concepts Used](#-key-java-concepts-used)
- [Comparison of All Three Models](#-comparison-of-all-three-models)
- [Running the Project](#-running-the-project)
- [Requirements](#-requirements)
- [Known Limitations & Improvements](#-known-limitations--improvements)
- [Learning Outcomes](#-learning-outcomes)

---

##  Project Overview

This project explores **socket-level TCP communication** in Java and answers a core systems design question:

> *What happens when 100 clients connect to a server simultaneously — and how do you handle it efficiently?*

Each of the three packages (`SingleThreaded`, `MultiThreaded`, `ThreadPool`) is a complete, runnable implementation of the same basic server — accept a connection, greet the client, close — but with fundamentally different concurrency strategies. Reading them in order shows exactly why real-world servers use thread pools.

| Package | Server Strategy | Client Strategy |
|---------|----------------|-----------------|
| `SingleThreaded` | Blocks on each connection sequentially | Single connection |
| `MultiThreaded` | Spawns a new `Thread` per connection | Spawns 100 concurrent threads |
| `ThreadPool` | Fixed pool of 10 worker threads via `ExecutorService` | *(reuse MultiThreaded client)* |

All servers bind to **port 8010** on localhost.

---

##  Project Structure

```
Multithreaded-Web-server-JAVA-main/
├── src/
│   ├── SingleThreaded/
│   │   ├── Server.java          # Blocking, one-at-a-time server
│   │   └── Client.java          # Single connection client
│   │
│   ├── MultiThreaded/
│   │   ├── Server.java          # New Thread spawned per connection
│   │   └── Client.java          # Spawns 100 concurrent client threads (load tester)
│   │
│   └── ThreadPool/
│       └── Server.java          # Fixed-size thread pool (10 workers)
│
├── SingleThread.iml             # IntelliJ IDEA module file
└── .idea/
    ├── misc.xml                 # JDK config (OpenJDK 24)
    ├── modules.xml
    ├── vcs.xml
    └── .gitignore
```

> **Note:** `.class` files are committed alongside `.java` files — the project was compiled with OpenJDK 24 (declared in `.idea/misc.xml`). The `ThreadPool` package contains only a server; use `MultiThreaded.Client` to stress-test it.

---

##  Architecture & Concurrency Models

### Model 1 — Single-Threaded Server

```
Client 1 ──► [ServerSocket.accept()] ──► handle ──► close ──► back to accept()
Client 2 ──► WAITING ...
Client 3 ──► WAITING ...
```

The server processes **one client at a time**. While it is talking to Client 1, every other client blocks waiting for `accept()` to return. This is the simplest model but completely unusable under real load — if handling a request takes 1 second, the 10th client in the queue waits at least 9 seconds.

A `setSoTimeout(10000)` is set on the `ServerSocket`, meaning if no client connects within 10 seconds an `IOException` is thrown (the loop catches and prints it, then continues listening).

---

### Model 2 — Multi-Threaded Server (Thread-Per-Connection)

```
Client 1 ──► accept() ──► new Thread(t1) ──► handle concurrently
Client 2 ──► accept() ──► new Thread(t2) ──► handle concurrently
Client 3 ──► accept() ──► new Thread(t3) ──► handle concurrently
  ...              (up to OS thread limit — no upper bound enforced)
```

For every incoming connection the server immediately spawns a brand-new OS thread and delegates the work to it. The main thread loops back to `accept()` instantly, so no client has to wait in a queue.

This scales well up to a point — but **creates an unbounded number of threads**. If 10,000 clients connect simultaneously, 10,000 threads are created, which will exhaust memory and overwhelm the OS scheduler. The `MultiThreaded.Client` deliberately fires 100 concurrent threads to demonstrate this model under load.

---

### Model 3 — Thread Pool Server

```
Client 1  ──► accept() ──► threadPool.execute(task) ──► [Worker 1] handles it
Client 2  ──► accept() ──► threadPool.execute(task) ──► [Worker 2] handles it
  ...
Client 11 ──► accept() ──► threadPool.execute(task) ──► [QUEUE — waiting for free worker]
```

A fixed pool of **10 worker threads** is created once at startup via `Executors.newFixedThreadPool(10)`. Incoming connections are submitted as tasks to an internal `ExecutorService` queue. If all 10 workers are busy, new tasks wait in the queue rather than spawning new threads.

This is the closest model to what production servers actually do — bounded resource usage, no thread explosion, and built-in backpressure. The `finally` block ensures `threadPool.shutdown()` is called on server exit for clean resource cleanup.

---

##  How It Works — Communication Flow

All three models share the same underlying socket communication protocol:

```
CLIENT                                         SERVER
  │                                               │
  │──── new Socket("localhost", 8010) ───────────►│  ServerSocket.accept() returns
  │                                               │
  │◄─── "Hello from server <IP>" ────────────────│  PrintWriter.println(...)
  │                                               │
  │  (client prints received message)             │  Socket closed
  │                                               │
  └───────────────────────────────────────────────┘
```

The exchange is intentionally minimal — one greeting from the server — so the focus stays on **concurrency**, not protocol complexity. Extending this to send HTTP responses, serve files, or return JSON would be the natural next step.

---

##  Code Walkthrough

### `SingleThreaded/Server.java`

```java
package SingleThreaded;

import java.io.*;
import java.net.*;

public class Server {
    public void run() throws IOException {
        int port = 8010;
        ServerSocket socket = new ServerSocket(port);
        socket.setSoTimeout(10000);          // Timeout: 10s of idle

        while (true) {
            try {
                System.out.println("SingleThreaded.Server is listening on port" + port);
                Socket acceptedConnection = socket.accept();  // BLOCKS here until a client connects
                System.out.println("Connection established at:" + acceptedConnection.getRemoteSocketAddress());

                PrintWriter toClient      = new PrintWriter(acceptedConnection.getOutputStream());
                BufferedReader fromClient = new BufferedReader(new InputStreamReader(acceptedConnection.getInputStream()));

                toClient.println("Hello from the server");

                toClient.close();
                fromClient.close();
                acceptedConnection.close();   // Explicit cleanup (no try-with-resources here)
            } catch (IOException e) {
                e.printStackTrace();          // Catches both timeout and client errors; loop continues
            }
        }
    }

    public static void main(String[] args) {
        Server server = new Server();
        try {
            server.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

**Notable details:**

- `socket.setSoTimeout(10000)` — if `accept()` blocks for more than 10 seconds with no incoming connection, a `SocketTimeoutException` (subclass of `IOException`) is thrown. The catch block handles it and the loop continues, preventing the server from hanging forever on an idle port.
- Streams are closed **manually** rather than via try-with-resources, which is a resource-leak risk — an exception thrown between stream creation and `.close()` would leave the socket open.
- `toClient.println(...)` without `autoFlush = true` may not send data until the stream is explicitly flushed or closed. Here it works because `toClient.close()` flushes the buffer before closing.
- The server opens `fromClient` but **never calls `readLine()`** on it — the client's message is sent but silently ignored.

---

### `SingleThreaded/Client.java`

```java
package SingleThreaded;

import java.io.*;
import java.net.*;

public class Client {
    public void run() throws IOException {
        int port = 8010;
        InetAddress address = InetAddress.getByName("localHost"); // mixed-case is valid; DNS is case-insensitive
        Socket socket = new Socket(address, port);

        PrintWriter toSocket      = new PrintWriter(socket.getOutputStream());
        BufferedReader fromSocket = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        toSocket.println("Hello from the client");

        String line = fromSocket.readLine();
        System.out.println("Response from the socket is:" + line);

        toSocket.close();
        fromSocket.close();
        socket.close();
    }

    public static void main(String[] args) {
        try {
            Client client = new Client();
            client.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

**Notable details:**

- `InetAddress.getByName("localHost")` — mixed-case `"localHost"` is valid; Java normalizes it to `127.0.0.1`. The `MultiThreaded.Client` uses lowercase `"localhost"` — both resolve identically.
- The client sends a message but the single-threaded server never reads it — `fromClient.readLine()` is never called server-side. The send is harmless but effectively ignored.
- No `autoFlush` on `PrintWriter`, but `close()` flushes it before the socket closes.

---

### `MultiThreaded/Server.java`

```java
package MultiThreaded;

import java.io.*;
import java.net.*;
import java.util.function.Consumer;

public class Server {

    // Returns a Consumer<Socket> — a functional handler for a single client connection
    public Consumer<Socket> getConsumer() {
        return (clientSocket) -> {
            try (PrintWriter toSocket = new PrintWriter(clientSocket.getOutputStream(), true)) {
                toSocket.println("Hello from server " + clientSocket.getInetAddress());
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            // try-with-resources closes PrintWriter (and flushes) automatically
        };
    }

    public static void main(String[] args) {
        int port = 8010;
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            serverSocket.setSoTimeout(10000);
            System.out.println("Server is listening on port:" + port);
            Server server = new Server();

            while (true) {
                Socket acceptedSocket = serverSocket.accept();
                // Spawn a brand-new OS thread for every single connection — no upper bound
                Thread thread = new Thread(() -> server.getConsumer().accept(acceptedSocket));
                thread.start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

**Notable details:**

- `new PrintWriter(outputStream, true)` — the second argument `true` enables **auto-flush on `println`**, so the response is sent to the client immediately without needing an explicit `.flush()` call. This is a correctness improvement over the SingleThreaded server.
- The handler is expressed as a `Consumer<Socket>` lambda — a functional-style design that makes the connection-handling logic portable, composable, and testable in isolation.
- `try-with-resources` on `PrintWriter` ensures stream cleanup even if an exception is thrown mid-response.
- **Critical limitation:** there is no upper bound on thread creation. If 10,000 clients connect, 10,000 threads are spawned. The OS typically caps threads between 1,000–10,000 depending on stack size and heap settings — beyond that, you get `OutOfMemoryError`.

---

### `MultiThreaded/Client.java`

```java
package MultiThreaded;

import java.io.*;
import java.net.*;

public class Client {

    public Runnable getRunnable() throws UnknownHostException, IOException {
        return new Runnable() {
            @Override
            public void run() {
                int port = 8010;
                try {
                    InetAddress address = InetAddress.getByName("localhost");
                    Socket socket = new Socket(address, port);
                    try (
                        PrintWriter toSocket      = new PrintWriter(socket.getOutputStream(), true);
                        BufferedReader fromSocket = new BufferedReader(new InputStreamReader(socket.getInputStream()))
                    ) {
                        toSocket.println("Hello from Client " + socket.getLocalSocketAddress());
                        String line = fromSocket.readLine();
                        System.out.println("Response from Server " + line);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    public static void main(String[] args) {
        Client client = new Client();
        for (int i = 0; i < 100; i++) {       // Fire 100 concurrent connections simultaneously
            try {
                Thread thread = new Thread(client.getRunnable());
                thread.start();
            } catch (Exception ex) {
                return;
            }
        }
    }
}
```

**Notable details:**

- This client is a **load simulator** — it spawns 100 threads simultaneously, each making a separate TCP connection. Use it to stress-test both the `MultiThreaded` and `ThreadPool` servers.
- Each `Runnable` identifies itself with its local socket address (`socket.getLocalSocketAddress()`), so you can observe in the server logs that all 100 connections are being served.
- `getRunnable()` declares `throws UnknownHostException, IOException` in the factory method signature, but these exceptions are fully caught inside the `Runnable.run()` body — the declaration on the factory method is technically unnecessary.
- `try-with-resources` handles both `PrintWriter` and `BufferedReader` cleanup together in a single block — significantly cleaner than the SingleThreaded client.

---

### `ThreadPool/Server.java`

```java
package ThreadPool;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class Server {
    private final ExecutorService threadPool;

    public Server(int poolSize) {
        this.threadPool = Executors.newFixedThreadPool(poolSize);  // Exactly 10 reusable threads
    }

    public void handleClient(Socket clientSocket) {
        try (PrintWriter toSocket = new PrintWriter(clientSocket.getOutputStream(), true)) {
            toSocket.println("Hello from server " + clientSocket.getInetAddress());
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        int port     = 8010;
        int poolSize = 10;
        Server server = new Server(poolSize);

        try {
            ServerSocket serverSocket = new ServerSocket(port);
            serverSocket.setSoTimeout(70000);  // 70s timeout — long enough for the 100-client load test
            System.out.println("Server is listening on port " + port);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                // Submit task to pool — reuses a worker thread, never spawns a new one
                server.threadPool.execute(() -> server.handleClient(clientSocket));
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            server.threadPool.shutdown();  // Allows in-flight tasks to finish; rejects new ones
        }
    }
}
```

**Notable details:**

- `Executors.newFixedThreadPool(10)` creates exactly 10 threads at startup and reuses them for every connection for the lifetime of the server. No new threads are ever created after initialization.
- `threadPool.execute(Runnable)` is used instead of `threadPool.submit(Callable)` — `execute` is fire-and-forget (no `Future` return value), which is correct here since we don't need to track the result.
- If all 10 workers are busy, `execute()` queues the task in an internal `LinkedBlockingQueue` — the client connection is held open waiting, rather than being rejected. This is the **backpressure** mechanism.
- The `finally` block calls `threadPool.shutdown()`, which allows already-submitted tasks to complete but rejects any new submissions — correct graceful shutdown behaviour.
- `setSoTimeout(70000)` — a 70-second accept timeout (vs. 10s in the other servers) gives more time for the 100-client load test to fully complete before the server times out.
- **This is the correct production pattern**: bounded resource usage, implicit task queuing, clean lifecycle management.

---

##  Key Java Concepts Used

| Concept | Where Used | What It Demonstrates |
|---------|-----------|----------------------|
| `ServerSocket` / `Socket` | All packages | Raw TCP socket creation and lifecycle |
| `PrintWriter(stream, autoFlush)` | MultiThreaded, ThreadPool | Immediate response flushing on `println` |
| `BufferedReader` + `InputStreamReader` | All packages | Wrapping raw `InputStream` for line-based reading |
| `InetAddress.getByName()` | All clients | Hostname/DNS resolution |
| `setSoTimeout()` | All servers | Preventing `accept()` from blocking forever |
| `new Thread(Runnable)` + `.start()` | MultiThreaded | Manual OS thread creation and start |
| `Consumer<Socket>` lambda | MultiThreaded Server | Functional-style, reusable request handler |
| `Runnable` anonymous class | MultiThreaded Client | Task encapsulation for thread dispatch |
| `ExecutorService` / `Executors.newFixedThreadPool()` | ThreadPool | Managed thread pool with fixed concurrency |
| `threadPool.execute(Runnable)` | ThreadPool | Fire-and-forget task submission |
| `threadPool.shutdown()` | ThreadPool | Graceful shutdown in `finally` block |
| `try-with-resources` on `Closeable` | MultiThreaded, ThreadPool | Automatic stream/socket cleanup on exception |

---

##  Comparison of All Three Models

| Dimension | SingleThreaded | MultiThreaded | ThreadPool |
|-----------|---------------|---------------|------------|
| **Concurrency** | None — sequential | Unbounded parallelism | Bounded (10 workers) |
| **Throughput under load** | 1 request at a time | High (until OS thread limit) | High, controlled |
| **Max threads created** | 1 (forever) | 1 per connection (unlimited) | Exactly 10 (fixed) |
| **Memory per connection** | Minimal (1 thread total) | ~1 MB stack per thread | Minimal (fixed pool) |
| **Backpressure** | Implicit (queue at `accept()`) | None — spawns threads forever | Built-in task queue |
| **Resource leak risk** | Medium (manual stream close) | High (no pool cleanup on crash) | Low (`finally` shutdown) |
| **Code complexity** | Simplest | Moderate | Moderate |
| **Real-world suitability** | Never | Rarely (tiny internal tools) | Always (production baseline) |
| **Accept timeout** | 10s | 10s | 70s |
| **Client provided** | Yes | Yes + doubles as load tester | No — reuse MultiThreaded client |

---

##  Running the Project

### Prerequisites

- Java 11+ (project compiled with OpenJDK 24)
- IntelliJ IDEA (recommended — `.iml` and `.idea/` configs included), or any Java IDE, or plain `javac` + `java`

### Option A — IntelliJ IDEA

1. Open the project root folder (`Multithreaded-Web-server-JAVA-main/`) in IntelliJ IDEA
2. IntelliJ detects `SingleThread.iml` and configures `src/` as the sources root automatically
3. Right-click any `Server.java` → **Run**
4. In a second run configuration or terminal, right-click the matching `Client.java` → **Run**

### Option B — Command Line

```bash
# Navigate to the src directory
cd Multithreaded-Web-server-JAVA-main/src

# ── 1. Single-Threaded ───────────────────────────────────────────────────
javac SingleThreaded/Server.java SingleThreaded/Client.java

# Terminal 1 — start server
java SingleThreaded.Server
# Output: SingleThreaded.Server is listening on port8010

# Terminal 2 — connect once
java SingleThreaded.Client
# Output: Response from the socket is:Hello from the server


# ── 2. Multi-Threaded ────────────────────────────────────────────────────
javac MultiThreaded/Server.java MultiThreaded/Client.java

# Terminal 1 — start server
java MultiThreaded.Server
# Output: Server is listening on port:8010

# Terminal 2 — fire 100 concurrent clients
java MultiThreaded.Client
# Output (100 lines): Response from Server Hello from server /127.0.0.1


# ── 3. Thread Pool ───────────────────────────────────────────────────────
javac ThreadPool/Server.java

# Terminal 1 — start pool server
java ThreadPool.Server
# Output: Server is listening on port 8010

# Terminal 2 — reuse the multithreaded load tester
java MultiThreaded.Client
# Output: same 100 responses — but the server used only 10 threads
```

> **Experiment:** Run the MultiThreaded and ThreadPool servers side by side (different ports) and compare server-side output. The multithreaded server logs 100 connection lines instantly; the thread pool logs them in batches of 10 as workers become free.

---

##  Requirements

| Requirement | Version / Notes |
|-------------|-----------------|
| Java (JDK) | 11 or higher; built with OpenJDK 24 |
| Operating System | Windows / Linux / macOS |
| IDE | IntelliJ IDEA (any edition) — optional |
| Build tool | None — pure `javac`, no Maven or Gradle |
| External dependencies | None — only `java.net`, `java.io`, `java.util.concurrent` |

---

##  Known Limitations & Improvements

### Current Limitations

**1. No HTTP protocol** — the server speaks raw TCP, not HTTP. A browser connecting to port 8010 would receive the plain-text greeting but show a "connection error" because there are no HTTP headers. Adding a proper response would make this a functional web server:

```java
// Minimal HTTP/1.1 response
toSocket.print("HTTP/1.1 200 OK\r\n");
toSocket.print("Content-Type: text/plain\r\n");
toSocket.print("Connection: close\r\n\r\n");
toSocket.print("Hello from the server\r\n");
```

**2. Stream leak in SingleThreaded** — if an exception occurs after `new PrintWriter(...)` but before `toClient.close()`, the socket leaks. Fix with try-with-resources:

```java
try (
    Socket acceptedConnection = socket.accept();
    PrintWriter toClient      = new PrintWriter(acceptedConnection.getOutputStream(), true);
    BufferedReader fromClient = new BufferedReader(new InputStreamReader(acceptedConnection.getInputStream()))
) {
    toClient.println("Hello from the server");
}
```

**3. Unbounded ThreadPool queue** — `Executors.newFixedThreadPool()` uses a `LinkedBlockingQueue` with no capacity limit. Under extreme load, queued tasks consume unbounded heap. A custom `ThreadPoolExecutor` with a bounded queue is safer:

```java
ExecutorService pool = new ThreadPoolExecutor(
    10,                                        // core threads
    20,                                        // max threads
    60L, TimeUnit.SECONDS,                     // idle thread keepalive
    new ArrayBlockingQueue<>(100),             // bounded queue — max 100 waiting tasks
    new ThreadPoolExecutor.CallerRunsPolicy()  // backpressure: run on caller if queue full
);
```

**4. No logging framework** — `System.out.println` and `e.printStackTrace()` are used throughout. In production, replace with SLF4J + Logback for structured, level-controlled output.

**5. MultiThreaded client silently swallows errors** — `catch (Exception ex) { return; }` in `main` silently stops spawning threads on any exception without reporting which connection failed.

**6. No persistent connections** — each connection is opened, greeted, and immediately closed. Real HTTP/1.1 uses `Connection: keep-alive` to reuse sockets across multiple requests, reducing handshake overhead dramatically.

---

##  Learning Outcomes

After studying this project you will understand:

- How TCP `ServerSocket` / `Socket` work at the Java API level — binding a port, accepting connections, reading/writing streams, and closing cleanly
- Why a single-threaded server is fundamentally broken under concurrent load, and how to measure that failure
- The trade-off between **thread-per-connection** (simple, fragile at scale) and **thread pool** (bounded, production-safe, slightly more complex)
- How `ExecutorService` and `Executors.newFixedThreadPool()` abstract away thread lifecycle management into a clean task-submission model
- The role of `setSoTimeout()` in preventing `accept()` from blocking indefinitely on an idle port
- Why `try-with-resources` is essential for `Closeable` network streams and how skipping it creates resource leaks
- The concept of **backpressure** — what happens when a thread pool's queue fills up and why bounded queues matter
- How to use a single `Client` implementation as a load tester across multiple server implementations

---

##  License

MIT — Built for learning Java networking and concurrency fundamentals.

---

**Built as part of a BTech CSE curriculum — exploring network programming from blocking I/O to managed concurrency.**
