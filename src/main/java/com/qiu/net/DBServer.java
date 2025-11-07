package com.qiu.net;

import com.qiu.core.MiniDB;
import com.qiu.core.Options;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于NIO的数据库服务器 (已修复异常处理)
 */
public class DBServer {
    private final int port;
    private final String dbPath;
    private final MiniDB db;
    private volatile boolean running = false;

    // NIO组件
    private ServerSocketChannel serverChannel;
    private Selector selector;

    // 线程池
    private final ExecutorService bossPool;
    private final ExecutorService workerPool;
    private final ExecutorService commandPool;

    // 会话管理
    private final Map<SocketChannel, ClientSession> sessions = new ConcurrentHashMap<>();
    private final AtomicInteger connectionCount = new AtomicInteger(0);
    private final AtomicLong totalCommands = new AtomicLong(0);

    public DBServer(int port, String dbPath, Options options) throws IOException {
        this.port = port;
        this.dbPath = dbPath;
        this.db = MiniDB.open(dbPath, options);

        // 初始化线程池
        this.bossPool = Executors.newFixedThreadPool(1, r -> {
            Thread t = new Thread(r, "NIO-Boss");
            t.setDaemon(true);
            return t;
        });

        int workerThreads = Runtime.getRuntime().availableProcessors();
        this.workerPool = Executors.newFixedThreadPool(workerThreads, r -> {
            Thread t = new Thread(r, "NIO-Worker");
            t.setDaemon(true);
            return t;
        });

        this.commandPool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "Command-Executor");
            t.setDaemon(true);
            return t;
        });
    }

    public DBServer(int port, String dbPath) throws IOException {
        this(port, dbPath, Options.defaultOptions());
    }

    /**
     * 启动服务器
     */
    public void start() {
        try {
            serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            serverChannel.bind(new InetSocketAddress(port));

            selector = Selector.open();
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            running = true;

            System.out.println("MiniDB Server started on port " + port);
            System.out.println("Database path: " + dbPath);
            System.out.println("Worker threads: " + Runtime.getRuntime().availableProcessors());

            // 启动统计线程
            startStatsThread();

            bossLoop();

        } catch (IOException e) {
            System.err.println("Failed to start server: " + e.getMessage());
            stop();
        }
    }

    /**
     * Boss线程 - 处理连接事件
     */
    private void bossLoop() {
        while (running) {
            try {
                if (selector.select(1000) > 0) {
                    Set<SelectionKey> selectedKeys = selector.selectedKeys();
                    Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

                    while (keyIterator.hasNext()) {
                        SelectionKey key = keyIterator.next();
                        keyIterator.remove();

                        if (key.isValid()) {
                            if (key.isAcceptable()) {
                                handleAccept(key);
                            } else if (key.isReadable()) {
                                // (保持原逻辑)
                                workerPool.submit(() -> handleRead(key));
                            }
                        }
                    }
                }
            } catch (IOException e) {
                if (running) {
                    System.err.println("Error in boss loop: " + e.getMessage());
                }
            } catch (ClosedSelectorException e) {
                break;
            }
        }
    }

    private void handleAccept(SelectionKey key) {
        try {
            ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
            SocketChannel clientChannel = serverChannel.accept();

            if (clientChannel != null) {
                clientChannel.configureBlocking(false);

                ClientSession session = new ClientSession(clientChannel, db);
                sessions.put(clientChannel, session);

                clientChannel.register(selector, SelectionKey.OP_READ, session);

                connectionCount.incrementAndGet();
                System.out.println("New client: " + clientChannel.getRemoteAddress() +
                        " (active: " + connectionCount.get() + ")");
            }
        } catch (IOException e) {
            System.err.println("Error accepting connection: " + e.getMessage());
        }
    }

    private void handleRead(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        ClientSession session = (ClientSession) key.attachment();

        try {
            ByteBuffer buffer = ByteBuffer.allocate(8192);
            int bytesRead = channel.read(buffer);

            if (bytesRead == -1) {
                closeConnection(channel, session);
                return;
            }

            if (bytesRead > 0) {
                buffer.flip();
                commandPool.submit(() -> processData(session, buffer));
            }

        } catch (IOException e) {
            closeConnection(channel, session);
        }
    }

    /**
     * (已修复) 处理接收到的数据
     */
    private void processData(ClientSession session, ByteBuffer buffer) {
        try {
            // 将数据添加到会话的缓冲区
            session.appendBuffer(buffer);

        } catch (Exception e) {
            // 缓冲区追加失败 (例如 OOM)
            System.err.println("Error appending buffer: " + e.getMessage());
            session.sendError("Internal buffer error: " + e.getMessage());
            closeConnection(session.getChannel(), session);
            return;
        }

        // 解析并处理所有完整命令
        while (true) {
            Command command = null;
            try {
                // 1. 尝试解析
                command = session.parseCommand();
                if (command == null) {
                    break; // 没有完整命令了, 退出循环
                }

                // 2. 尝试执行
                handleCommand(session, command);
                totalCommands.incrementAndGet();

            } catch (Exception e) {
                // (*** 关键修复 ***)
                // 捕获到了解析或执行时的异常

                System.err.println("Error processing data: " + e.getMessage());
                session.sendError("Processing error: " + e.getMessage());

                if (command == null) {
                    // 如果 command == null, 说明是 parseCommand 抛出了异常
                    // 这通常意味着协议错误，缓冲区可能已损坏。
                    System.err.println("Unrecoverable parsing error. Closing connection.");
                    closeConnection(session.getChannel(), session);
                    break; // 退出循环
                }

                // 如果 command != null, 说明是 handleCommand (执行) 抛出了异常 (例如 DB.put() 的 NPE)
                // 缓冲区是正常的，只是这个命令失败了。
                // 我们已经发送了错误，现在继续循环，尝试解析缓冲区中的下一个命令。
            }
        }
    }


    /**
     * 处理命令
     */
    private void handleCommand(ClientSession session, Command command) {
        try {
            byte cmdType = command.getCommandType();

            // 批处理控制命令立即执行
            if (cmdType == Protocol.BATCH_START ||
                    cmdType == Protocol.BATCH_COMMIT ||
                    cmdType == Protocol.BATCH_CANCEL) {
                command.execute(session);
            }
            // 在批处理模式中：累积数据操作命令
            else if (session.isInBatch()) {
                session.addToBatch(command);
                session.sendResponse("BATCH_OP_ADDED");
            }
            // 非批处理模式：立即执行数据操作命令
            else {
                command.execute(session);
            }
        } catch (Exception e) {
            // 将异常抛出，由 processData 统一处理
            throw new RuntimeException("Command execution failed: " + e.getMessage(), e);
        }
    }

    private void closeConnection(SocketChannel channel, ClientSession session) {
        try {
            if (session != null) {
                session.cleanup();
                sessions.remove(channel);
            }

            if (channel != null) {
                channel.close();
            }

            connectionCount.decrementAndGet();
            System.out.println("Client disconnected. Active: " + connectionCount.get());

        } catch (IOException e) {
            System.err.println("Error closing connection: " + e.getMessage());
        }
    }

    private void startStatsThread() {
        Thread statsThread = new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(30000);
                    printStats();
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        statsThread.setDaemon(true);
        statsThread.setName("Stats-Thread");
        statsThread.start();
    }

    private void printStats() {
        var dbStats = db.getStats();
        System.out.println("=== MiniDB Server Stats ===");
        System.out.println("Active connections: " + connectionCount.get());
        System.out.println("Total commands: " + totalCommands.get());
        System.out.println("DB - Puts: " + dbStats.getTotalPuts() +
                ", Gets: " + dbStats.getTotalGets() +
                ", Deletes: " + dbStats.getTotalDeletes());
        System.out.println("MemTable size: " + dbStats.getMemtableSize() + " bytes");
        System.out.println("SSTable files: " + dbStats.getSstableCount());
        System.out.println("===========================");
    }

    /**
     * 停止服务器
     */
    public void stop() {
        running = false;

        System.out.println("Shutting down MiniDB Server...");

        // 关闭所有会话
        for (ClientSession session : sessions.values()) {
            session.cleanup();
        }
        sessions.clear();

        // 关闭数据库
        try {
            db.close();
        } catch (IOException e) {
            System.err.println("Error closing database: " + e.getMessage());
        }

        // 关闭线程池
        shutdownPool(bossPool, "Boss");
        shutdownPool(workerPool, "Worker");
        shutdownPool(commandPool, "Command");

        // 关闭NIO组件
        try {
            if (selector != null) selector.close();
            if (serverChannel != null) serverChannel.close();
        } catch (IOException e) {
            System.err.println("Error closing NIO components: " + e.getMessage());
        }

        System.out.println("MiniDB Server stopped");
    }

    private void shutdownPool(ExecutorService pool, String name) {
        if (pool != null) {
            try {
                pool.shutdown();
                if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                }
            } catch (InterruptedException e) {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}