package keyvalrep;

import java.io.*;
import java.net.*;
import org.apache.zookeeper.*;
import java.util.concurrent.CountDownLatch;

/**
 * ApplicationServer acts as a key-value storage server that interacts with clients
 * and synchronizes data using Apache ZooKeeper for replication and consistency.
 */
public class ApplicationServer {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final String SERVERS_NODE = "/servers";
    private static ZooKeeper zooKeeper;
    private static String serverNodePath;
    private static ZookeeperKeyValueStore store;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        int port = (args.length > 0) ? Integer.parseInt(args[0]) : 5000; // Default to port 5000 if not specified

        // Connect to ZooKeeper
        CountDownLatch connectedLatch = new CountDownLatch(1);
        zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, 3000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedLatch.countDown();
            }
        });
        connectedLatch.await(); // Wait for ZooKeeper connection to establish

        // Initialize the Key-Value Store
        store = new ZookeeperKeyValueStore(zooKeeper);

        // Register this server instance in ZooKeeper for dynamic client discovery
        registerServer(port);

        // Start the TCP server to handle client requests
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Application Server started on port " + port);

            while (true) {
                Socket socket = serverSocket.accept(); // Accept client connections
                new Thread(new ClientHandler(socket)).start(); // Handle each client request in a new thread
            }
        }
    }

    /**
     * Registers this server instance in ZooKeeper so clients can discover available servers.
     *
     * @param port The port on which this server is running.
     */
    private static void registerServer(int port) throws KeeperException, InterruptedException {
        if (zooKeeper.exists(SERVERS_NODE, false) == null) {
            zooKeeper.create(SERVERS_NODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        String serverAddress = "localhost:" + port;
        serverNodePath = zooKeeper.create(SERVERS_NODE + "/server-", serverAddress.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Registered server: " + serverAddress);
    }

    /**
     * Handles client connections and processes key-value store commands (put/get).
     */
    private static class ClientHandler implements Runnable {
        private Socket socket;

        public ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                String request;
                while ((request = in.readLine()) != null) {
                    System.out.println("Received: " + request);
                    String[] parts = request.split(" ", 2);

                    if (parts.length != 2) {
                        out.println("Invalid command format.");
                        continue;
                    }

                    String command = parts[0];
                    if (command.equals("put")) {
                        // Store key-value pair in local cache and sync with ZooKeeper
                        String[] keyValue = parts[1].split("=", 2);
                        if (keyValue.length == 2) {
                            store.put(keyValue[0], keyValue[1]);
                            out.println("Stored: " + keyValue[0] + "=" + keyValue[1]);
                        } else {
                            out.println("Invalid format. Use put key=value");
                        }
                    } else if (command.equals("get")) {
                        // Retrieve value from the key-value store
                        String value = store.get(parts[1]);
                        if (value != null) {
                            out.println("Value: " + value);
                        } else {
                            out.println("Key not found.");
                        }
                    } else {
                        out.println("Unknown command.");
                    }
                }
            } catch (IOException | KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
