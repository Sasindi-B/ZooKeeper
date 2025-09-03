package keyvalrep;

import java.io.*;
import java.net.*;
import java.util.*;
import org.apache.zookeeper.*;
import java.util.concurrent.CountDownLatch;

/**
 * ClientApp connects to an application server and allows users to interact with the key-value store.
 *
 * Now uses automatic server discovery via ZooKeeper (no manual prompt).
 */
public class ClientApp {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final String SERVERS_NODE = "/servers";
    private ZooKeeper zooKeeper;
    private List<String> availableServers = new ArrayList<>();
    private Random random = new Random();
    private CountDownLatch connectedLatch = new CountDownLatch(1);

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ClientApp client = new ClientApp();
        client.connectToZooKeeper(); // Connect to ZooKeeper
        client.runClient(); // Start client interaction
        client.close(); // Close ZooKeeper connection on exit
    }

    /**
     * Connects the client to the ZooKeeper service.
     */
    private void connectToZooKeeper() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, 3000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedLatch.countDown();
            }
        });
        connectedLatch.await(); // Wait until ZooKeeper connection is established
    }

    /**
     * Fetches the list of available application servers from ZooKeeper.
     */
    private void fetchAvailableServers() throws KeeperException, InterruptedException {
        List<String> nodes = zooKeeper.getChildren(SERVERS_NODE, event -> {
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                try {
                    fetchAvailableServers(); // Refresh available servers on update
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        availableServers.clear(); // Clear old list before updating

        for (String node : nodes) {
            String nodePath = SERVERS_NODE + "/" + node; // Full path: /servers/server-xxxxx
            byte[] data = zooKeeper.getData(nodePath, false, null);
            String serverInfo = new String(data); // Extract stored server address (e.g., "localhost:5000")

            if (serverInfo.contains(":")) { // Ensure correct format
                availableServers.add(serverInfo);
            }
        }
    }

    /**
     * Selects an available server randomly for connection.
     *
     * @return The server address (host:port) or null if no servers are available.
     */
    private String getAvailableServer() {
        if (availableServers.isEmpty()) {
            System.out.println("No available servers. Retrying...");
            return null;
        }
        return availableServers.get(random.nextInt(availableServers.size()));
    }

    /**
     * Connects to an automatically discovered application server and allows the user to interact with it.
     * - Uses ZooKeeper to discover servers
     * - Picks one at random
     * - Retries discovery with backoff if none are available or reachable
     */
    private void runClient() {
        final long retryDelayMs = 2000L; // backoff between discovery retries
        final Scanner scanner = new Scanner(System.in);

        while (true) {
            // 1) Discover servers (with watch) and pick one at random
            try {
                fetchAvailableServers();
            } catch (KeeperException | InterruptedException e) {
                System.out.println("Failed to fetch servers from ZooKeeper: " + e.getMessage());
            }

            String selected = getAvailableServer();
            if (selected == null) {
                System.out.println("No servers registered yet. Retrying in " + retryDelayMs + " ms...");
                try { Thread.sleep(retryDelayMs); } catch (InterruptedException ignored) {}
                continue; // try discovery again
            }

            // 2) Build a candidate list so we can try others if the first choice fails
            List<String> candidates = new ArrayList<>(availableServers);
            // Try the randomly-selected one first by moving it to the end and popping from the end
            candidates.remove(selected);
            candidates.add(selected);

            boolean connected = false;

            // 3) Try servers until one connects; if none do, sleep and rediscover
            while (!candidates.isEmpty() && !connected) {
                String server = candidates.remove(candidates.size() - 1); // take selected first, then others
                String[] parts = server.split(":");
                if (parts.length != 2) {
                    System.out.println("Skipping malformed server address: " + server);
                    continue;
                }

                String host = parts[0];
                int port;
                try {
                    port = Integer.parseInt(parts[1]);
                } catch (NumberFormatException nfe) {
                    System.out.println("Skipping server with invalid port: " + server);
                    continue;
                }

                // 4) Attempt connection & interactive loop
                try (Socket socket = new Socket(host, port);
                     PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                     BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                    System.out.println("Connected to server: " + server);
                    connected = true;

                    while (true) {
                        System.out.println("Enter command (put key=value, get key, exit):");
                        String input = scanner.nextLine();
                        if ("exit".equalsIgnoreCase(input)) {
                            scanner.close();
                            return; // exit the client app
                        }

                        out.println(input);
                        String response = in.readLine();
                        if (response == null) {
                            System.out.println("Server closed the connection. Reconnecting...");
                            break; // break out to try another server
                        }
                        System.out.println("Server Response: " + response);
                    }
                } catch (IOException ioe) {
                    System.out.println("Could not connect to server " + server + ": " + ioe.getMessage());
                    // try next candidate
                }
            }

            if (!connected) {
                System.out.println("All discovered servers were unreachable. Retrying in " + retryDelayMs + " ms...");
                try { Thread.sleep(retryDelayMs); } catch (InterruptedException ignored) {}
            }
            // loop continues: rediscover and try again
        }
    }

    /**
     * Closes the ZooKeeper connection when the client exits.
     */
    private void close() throws InterruptedException {
        zooKeeper.close();
    }
}
