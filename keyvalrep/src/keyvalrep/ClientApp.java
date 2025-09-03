package keyvalrep;
 
import java.io.*;
import java.net.*;
import java.util.*;
import org.apache.zookeeper.*;
import java.util.concurrent.CountDownLatch;

/**
 * ClientApp connects to an application server and allows users to interact with the key-value store.
 *
 * This implementation currently prompts the user to enter the server's hostname and port manually.
 * Future enhancements could include automatic server discovery using ZooKeeper.
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
     * Connects to a manually entered application server and allows the user to interact with it.
     *
     * TODO: Modify this method to automatically fetch an available server instead of manual input.
     *       - Call `fetchAvailableServers()` to get the latest list of servers.
     *       - Use `getAvailableServer()` to select a random or least-loaded server.
     *       - If no servers are available, retry fetching and selecting after a short delay.
     *       - If a selected server is unreachable, attempt another available server.
     */
    private void runClient() throws IOException {
        Scanner scanner = new Scanner(System.in);

        // Prompt the user to enter server details manually
        System.out.print("Enter Application Server address (e.g., localhost:5000): ");
        String server = scanner.nextLine();

        // Validate the entered server address format
        String[] serverParts = server.split(":");
        while (serverParts.length != 2) {
            System.out.print("Invalid format. Please enter again (e.g., localhost:5000): ");
            server = scanner.nextLine();
            serverParts = server.split(":");
        }

        String host = serverParts[0];
        int port = Integer.parseInt(serverParts[1]);

        while (true) {
            try (Socket socket = new Socket(host, port);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                System.out.println("Connected to server: " + server);
                while (true) {
                    System.out.println("Enter command (put key=value, get key, exit):");
                    String input = scanner.nextLine();
                    if (input.equals("exit")) break;

                    out.println(input);
                    System.out.println("Server Response: " + in.readLine());
                }
            } catch (IOException e) {
                System.out.println("Server " + server + " is down. Please restart the client and enter a valid server.");
                break; // Exit loop if the server is unreachable
            }
        }
        scanner.close(); // Close scanner to prevent resource leaks
    }

    /**
     * Closes the ZooKeeper connection when the client exits.
     */
    private void close() throws InterruptedException {
        zooKeeper.close();
    }
}
