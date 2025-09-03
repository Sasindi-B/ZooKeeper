package keyvalrep;

import org.apache.zookeeper.*;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * ZookeeperClient is responsible for establishing a connection to a ZooKeeper server.
 * It allows the application to interact with ZooKeeper for distributed coordination.
 */
public class ZookeeperClient {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181"; // Address of the ZooKeeper server
    private static final int SESSION_TIMEOUT = 3000; // Session timeout in milliseconds
    private ZooKeeper zooKeeper;
    private CountDownLatch connectedLatch = new CountDownLatch(1);

    /**
     * Establishes a connection to the ZooKeeper server and waits until the connection is established.
     *
     * @throws IOException if an I/O error occurs while connecting to ZooKeeper.
     * @throws InterruptedException if the connection process is interrupted.
     */
    public void connect() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedLatch.countDown(); // Signal that the connection has been established
            }
        });
        connectedLatch.await(); // Wait until ZooKeeper connection is established
    }

    /**
     * Retrieves the active ZooKeeper instance after a successful connection.
     *
     * @return the connected ZooKeeper instance.
     */
    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    /**
     * Closes the ZooKeeper connection gracefully.
     *
     * @throws InterruptedException if the closing process is interrupted.
     */
    public void close() throws InterruptedException {
        zooKeeper.close();
    }
}