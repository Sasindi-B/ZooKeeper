package keyvalrep;

import org.apache.zookeeper.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * ZookeeperKeyValueStore is a distributed key-value store that uses ZooKeeper
 * for synchronization and consistency across multiple application servers.
 * It maintains a local cache for fast access and updates ZooKeeper to ensure
 * global consistency.
 */
public class ZookeeperKeyValueStore {
    private static final String STORE_NAMESPACE = "/kvstore"; // Root path in ZooKeeper for storing key-value pairs
    private final ZooKeeper zooKeeper;
    private final Map<String, String> localCache = new ConcurrentHashMap<>(); // Local in-memory cache

    /**
     * Initializes the key-value store by connecting to ZooKeeper, ensuring the
     * storage node exists, and synchronizing local cache with ZooKeeper data.
     *
     * @param zooKeeper The ZooKeeper instance
     * @throws KeeperException if a ZooKeeper error occurs
     * @throws InterruptedException if the operation is interrupted
     */
    public ZookeeperKeyValueStore(ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
        this.zooKeeper = zooKeeper;
        initializeStore();
        syncFromZooKeeper();
    }

    /**
     * Ensures that the root node for the key-value store exists in ZooKeeper.
     *
     * @throws KeeperException if a ZooKeeper error occurs
     * @throws InterruptedException if the operation is interrupted
     */
    private void initializeStore() throws KeeperException, InterruptedException {
        if (zooKeeper.exists(STORE_NAMESPACE, false) == null) {
            zooKeeper.create(STORE_NAMESPACE, new byte[]{},
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    /**
     * Stores a key-value pair in the local cache and propagates it to ZooKeeper.
     *
     * @param key The key to store
     * @param value The value associated with the key
     * @throws KeeperException if a ZooKeeper error occurs
     * @throws InterruptedException if the operation is interrupted
     */
    public void put(String key, String value) throws KeeperException, InterruptedException {
        localCache.put(key, value); // Store in local cache
        String path = STORE_NAMESPACE + "/" + key;
        byte[] data = value.getBytes(StandardCharsets.UTF_8);

        if (zooKeeper.exists(path, false) == null) {
            zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            zooKeeper.setData(path, data, -1);
        }
    }

    /**
     * Retrieves a value for a given key. First checks the local cache, and if
     * not found, fetches the value from ZooKeeper and updates the cache.
     *
     * @param key The key to retrieve
     * @return The value associated with the key or null if not found
     * @throws KeeperException if a ZooKeeper error occurs
     * @throws InterruptedException if the operation is interrupted
     */
    public String get(String key) throws KeeperException, InterruptedException {
        if (localCache.containsKey(key)) {
            return localCache.get(key);
        }

        String path = STORE_NAMESPACE + "/" + key;
        zooKeeper.sync(path, null, null); // Ensure latest data is fetched
        byte[] data = zooKeeper.getData(path, false, null);
        String value = new String(data, StandardCharsets.UTF_8);
        
        localCache.put(key, value); // Store fetched value in local cache
        return value;
    }

    /**
     * Synchronizes the local cache with data stored in ZooKeeper.
     * This is called at startup to ensure the local cache is up-to-date.
     *
     * @throws KeeperException if a ZooKeeper error occurs
     * @throws InterruptedException if the operation is interrupted
     */
    private void syncFromZooKeeper() throws KeeperException, InterruptedException {
        for (String key : zooKeeper.getChildren(STORE_NAMESPACE, false)) {
            String path = STORE_NAMESPACE + "/" + key;
            byte[] data = zooKeeper.getData(path, false, null);
            localCache.put(key, new String(data, StandardCharsets.UTF_8));
        }
    }

    /**
     * Watches for changes in ZooKeeper and updates the local cache when necessary.
     * Ensures that local copies remain consistent with the distributed store.
     *
     * @throws KeeperException if a ZooKeeper error occurs
     * @throws InterruptedException if the operation is interrupted
     */
    public void watchForUpdates() throws KeeperException, InterruptedException {
        zooKeeper.getChildren(STORE_NAMESPACE, event -> {
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                try {
                    syncFromZooKeeper(); // Refresh local cache on updates
                    watchForUpdates(); // Re-register the watch for future updates
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }
}