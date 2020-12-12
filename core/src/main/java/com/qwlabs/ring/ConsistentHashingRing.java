package com.qwlabs.ring;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;


public class ConsistentHashingRing {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsistentHashingRing.class);
    public static final int VIRTUAL_NODE_COUNT_PER_NODE = 500;
    private static final int MAX_SPIN_RETRY_TIMES = 40;
    private static final long SPIN_MILLIS_PER_ONE_TIME = 100L;
    private static final long SPIN_MILLIS_WARNING = (MAX_SPIN_RETRY_TIMES * SPIN_MILLIS_PER_ONE_TIME) / 2;
    @NonNull
    private final String name;
    @NonNull
    private final HashFunction hashFunction;
    @NonNull
    private final Supplier<SpinListener> spinListenerSupplier;
    @VisibleForTesting
    @NonNull
    protected volatile Ring ring;

    public ConsistentHashingRing(@NonNull String name,
                                 @NonNull HashFunction hashFunction) {
        this(name, hashFunction, DefaultSpinListener::new);
    }

    public ConsistentHashingRing(@NonNull String name,
                                 @NonNull HashFunction hashFunction,
                                 @NonNull Supplier<SpinListener> spinListenerSupplier) {
        this.name = name;
        this.hashFunction = hashFunction;
        this.spinListenerSupplier = spinListenerSupplier;
        this.ring = new Ring(new Node[0], ImmutableSet.of());
    }

    public synchronized void add(@NonNull String name) {
        Preconditions.checkNotNull(name, "Name must not be null");
        Ring oldRing = this.ring;
        if (oldRing.nodeNames.contains(name)) {
            return;
        }
        LOGGER.trace("Add node {} to the {}.", name, this.name);
        TreeSet<Node> treeSet = new TreeSet<>();
        for (int virtualNodeIndex = 0; virtualNodeIndex < VIRTUAL_NODE_COUNT_PER_NODE; virtualNodeIndex++) {
            treeSet.add(new Node(name, virtualNodeIndex, this.hashFunction));
        }
        Node[] newNodes = new Node[oldRing.nodes.length + VIRTUAL_NODE_COUNT_PER_NODE];
        int oldIndex = 0;
        Node oldNode = (oldIndex < oldRing.nodes.length) ? oldRing.nodes[oldIndex] : null;
        Iterator<Node> iterator = treeSet.iterator();
        Node newNode = iterator.next();
        for (int newIndex = 0; newIndex < newNodes.length; newIndex++) {
            if (oldNode != null && (newNode == null || oldNode.compareTo(newNode) < 0)) {
                newNodes[newIndex] = new Node(oldNode.name, oldNode.index, oldNode.hash);
                oldNode = (++oldIndex < oldRing.nodes.length) ? oldRing.nodes[oldIndex] : null;
            } else {
                newNodes[newIndex] = newNode;
                newNode = iterator.hasNext() ? iterator.next() : null;
            }
        }
        ImmutableSet<String> nodeNames = ImmutableSet.<String>builderWithExpectedSize(oldRing.nodeNames.size() + 1)
                .addAll(oldRing.nodeNames)
                .add(name)
                .build();
        this.ring = new Ring(newNodes, nodeNames);
    }

    public synchronized void remove(@NonNull String name) {
        Preconditions.checkNotNull(name, "Name must not be null");
        Ring oldRing = this.ring;
        if (!oldRing.nodeNames.contains(name)) {
            return;
        }
        LOGGER.trace("Remove node {} from the {}.", name, this.name);
        Node[] newNodes = new Node[oldRing.nodes.length - VIRTUAL_NODE_COUNT_PER_NODE];
        int oldIndex = 0;
        int newIndex = 0;
        while (oldIndex < oldRing.nodes.length) {
            Node oldNode = oldRing.nodes[oldIndex];
            if (!oldNode.name.equals(name)) {
                newNodes[newIndex] = new Node(oldNode.name, oldNode.index, oldNode.hash);
                newIndex++;
            }
            oldIndex++;
        }
        HashSet<String> newNodeNames = new HashSet<>(oldRing.nodeNames);
        newNodeNames.remove(name);
        this.ring = new Ring(newNodes, ImmutableSet.copyOf(newNodeNames));
    }


    @NonNull
    public String responsibleNode(@NonNull String key) {
        String responsibleNode = responsibleNode(key, true);
        return Preconditions.checkNotNull(responsibleNode, "after spinning ResponsibleNode must not be null");
    }

    @Nullable
    public String responsibleNode(@NonNull String key, boolean spinning) {
        Preconditions.checkNotNull(key, "key must not be null");
        Ring ring = spin(spinning);
        int index = search(ring, key);
        return (index < 0) ? null : (ring.nodes[index]).name;
    }

    @NonNull
    public ImmutableSet<String> replicateNodes(@NonNull String key, int replicas) {
        return replicateNodes(key, replicas, true);
    }


    @NonNull
    public ImmutableSet<String> replicateNodes(@NonNull String key, int replicas, boolean spinning) {
        Preconditions.checkNotNull(key, "key must not be null");
        Ring ring = spin(spinning);
        int nodeIndex = search(ring, key);
        if (nodeIndex < 0) {
            return ImmutableSet.of();
        }
        int maxReplicateNodes = ring.nodeNames.size();
        if (replicas > maxReplicateNodes - 1) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("There are not enough buckets in the consistent hash ring for {} replicas.", replicas);
            }
            replicas = maxReplicateNodes - 1;
        }
        Node node = ring.nodes[nodeIndex];
        ImmutableSet<String>[] originalReplicateNodes = node.replicateNodes;
        if (originalReplicateNodes != null) {
            for (ImmutableSet<String> nodes : originalReplicateNodes) {
                if (nodes.size() == replicas) {
                    return nodes;
                }
            }
        }
        HashSet<String> replicateNodes = new HashSet<>();
        while (replicateNodes.size() < replicas) {
            if (++nodeIndex == ring.nodes.length) {
                nodeIndex = 0;
            }
            String nodeName = ring.nodes[nodeIndex].name;
            if (!nodeName.equals(node.name)) {
                replicateNodes.add(nodeName);
            }
        }
        ImmutableSet<String> resultNodes = ImmutableSet.copyOf(replicateNodes);
        ImmutableSet<String>[] newNodes;
        if (originalReplicateNodes == null) {
            newNodes = new ImmutableSet[]{resultNodes};
        } else {
            newNodes = Arrays.copyOf(originalReplicateNodes, originalReplicateNodes.length + 1);
            newNodes[newNodes.length - 1] = resultNodes;
        }
        node.replicateNodes = newNodes;
        return resultNodes;
    }

    @NonNull
    public Set<String> nodes() {
        return this.ring.nodeNames;
    }

    public int size() {
        return this.ring.nodeNames.size();
    }

    private int search(@NonNull Ring ring, @NonNull String key) {
        if (ring.nodes.length == 0) {
            return -1;
        }
        long hash = this.hashFunction.hash(key);
        if (hash > (ring.nodes[ring.nodes.length - 1]).hash) {
            return 0;
        }
        int midIndex = 0;
        int highIndex;
        int lowIndex;
        for (highIndex = ring.nodes.length - 1; midIndex <= highIndex; highIndex = lowIndex - 1) {
            lowIndex = midIndex + highIndex >>> 1;
            Node node = ring.nodes[lowIndex];
            if (hash > node.hash) {
                midIndex = lowIndex + 1;
                continue;
            }
        }
        return Math.max(midIndex, highIndex);
    }

    @NonNull
    private Ring spin() {
        Ring result = this.ring;
        if (result.nodes.length == 0) {
            SpinListener spinListener = spinListenerSupplier.get();
            spinListener.onStart();
            long nowMillis = System.currentTimeMillis();
            for (int retryTimes = 0; result.nodes.length == 0; retryTimes++) {
                try {
                    Thread.sleep(SPIN_MILLIS_PER_ONE_TIME);
                } catch (InterruptedException e) {
                    LOGGER.debug("Spin while consistent hashing is empty was interrupted", e);
                }
                if (System.currentTimeMillis() - nowMillis > SPIN_MILLIS_WARNING) {
                    LOGGER.warn("Node is waiting for an operational cluster member");
                    nowMillis = System.currentTimeMillis();
                }
                if (retryTimes == MAX_SPIN_RETRY_TIMES) {
                    spinListener.onOutOfRetryTimes();
                }
                result = this.ring;
            }
            spinListener.onStop();
        }
        return result;
    }

    @NonNull
    private Ring spin(boolean spinning) {
        Ring ring;
        if (spinning) {
            ring = spin();
        } else {
            ring = this.ring;
        }
        return ring;
    }


    @VisibleForTesting
    protected static class Node implements Comparable<Node> {
        @NonNull
        protected final String name;

        protected final int index;

        protected final long hash;

        @Nullable
        protected volatile ImmutableSet<String>[] replicateNodes;

        protected Node(@NonNull String name, int index, @NonNull HashFunction hashFunction) {
            this.name = name;
            this.index = index;
            this.hash = hashFunction.hash(toString());
        }

        protected Node(@NonNull String name, int index, long hash) {
            this.name = name;
            this.index = index;
            this.hash = hash;
        }

        @Override
        public int compareTo(Node o) {
            return (this.hash > o.hash) ? 1 : ((this.hash < o.hash) ? -1 : toString().compareTo(o.toString()));
        }

        @NonNull
        public String toString() {
            return this.name + this.index;
        }
    }

    protected static class Ring {
        protected final Node[] nodes;
        @NonNull
        protected final ImmutableSet<String> nodeNames;

        Ring(@NonNull Node[] nodes, @NonNull ImmutableSet<String> nodeNames) {
            this.nodes = nodes;
            this.nodeNames = nodeNames;
        }
    }

}
