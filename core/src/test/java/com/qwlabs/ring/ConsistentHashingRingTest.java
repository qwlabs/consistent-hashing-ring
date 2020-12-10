package com.qwlabs.ring;


import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;

public class ConsistentHashingRingTest {
    private ConsistentHashingRing ring;

    @BeforeEach
    void setUp() {
        LongHashFunction longHashFunction = LongHashFunction.xx();
        ring = new ConsistentHashingRing("Test Ring", longHashFunction::hashChars);
    }

    @Test
    void test() {
        ring.add("node1");
        ring.add("node2");
        ring.add("node3");
        ring.add("node4");
        ring.add("node5");
        ring.add("node6");
        ring.add("node7");
        Set<String> buckets = ring.replicateBuckets("1", 2);
        System.out.println(buckets);
        ring.remove(buckets.iterator().next());
        System.out.println(ring.replicateBuckets("1", 2));
        System.out.println(ring);
    }
}
