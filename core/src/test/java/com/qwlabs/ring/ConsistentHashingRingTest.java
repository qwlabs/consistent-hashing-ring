package com.qwlabs.ring;


import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class ConsistentHashingRingTest {
    private ConsistentHashingRing ring;

    @BeforeEach
    void setUp() {
        LongHashFunction longHashFunction = LongHashFunction.xx();
        ring = new ConsistentHashingRing("Test Ring", longHashFunction::hashChars);
    }

    @Test
    void test_remove() {
        for (int index = 0; index <= 50; index++) {
            ring.add("node" + index);
        }
        Set<String> buckets = ring.replicateBuckets("1", 5);
        System.out.println(buckets);
        ring.remove(buckets.iterator().next());
        System.out.println(ring.replicateBuckets("1", 5));
        System.out.println(ring);
    }

    @Test
    void test_add() {
        for (int index = 0; index <= 50; index++) {
            ring.add("node" + index);
        }
        Set<String> buckets = ring.replicateBuckets("1", 5);
        System.out.println(buckets);
        ring.add("node101");
        System.out.println(ring.replicateBuckets("1", 5));
        System.out.println(ring);
    }
}
