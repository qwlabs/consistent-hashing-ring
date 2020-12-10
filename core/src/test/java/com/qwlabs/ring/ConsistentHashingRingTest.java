package com.qwlabs.ring;


import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

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
        System.out.println(ring.responsibleNode("1"));
        System.out.println(ring.replicateBuckets("1", 1));
        System.out.println(ring.replicateBuckets("1", 2));
    }
}
