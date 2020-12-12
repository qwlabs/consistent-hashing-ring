package com.qwlabs.ring;


import net.openhft.hashing.LongHashFunction;
import org.checkerframework.org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class ConsistentHashingRingTest {
    private ConsistentHashingRing ring;

    @BeforeEach
    void setUp() {
        LongHashFunction longHashFunction = LongHashFunction.xx(Long.MAX_VALUE);
        ring = new ConsistentHashingRing("Test Ring", longHashFunction::hashChars);
    }

    @Test
    void test_remove() {
        for (int index = 0; index <= 10; index++) {
            ring.add(createNode());
        }
        Set<String> nodes = ring.replicateNodes("1", 5);
        System.out.println(nodes);
        String removeNode = nodes.iterator().next();
        System.out.println("remove node:" + removeNode);
        ring.remove(removeNode);
        System.out.println(ring.replicateNodes("1", 5));
        System.out.println(ring);
    }

    @Test
    void test_add() {
        for (int index = 0; index <= 10; index++) {
            ring.add(createNode());
        }
        Set<String> nodes = ring.replicateNodes("1", 3);
        System.out.println(nodes);
        String createNode = createNode();
        System.out.println("add node:" + createNode);
        ring.add(createNode);
        System.out.println(ring.replicateNodes("1", 3));
        System.out.println(ring);
    }

    private String createNode(){
        return RandomStringUtils.randomAlphanumeric(5);
    }
}
