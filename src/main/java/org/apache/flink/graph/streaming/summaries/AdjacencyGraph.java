package org.apache.flink.graph.streaming.summaries;

import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;
import java.util.*;

/**
 * A simple, undirected adjacency list graph representation with methods for traversals.
 * Used in the Spanner library method.
 * @param <K> the vertex id type
 */
public class AdjacencyGraph<K extends Comparable<K>, W, T> implements Serializable {

    private Map<K, HashSet<Tuple3<K, W, T>>> adjacencyMap;

    public AdjacencyGraph() {
        adjacencyMap = new HashMap<>();
    }

    public Map<K, HashSet<Tuple3<K, W, T>>> getAdjacencyMap() {
        return adjacencyMap;
    }


    public Integer size()
    {
        return adjacencyMap.size();
    }
    public void addEdge(K src, K trg, W weight, T timestamp) {
        HashSet<Tuple3<K, W, T>> neighbors;
        // add the neighbor to the src
        if (adjacencyMap.containsKey(src)) {
            neighbors = adjacencyMap.get(src);
        }
        else {
            neighbors = new HashSet<>();
        }
        neighbors.add(new Tuple3<>(trg, weight, timestamp));
        adjacencyMap.put(src, neighbors);
    }

    public void reset() {
        adjacencyMap.clear();
    }

    public boolean patternFound(K src, K trg)
    {
        if (!adjacencyMap.containsKey(src))
        {
            return false;
        }
        for (Tuple3<K, W, T> neighbor : adjacencyMap.get(src))
        {
            if(neighbor.f0 == trg) return true;
        }
        return false;
    }
}
