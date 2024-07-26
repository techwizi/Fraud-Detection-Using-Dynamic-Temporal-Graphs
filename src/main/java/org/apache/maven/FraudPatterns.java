package org.apache.maven;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.streaming.summaries.AdjacencyGraph;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class FraudPatterns implements Serializable
{
    public String MoneyMovement(AdjacencyGraph<Long, Long, Long> graph)
    {
        boolean fraud = false;
        String output = "";
        Map<Long, HashSet<Tuple3<Long, Long, Long>>> adjMap = graph.getAdjacencyMap();
        for(Long key : adjMap.keySet())
        {
            int distinctPayee;
            long totalAmt = 0;
            long minTime = Long.MAX_VALUE;
            long maxTime = 0;

            Map<Long, Long> payee = new HashMap<>();
            for(Tuple3<Long, Long, Long> edge : adjMap.get((key)))
            {
                if(!payee.containsKey(edge.f0))
                {
                    payee.put(edge.f0, 1L);
                }
                totalAmt += edge.f1;
                if(edge.f2 < minTime) minTime = edge.f2;
                if(edge.f2 > maxTime) maxTime = edge.f2;
            }
            distinctPayee = payee.size();

            if(totalAmt >= 1000000 && distinctPayee >=5 && distinctPayee <= 10 && maxTime-minTime >= 15000) {
                fraud = true;
                output += "Huge Money Movement by payer id " + key + "\n";
            }
        }

        if(!fraud) return "No fraudulent activity in this window";
        else return output;
    }
}
