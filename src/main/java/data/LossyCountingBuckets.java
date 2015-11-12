package data;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ctebbe
 */
public class LossyCountingBuckets {

    private int n;
    private int currBucket;
    private int sizeBucket;
    private final Map<String, LossyCountingElement> buckets = new HashMap<>();

    public LossyCountingBuckets(int size) {
        sizeBucket = size;
        n = 0;
        currBucket = 1;
    }

    public void insert(String element) {
        if(buckets.containsKey(element))
            buckets.get(element).increment();
        else
            buckets.put(element, new LossyCountingElement(element, currBucket-1));

        if(++n % sizeBucket == 0) { // bucket is full
            deletePhase();
            currBucket++;
        }
    }

    /*
        remove elements that satisfy freq + delta <= currBucket
     */
    private void deletePhase() {
        for(Map.Entry<String, LossyCountingElement> entry : buckets.entrySet()) {
            if(entry.getValue().frequency + entry.getValue().delta <= currBucket)
                buckets.remove(entry.getKey());
        }
    }

    public String getResults() {
        buckets.clear();
        return null;
    } 

    public static void main(String[] args) {
        LossyCountingBuckets buckets = new LossyCountingBuckets(5);
        buckets.insert("1");
        buckets.insert("2");
        buckets.insert("4");
        buckets.insert("3");
        buckets.insert("4");

        buckets.insert("3");
        buckets.insert("4");
        buckets.insert("5");
        buckets.insert("4");
        buckets.insert("6");

        buckets.insert("7");
        buckets.insert("3");
        buckets.insert("3");
        buckets.insert("6");
        buckets.insert("1");

        buckets.insert("1");
        buckets.insert("3");
        buckets.insert("2");
        buckets.insert("4");
        buckets.insert("7");
        System.out.println(buckets.getResults());
    }
}
