package data;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ctebbe
 */
public class LossyCountingBuckets implements Serializable {

    private int n;
    private int currBucket;
    private int sizeBucket;
    private final List<LossyCountingElement> bucket = Lists.newArrayList();

    public LossyCountingBuckets(int size) {
        sizeBucket = size;
        n = 0;
        currBucket = 1;
    }

    public void insert(String element) {
        findOrInsert(element);
        if(++n % sizeBucket == 0) { // bucket is full
            deletePhase();
            currBucket++;
        }
    }

    private void findOrInsert(String hashtag) {
        LossyCountingElement element = null;
        for(LossyCountingElement e : bucket) {
            if(e.element.equalsIgnoreCase(hashtag)) {
                element = e;
            }
        }
        if(element == null)
            element = new LossyCountingElement(hashtag, currBucket-1);
        bucket.add(element);
    }

    /*
        remove elements that satisfy freq + delta <= currBucket
     */
    private void deletePhase() {
        for(LossyCountingElement e : bucket) {
            if(e.frequency + e.delta <= currBucket)
                bucket.remove(e);
        }
    }

    public List<LossyCountingElement> getResults() {
        List<LossyCountingElement> l = Lists.newArrayList(bucket);
        bucket.clear();
        return l;
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
