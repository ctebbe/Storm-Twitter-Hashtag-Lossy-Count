package util;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

/**
 * Created by ctebbe
 */
public class Util {
    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
}