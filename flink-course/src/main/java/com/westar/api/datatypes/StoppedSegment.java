package com.westar.api.datatypes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *  从车的起始开始到 speed = 0 的发出的所有的事件
 */
public class StoppedSegment extends Segment {
    public StoppedSegment(Iterable<ConnectedCarEvent> events){
        List<ConnectedCarEvent> list = new ArrayList<>();
        for(Iterator<ConnectedCarEvent> itor =events.iterator();itor.hasNext(); ){
            ConnectedCarEvent event = itor.next();
            if (event.getSpeed() != 0.0) { // speed 等于 0 的事件不算
                list.add(event);
            }
        }

        this.setLength(list.size());

        if (this.getLength() > 0) {
            this.setCarId(list.get(0).getCarId());
            this.setStartTime(Segment.minTimestamp(list));
            this.setMaxSpeed(Segment.maxSpeed(list));
            this.setErraticness(Segment.stddevThrottle(list));
        }
    }
}
