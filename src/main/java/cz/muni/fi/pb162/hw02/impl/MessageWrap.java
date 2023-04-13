package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Message;

import java.util.Map;
import java.util.Set;

public class MessageWrap implements Message, Comparable<MessageWrap> {
    private static long globalIdCount = 1;
    private final Message wrappedMessage;
    private final long id;
    @Override
    public int compareTo(MessageWrap o2) {
        return Long.compare(this.id(), o2.id());
    }

    /**
     * Wraps a Message in MessageWrap object
     * @param msg Message to be wrapped
     */
    public MessageWrap(Message msg){
        wrappedMessage = msg;
        id = globalIdCount++;
    }

    @Override
    public Long id() {
        return id;
    }

    @Override
    public Set<String> topics() {
        return wrappedMessage.topics();
    }

    @Override
    public Map<String, Object> data() {
        return wrappedMessage.data();
    }
}
