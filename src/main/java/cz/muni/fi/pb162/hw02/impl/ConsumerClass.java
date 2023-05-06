package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Broker;
import cz.muni.fi.pb162.hw02.mesaging.broker.Message;
import cz.muni.fi.pb162.hw02.mesaging.client.Consumer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.TreeMap;



public class ConsumerClass extends ClientClass implements Consumer {
    private Map<String, Long> topicOffsets = new HashMap<>();

    /**
     * Creates a consumer object
     * @param broker Broker to link with consumer
     */
    public ConsumerClass(Broker broker){
        super(broker);
    }

    @Override
    public Collection<Message> consume(int num, String... topics) {

        Collection<Message> result = consume(topicOffsets, num, topics);
        HashMap<String, Long> readThisConsume = new HashMap<>();
        HashMap<String, Message> lastReadThisConsume = new HashMap<>();
        for (String topic : topics){
            readThisConsume.put(topic, (long)0);
            lastReadThisConsume.put(topic, null);
        }
        for (Message msg : result){
            for (String topic : msg.topics()){
                if (readThisConsume.get(topic) == null){
                    continue;
                }
                if (readThisConsume.get(topic) < num){
                    readThisConsume.put(topic, readThisConsume.get(topic) + 1);
                    lastReadThisConsume.put(topic, msg);
                }
            }
        }

        for (String topic : topics){
            if (lastReadThisConsume.get(topic) == null){
                continue;
            }
            topicOffsets.put(topic, (lastReadThisConsume.get(topic)).id());
        }

        return result;
    }

    @Override
    public Collection<Message> consume(Map<String, Long> offsets, int num, String... topics) {
        return broker.poll(offsets, num, Arrays.stream(topics).toList());
    }

    @Override
    public void updateOffsets(Map<String, Long> offsets) {

        topicOffsets.putAll(offsets);
    }

    @Override
    public void clearOffsets() {
        topicOffsets = new HashMap<>();
    }

    @Override
    public void setOffsets(Map<String, Long> offsets) {
        topicOffsets = new HashMap<>(offsets);
    }

    @Override
    public Map<String, Long> getOffsets() {
        return new TreeMap<>(topicOffsets);
    }
}
