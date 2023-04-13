package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Broker;
import cz.muni.fi.pb162.hw02.mesaging.broker.Message;
import cz.muni.fi.pb162.hw02.mesaging.client.Consumer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.TreeMap;



public class ConsumerClass implements Consumer {
    private Map<String, Long> topicOffsets = new HashMap<>();
    private final Broker broker;

    /**
     * Creates a consumer object
     * @param broker Broker to link with consumer
     */
    public ConsumerClass(Broker broker){
        this.broker = broker;
    }

    @Override
    public Broker getBroker() {
        return broker;
    }

    @Override
    public Collection<String> listTopics() {
        return broker.listTopics();
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
        for (String topic : offsets.keySet()){
            topicOffsets.put(topic, offsets.get(topic));
        }
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
