package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Broker;
import cz.muni.fi.pb162.hw02.mesaging.broker.Message;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.Map;
import java.util.Set;

public class BrokerClass implements Broker {

    private final HashMap<String, TreeSet<MessageWrap>> topics = new HashMap<>();
    @Override
    public Collection<String> listTopics() {
        return topics.keySet().stream().toList();
    }
    @Override
    public Collection<Message> push(Collection<Message> messages) {
        HashSet<Message> wraps = new HashSet<>();
        for (Message msg : messages){

            MessageWrap wrappedMessage = new MessageWrap(msg);
            wraps.add(wrappedMessage);
            for (String topic : msg.topics()){

                if (topics.containsKey(topic)) {
                    topics.get(topic).add(wrappedMessage);
                } else {
                    TreeSet<MessageWrap> initial = new TreeSet<>();
                    initial.add(wrappedMessage);
                    topics.put(topic, initial);

                }
            }
        }
        return wraps;
    }

    @Override
    public Collection<Message> poll(Map<String, Long> offsets, int num, Collection<String> topics) {
        TreeSet<Message> pollResult = new TreeSet<>();

        for (String topic : offsets.keySet()) {
            if (!topics.contains(topic)){
                continue;
            }
            int fetchedCount = 0;
            for (MessageWrap wrap : this.topics.get(topic)) {
                if (offsets.get(topic) < wrap.id() && !pollResult.contains(wrap)) {
                    if (fetchedCount >= num){
                        break;
                    }
                    fetchedCount++;
                    pollResult.add(wrap);
                }
            }
        }
        Set<String> setCopy = new HashSet<>();
        setCopy.addAll(topics);
        setCopy.removeAll(offsets.keySet());

        for (String topic : setCopy){
            int fetchedCount = 0;
            if (this.topics.get(topic) == null){
                continue;
            }
            for (MessageWrap wrap : this.topics.get(topic)){
                if (fetchedCount >= num){
                    break;
                }
                pollResult.add(wrap);
                fetchedCount++;
            }
        }
        return pollResult;
    }
}
