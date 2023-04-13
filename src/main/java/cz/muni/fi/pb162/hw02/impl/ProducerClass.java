package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Broker;
import cz.muni.fi.pb162.hw02.mesaging.broker.Message;
import cz.muni.fi.pb162.hw02.mesaging.client.Producer;

import java.util.ArrayList;
import java.util.Collection;

public class ProducerClass implements Producer {
    private final Broker broker;

    /**
     * Creates a producer object
     * @param broker Broker to link with producer
     */
    public ProducerClass(Broker broker){
        this.broker = broker;
    }
    @Override
    public Collection<Message> produce(Collection<Message> messages) {
        return broker.push(messages);
    }

    @Override
    public Message produce(Message message) {
        Collection<Message> msgSet = new ArrayList<>();
        msgSet.add(message);
        Collection<Message> push = broker.push(msgSet);
        return push.stream().toList().get(0);
    }

    @Override
    public Broker getBroker() {
        return broker;
    }

    @Override
    public Collection<String> listTopics() {
        return broker.listTopics();
    }
}
