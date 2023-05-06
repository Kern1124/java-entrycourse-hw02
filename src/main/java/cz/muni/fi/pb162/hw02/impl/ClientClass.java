package cz.muni.fi.pb162.hw02.impl;

import cz.muni.fi.pb162.hw02.mesaging.broker.Broker;
import cz.muni.fi.pb162.hw02.mesaging.client.Client;

import java.util.Collection;

public class ClientClass implements Client {

    protected final Broker broker;

    /**
     * Constructor for ClientClass
     * @param broker to set a broker attribute
     */
    public ClientClass(Broker broker){
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
}
