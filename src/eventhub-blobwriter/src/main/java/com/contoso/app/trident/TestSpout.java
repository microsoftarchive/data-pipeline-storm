// Copyright (c) Microsoft Corporation. All rights reserved. See License.txt in the project root for license information.

package com.contoso.app.trident;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.spout.ITridentSpout;
import java.util.Map;

@SuppressWarnings("rawtypes")
public class TestSpout implements ITridentSpout<Long> {
    private static final long serialVersionUID = 1L;
    BatchCoordinator<Long> coordinator = new TestCoordinator();
    Emitter<Long> emitter = new TestEmitter();

    @Override
    public BatchCoordinator<Long> getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return coordinator;
    }

    @Override
    public Emitter<Long> getEmitter(String txStateId, Map conf, TopologyContext context) {
        return emitter;
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("message");
    }
}
