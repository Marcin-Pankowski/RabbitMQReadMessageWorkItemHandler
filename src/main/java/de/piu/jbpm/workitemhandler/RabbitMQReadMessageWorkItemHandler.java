/*
 * Copyright 2020 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.piu.jbpm.workitemhandler;

import java.util.HashMap;
import java.util.Map;
import org.jbpm.process.workitem.core.AbstractLogOrThrowWorkItemHandler;
import org.jbpm.process.workitem.core.util.RequiredParameterValidator;
import org.kie.api.runtime.process.WorkItem;
import org.kie.api.runtime.process.WorkItemManager;
import org.jbpm.process.workitem.core.util.Wid;
import org.jbpm.process.workitem.core.util.WidParameter;
import org.jbpm.process.workitem.core.util.WidResult;
import org.jbpm.process.workitem.core.util.service.WidAction;
import org.jbpm.process.workitem.core.util.service.WidAuth;
import org.jbpm.process.workitem.core.util.service.WidService;
import org.jbpm.process.workitem.core.util.WidMavenDepends;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.kie.internal.runtime.Cacheable;

@Wid(widfile="${name}.wid", name="${name}",
        displayName="${name}",
        defaultHandler="mvel: new de.piu.jbpm.workitemhandler.RabbitMQReadMessageWorkItemHandler()",
        category = "${artifactId}",
        icon = "trueDefinitions.png",
        parameters={
                @WidParameter(name="Queue", required = true),
                @WidParameter(name="Message", required = true)
        },
        results={
            @WidResult(name="MessageSendResult")
        },
        mavenDepends={
                @WidMavenDepends(group = "${groupId}", artifact = "${artifactId}", version = "${version}")
        },
        serviceInfo = @WidService(category = "${name}", description = "${description}",
                keywords = "rabbitMQ, send, task",
                action = @WidAction(title = "Send RabbitMQ-Message"),
                authinfo = @WidAuth(required = true, params = {"BrokerURI"},
                        paramsdescription = {"Broker URI (amqp://user:pass@host:10000/vhost)"})
        )
)
public class RabbitMQReadMessageWorkItemHandler extends AbstractLogOrThrowWorkItemHandler implements Cacheable {
    private static final String EXCHANGE_NAME = "amq.direct";
    private String brokerURI;
        private String queue;
        private String message;

    public RabbitMQReadMessageWorkItemHandler(String BrokerURI, String Queue, String Message){
            this.brokerURI = BrokerURI;
            this.queue = Queue;
            this.message = Message;
        }

    public void executeWorkItem(WorkItem workItem, WorkItemManager manager) {
        try {
            RequiredParameterValidator.validate(this.getClass(), workItem);

            //  parameters
            brokerURI = (String) workItem.getParameter("BrokerURI");
            queue = (String) workItem.getParameter("Queue");
            message = (String) workItem.getParameter("Message");


            // complete workitem impl...

            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(brokerURI);
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
                channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + message + "'");
            }



            // return results
            Map<String, Object> results = new HashMap<String, Object>();
            Object messageSendResult = "";
            results.put("MessageSendResult", messageSendResult);


            manager.completeWorkItem(workItem.getId(), results);
        } catch(Throwable cause) {
            handleException(cause);
        }
    }

    @Override
    public void abortWorkItem(WorkItem workItem,
                              WorkItemManager manager) {
        // stub
    }

    @Override
    public void close() {

    }
}


