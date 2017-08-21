/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.reaktor.internal;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerSpi;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;

public final class DefaultController implements Controller
{
    private final ControllerSpi controllerSpi;
    private final String name;

    public static DefaultController create(Configuration configuration, String name)
    {
        return new ControllerBuilderImpl<DefaultController>(configuration, DefaultController.class)
                .setName(name)
                .setFactory(controllerSpi -> new DefaultController(controllerSpi, name))
                .build();
    }

    public DefaultController(
        ControllerSpi controllerSpi,
        String name)
    {
        this.controllerSpi = controllerSpi;
        this.name = name;
    }

    @Override
    public int process()
    {
        return controllerSpi.doProcess();
    }

    @Override
    public void close() throws Exception
    {
        controllerSpi.doClose();
    }

    @Override
    public Class<DefaultController> kind()
    {
        return DefaultController.class;
    }

    @Override
    public String name()
    {
        return name;
    }

    public CompletableFuture<Long> route(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return controllerSpi.doRoute(msgTypeId, buffer, index, length);
    }

    public CompletableFuture<Void> unroute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return controllerSpi.doUnroute(msgTypeId, buffer, index, length);
    }

    public <T> T supplySource(
        String source,
        BiFunction<MessagePredicate, ToIntFunction<MessageConsumer>, T> factory)
    {
        return controllerSpi.doSupplySource(source, factory);
    }

    public <T> T supplyTarget(
        String target,
        BiFunction<ToIntFunction<MessageConsumer>, MessagePredicate, T> factory)
    {
        return controllerSpi.doSupplyTarget(target, factory);
    }

}
