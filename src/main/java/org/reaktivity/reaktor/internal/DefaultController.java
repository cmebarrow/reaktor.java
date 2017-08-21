package org.reaktivity.reaktor.internal;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerSpi;
import org.reaktivity.nukleus.ControllerSpi.Authorization;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.reaktor.internal.ControllerBuilderImpl.ControllerSpiImpl;

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

    public CompletableFuture<Authorization> authorize(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return controllerSpi.doAuthorize(msgTypeId, buffer, index, length);
    }

    public CompletableFuture<Void> unauthorize(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return controllerSpi.doUnauthorize(msgTypeId, buffer, index, length);
    }

    public CompletableFuture<Authorization> resolve(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return ((ControllerSpiImpl) controllerSpi).doResolve(msgTypeId, buffer, index, length);
    }

    public CompletableFuture<Void> unresolve(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return ((ControllerSpiImpl) controllerSpi).doUnresolve(msgTypeId, buffer, index, length);
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

    public long supplyCorrelationId()
    {
        return controllerSpi.nextCorrelationId();
    }

    public <T> T supplyTarget(
        String target,
        BiFunction<ToIntFunction<MessageConsumer>, MessagePredicate, T> factory)
    {
        return controllerSpi.doSupplyTarget(target, factory);
    }

}
