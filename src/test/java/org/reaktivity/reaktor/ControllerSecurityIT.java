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
package org.reaktivity.reaktor;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.nativeOrder;
import static java.util.Arrays.spliterator;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.reaktor.test.TestUtil.toTestRule;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerBuilder;
import org.reaktivity.nukleus.ControllerFactorySpi;
import org.reaktivity.nukleus.ControllerSpi;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusBuilder;
import org.reaktivity.nukleus.NukleusFactorySpi;
import org.reaktivity.reaktor.internal.types.control.AuthorizeFW;
import org.reaktivity.reaktor.internal.types.control.Role;
import org.reaktivity.reaktor.internal.types.control.RouteFW;
import org.reaktivity.reaktor.internal.types.control.UnauthorizeFW;
import org.reaktivity.reaktor.internal.types.control.UnrouteFW;
import org.reaktivity.reaktor.test.NukleusClassLoader;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ControllerSecurityIT
{
    Nukleus securityNukleus;
    Nukleus exampleNukleus;

    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery()
    {
        {
            setThreadingPolicy(new Synchroniser());
            exampleNukleus = mock(Nukleus.class, "exampleNukleus");
            securityNukleus = mock(Nukleus.class, "securityNukleus");
        }
    };

    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("control", "org/reaktivity/specification/nukleus/control");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .controller((c) -> c.isAssignableFrom(TestSecurityController.class))
        .nukleus((name) -> "example".equals(name) || "security".equals(name))
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .loader(new NukleusClassLoader(TestNukleusFactorySpi.class.getName(),
                                       TestSecurityNukleusFactorySpi.class.getName()))
        .clean();

    @Rule
    public final TestRule chain = outerRule(toTestRule(context)).around(k3po).around(reaktor).around(timeout);

    @Test
    @Specification({
        "${control}/route/server/controller",
        "${control}/resolve/succeeds/nukleus", // simulates security nukleus
        "${control}/authorize/succeeds/controller"
    })
    public void shouldAuthoriseRouteAsServer() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route/server/controller",
        "${control}/resolve/fails.too.many.roles/nukleus", // simulates security nukleus
        "${control}/authorize/fails.too.many.roles/controller"
    })
    public void shouldNotAuthoriseRouteAsServerWhenMaxNumberOfRolesIsExceeded() throws Exception
    {
        k3po.finish();
    }

    public static class TestNukleusFactorySpi implements NukleusFactorySpi
    {

        @Override
        public String name()
        {
           return "example";
        }

        @Override
        public Nukleus create(Configuration config, NukleusBuilder builder)
        {
            return builder.build();
        }

    }

    public static class TestSecurityNukleusFactorySpi implements NukleusFactorySpi
    {

        @Override
        public String name()
        {
           return "security";
        }

        @Override
        public Nukleus create(Configuration config, NukleusBuilder builder)
        {
            return builder.build();
        }

    }

    public final class TestSecurityControllerFactorySpi implements ControllerFactorySpi<TestSecurityController>
    {
        @Override
        public String name()
        {
            return "tcp";
        }

        @Override
        public Class<TestSecurityController> kind()
        {
            return TestSecurityController.class;
        }

        @Override
        public TestSecurityController create(
            Configuration config,
            ControllerBuilder<TestSecurityController> builder)
        {
            return builder.setName(name())
                    .setFactory(TestSecurityController::new)
                    .build();
        }

    }


    public final class TestSecurityController implements Controller
    {
        private static final int MAX_SEND_LENGTH = 1024; // TODO: Configuration and Context

        // TODO: thread-safe flyweights or command queue from public methods
        private final RouteFW.Builder routeRW = new RouteFW.Builder();
        private final UnrouteFW.Builder unrouteRW = new UnrouteFW.Builder();
        private final AuthorizeFW.Builder authorizeRW = new AuthorizeFW.Builder();
        private final UnauthorizeFW.Builder unauthorizeRW = new UnauthorizeFW.Builder();


        private final ControllerSpi controllerSpi;
        private final AtomicBuffer atomicBuffer;

        public TestSecurityController(ControllerSpi controllerSpi)
        {
            this.controllerSpi = controllerSpi;
            this.atomicBuffer = new UnsafeBuffer(allocateDirect(MAX_SEND_LENGTH).order(nativeOrder()));
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
        public Class<TestSecurityController> kind()
        {
            return TestSecurityController.class;
        }

        @Override
        public String name()
        {
            return "tcp";
        }

        public CompletableFuture<Long> routeServer(
            String source,
            long sourceRef,
            String target,
            long targetRef,
            InetAddress address)
        {
            return route(Role.SERVER, source, sourceRef, target, targetRef, address);
        }

        public CompletableFuture<Long> routeClient(
            String source,
            long sourceRef,
            String target,
            long targetRef,
            InetAddress address)
        {
            return route(Role.CLIENT, source, sourceRef, target, targetRef, address);
        }

        public CompletableFuture<Void> unrouteServer(
            String source,
            long sourceRef,
            String target,
            long targetRef,
            InetAddress address)
        {
            return unroute(Role.SERVER, source, sourceRef, target, targetRef, address);
        }

        public CompletableFuture<Void> unrouteClient(
            String source,
            long sourceRef,
            String target,
            long targetRef,
            InetAddress address)
        {
            return unroute(Role.CLIENT, source, sourceRef, target, targetRef, address);
        }

        public CompletableFuture<ControllerSpi.Authorization> authorize(
            long sourceRef,
            String securityNukleus,
            String[] roles)
        {

            long correlationId = controllerSpi.nextCorrelationId();

            AuthorizeFW authorizeRO = authorizeRW.wrap(atomicBuffer, 0, atomicBuffer.capacity())
                                     .correlationId(correlationId)
                                     .sourceRef(sourceRef)
                                     .securityNukleus(securityNukleus)
                                     .roles((b) -> spliterator(roles).forEachRemaining((role) -> b.item(
                                             (sb) -> sb.set(role,  StandardCharsets.UTF_8))))
                                     .build();

            return controllerSpi.doAuthorize(authorizeRO.typeId(), authorizeRO.buffer(),
                    authorizeRO.offset(), authorizeRO.sizeof());
        }

        public CompletableFuture<Void> unauthorize(
            long sourceRef,
            String securityNukleus)
        {

            long correlationId = controllerSpi.nextCorrelationId();

            UnauthorizeFW unauthorizeRO = unauthorizeRW.wrap(atomicBuffer, 0, atomicBuffer.capacity())
                                     .correlationId(correlationId)
                                     .sourceRef(sourceRef)
                                     .securityNukleus(securityNukleus)
                                     .build();

            return controllerSpi.doUnauthorize(unauthorizeRO.typeId(), unauthorizeRO.buffer(),
                    unauthorizeRO.offset(), unauthorizeRO.sizeof());
        }

        public long count(String name)
        {
            return controllerSpi.doCount(name);
        }

        private CompletableFuture<Long> route(
            Role role,
            String source,
            long sourceRef,
            String target,
            long targetRef,
            InetAddress address)
        {
            long correlationId = controllerSpi.nextCorrelationId();

            RouteFW routeRO = routeRW.wrap(atomicBuffer, 0, atomicBuffer.capacity())
                                     .correlationId(correlationId)
                                     .role(b -> b.set(role))
                                     .source(source)
                                     .sourceRef(sourceRef)
                                     .target(target)
                                     .targetRef(targetRef)
                                     .extension(b -> b.reset())
                                     .build();

            return controllerSpi.doRoute(routeRO.typeId(), routeRO.buffer(), routeRO.offset(), routeRO.sizeof());
        }

        private CompletableFuture<Void> unroute(
            Role role,
            String source,
            long sourceRef,
            String target,
            long targetRef,
            InetAddress address)
        {
            long correlationId = controllerSpi.nextCorrelationId();

            UnrouteFW unrouteRO = unrouteRW.wrap(atomicBuffer, 0, atomicBuffer.capacity())
                                     .correlationId(correlationId)
                                     .role(b -> b.set(role))
                                     .source(source)
                                     .sourceRef(sourceRef)
                                     .target(target)
                                     .targetRef(targetRef)
                                     .extension(b -> b.reset())
                                     .build();

            return controllerSpi.doUnroute(unrouteRO.typeId(), unrouteRO.buffer(), unrouteRO.offset(), unrouteRO.sizeof());
        }

    }


}
