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

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.route.RouteKind.SERVER;
import static org.reaktivity.reaktor.test.TestUtil.toTestRule;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import org.agrona.LangUtil;
import org.jmock.Expectations;
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
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusBuilder;
import org.reaktivity.nukleus.NukleusFactorySpi;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ServerIT
{
    Nukleus testNukleus;

    private static StreamFactoryBuilder serverStreamFactory;

    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery()
    {
        {
            setThreadingPolicy(new Synchroniser());
            testNukleus = mock(Nukleus.class);
            serverStreamFactory = mock(StreamFactoryBuilder.class, "serverStreamFactory");
        }
    };

    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("control", "org/reaktivity/specification/nukleus/control")
            .addScriptRoot("client", "org/reaktivity/specification/tcp/rfc793")
            .addScriptRoot("server", "org/reaktivity/specification/nukleus/tcp/streams/rfc793");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("example"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .loader(new TestClassLoader(TestNukleusFactorySpi.class.getName()))
        .clean();

    @Rule
    public final TestRule chain = outerRule(toTestRule(context)).around(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${control}/route/input/new/controller"
    })
    public void shouldRouteAsServer() throws Exception
    {
        context.checking(new Expectations()
        {
            {
                oneOf(serverStreamFactory).setRouteManager(with(any(RouteManager.class)));
            }
        });
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route/input/new/controller",
        "${control}/unroute/input/new/controller"
    })
    public void shouldUnrouteAsServer() throws Exception
    {
        context.checking(new Expectations()
        {
            {
                oneOf(serverStreamFactory).setRouteManager(with(any(RouteManager.class)));
            }
        });
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
            return builder.streamFactory(SERVER, serverStreamFactory)
                   .build();
        }

    }

    private static class TestClassLoader extends ClassLoader
    {
        private final List<URL> urls;

        TestClassLoader(String... factorySpiClassNames)
        {
            final String contents = Arrays.stream(factorySpiClassNames).collect(joining("\n"));
            URI uri = URI.create("data:," + contents);
            URL url = null;
            try
            {
                url = new URL(null, uri.toString(), new URLStreamHandler()
                {

                    @Override
                    protected URLConnection openConnection(URL url) throws IOException
                    {
                        return new URLConnection(url)
                                {

                                    @Override
                                    public void connect() throws IOException
                                    {
                                        // no-op
                                    }

                                    @Override
                                    public InputStream getInputStream() throws IOException
                                    {
                                        return new ByteArrayInputStream(contents.getBytes());
                                    }
                                };
                    }

                });
            }
            catch(IOException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
            urls = Collections.singletonList(url);
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException
        {
            return name.equals("META-INF/services/" + NukleusFactorySpi.class.getName()) ?
                        Collections.enumeration(urls)
                        :  super.getResources(name);
        }

    }

}
