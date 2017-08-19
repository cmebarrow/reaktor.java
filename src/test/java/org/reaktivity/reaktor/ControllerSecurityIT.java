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
import static org.junit.rules.RuleChain.outerRule;

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
import org.reaktivity.reaktor.test.NukleusClassLoader;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ControllerSecurityIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("control", "org/reaktivity/specification/nukleus/control");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus((name) -> "example".equals(name))
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .loader(new NukleusClassLoader(TestNukleusFactorySpi.class.getName()))
        .clean();

    @Rule
    public final TestRule chain = outerRule(k3po).around(reaktor).around(timeout);

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


}
