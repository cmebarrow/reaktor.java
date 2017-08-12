package org.reaktivity.reaktor.test;

import java.lang.reflect.Field;
import org.junit.internal.runners.statements.InvokeMethod;
import org.junit.rules.MethodRule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

public final class TestUtil
{

    public static TestRule toTestRule(final MethodRule in)
    {
        return new TestRule()
        {

            @Override
            public Statement apply(Statement base, Description description)
            {
                if (base instanceof InvokeMethod)
                {
                    return doApplyInvokeMethod(in, base, (InvokeMethod) base);
                }

                return in.apply(base, null, description);
            }

            private Statement doApplyInvokeMethod(MethodRule in, Statement base, InvokeMethod invokeMethod)
            {
                try
                {
                    FrameworkMethod frameworkMethod = (FrameworkMethod) FIELD_FRAMEWORK_METHOD.get(invokeMethod);
                    Object target = FIELD_TARGET.get(invokeMethod);

                    return in.apply(base, frameworkMethod, target);
                }
                catch (IllegalArgumentException ex)
                {
                    throw new RuntimeException(ex);
                }
                catch (IllegalAccessException ex)
                {
                    throw new RuntimeException(ex);
                }
            }

        };
    }

    private TestUtil()
    {

    }

    private static final Field FIELD_TARGET;
    private static final Field FIELD_FRAMEWORK_METHOD;

    static
    {
        try
        {
            final Field target = InvokeMethod.class.getDeclaredField("target");
            final Field frameworkMethod = InvokeMethod.class.getDeclaredField("testMethod");

            target.setAccessible(true);
            frameworkMethod.setAccessible(true);

            FIELD_TARGET = target;
            FIELD_FRAMEWORK_METHOD = frameworkMethod;
        }
        catch (NoSuchFieldException ex)
        {
            throw new RuntimeException(ex);
        }
        catch (SecurityException ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
