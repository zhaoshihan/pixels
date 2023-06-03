package io.pixelsdb.pixels.invoker.vhive;

import io.pixelsdb.pixels.common.turbo.FunctionService;
import io.pixelsdb.pixels.common.turbo.Invoker;
import io.pixelsdb.pixels.common.turbo.InvokerProvider;
import io.pixelsdb.pixels.common.turbo.WorkerType;

public class HelloInvokerProvider implements InvokerProvider {
    @Override
    public Invoker createInvoker() {
        return new HelloInvoker("HelloWorker");
    }

    @Override
    public WorkerType workerType() {
        return WorkerType.HELLO;
    }

    @Override
    public boolean compatibleWith(FunctionService functionService) {
        return functionService.equals(FunctionService.vhive);
    }
}
