package io.pixelsdb.pixels.worker.vhive;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author hank
 * @date 02/08/2022
 */
public class MetricsCollector
{
    public static class Timer
    {
        private long elapsedNs = 0;
        private long startTime = 0L;

        public MetricsCollector.Timer start()
        {
            startTime = System.nanoTime();
            return this;
        }

        /**
         * @return the cumulative duration of this timer
         */
        public long stop()
        {
            long endTime = System.nanoTime();
            elapsedNs += endTime - startTime;
            return elapsedNs;
        }

        public void add(long timeNs)
        {
            this.elapsedNs += timeNs;
        }

        public void minus(long timeNs)
        {
            this.elapsedNs -= timeNs;
        }

        public long getElapsedNs()
        {
            return elapsedNs;
        }
    }

    private final AtomicInteger numReadRequests = new AtomicInteger(0);
    private final AtomicInteger numWriteRequests = new AtomicInteger(0);
    private final AtomicLong readBytes = new AtomicLong(0);
    private final AtomicLong writeBytes = new AtomicLong(0);
    private final AtomicLong inputCostNs = new AtomicLong(0);
    private final AtomicLong outputCostNs = new AtomicLong(0);
    private final AtomicLong computeCostNs = new AtomicLong(0);

    public void clear()
    {
        numReadRequests.set(0);
        numWriteRequests.set(0);
        readBytes.set(0);
        writeBytes.set(0);
        inputCostNs.set(0);
        outputCostNs.set(0);
        computeCostNs.set(0);
    }

    public int getNumReadRequests()
    {
        return numReadRequests.get();
    }

    public int getNumWriteRequests()
    {
        return numWriteRequests.get();
    }

    public long getReadBytes()
    {
        return readBytes.get();
    }

    public long getWriteBytes()
    {
        return writeBytes.get();
    }

    public long getInputCostNs()
    {
        return inputCostNs.get();
    }

    public long getOutputCostNs()
    {
        return outputCostNs.get();
    }

    public long getComputeCostNs()
    {
        return computeCostNs.get();
    }

    public void addNumReadRequests(int numReadRequests)
    {
        this.numReadRequests.addAndGet(numReadRequests);
    }

    public void addNumWriteRequests(int numWriteRequests)
    {
        this.numWriteRequests.addAndGet(numWriteRequests);
    }

    public void addReadBytes(long readBytes)
    {
        this.readBytes.addAndGet(readBytes);
    }

    public void addWriteBytes(long writeBytes)
    {
        this.writeBytes.addAndGet(writeBytes);
    }

    public void addInputCostNs(long inputDurationNs)
    {
        this.inputCostNs.addAndGet(inputDurationNs);
    }

    public void addOutputCostNs(long outputDurationNs)
    {
        this.outputCostNs.addAndGet(outputDurationNs);
    }

    public void addComputeCostNs(long computeDurationNs)
    {
        this.computeCostNs.addAndGet(computeDurationNs);
    }
}

