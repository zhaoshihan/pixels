package io.pixelsdb.pixels.invoker.vhive;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.*;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import org.apache.commons.cli.*;

import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class Main
{
    // here are the default values if not specified in args
    private static final String HOST = "localhost";
    private static final int PORT = 50051;
    private static final String FUNC = "Hello";
    private static final int NUMBER = 1;

    public static void main(String[] args) throws InterruptedException
    {
        Options options = new Options();
        options.addOption(Option.builder("h")
                .longOpt("host")
                .hasArg(true)
                .desc("GRPC host (or use --host)")
                .required(false)
                .build());
        options.addOption(Option.builder("p")
                .longOpt("port")
                .hasArg(true)
                .desc("GRPC port (or use --port)")
                .required(false)
                .build());
        options.addOption(Option.builder("f")
                .longOpt("function")
                .hasArg(true)
                .desc("GRPC function (or use --function)")
                .required(false)
                .build());
        options.addOption(Option.builder("n")
                .longOpt("number")
                .hasArg(true)
                .desc("GRPC same request number (or use --number)")
                .required(false)
                .build());

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try
        {
            cmd = parser.parse(options, args);

            String host = cmd.getOptionValue("host", HOST);
            String port = cmd.getOptionValue("port", Integer.toString(PORT));
            String function = cmd.getOptionValue("function", FUNC);
            String number = cmd.getOptionValue("number", Integer.toString(NUMBER));

            StorageInfo storageInfo = new StorageInfo(Storage.Scheme.minio, null, null, null);
            InvokerFactory factory = InvokerFactory.Instance();

            CountDownLatch countDownLatch = new CountDownLatch(Integer.parseInt(number));
            List<Long> invokeTimes = new ArrayList<>();
            List<Long> rtts = new ArrayList<>();
//            WorkerAsyncClient client = new WorkerAsyncClient(host, Integer.parseInt(port));

            for (int i = 0; i < Integer.parseInt(number); ++i)
            {
                final CompletableFuture<Output> completableFuture;
                final Input input;
                long startTime = System.nanoTime();
                switch (function)
                {
                    case "Aggregation":
                        input = Utils.genAggregationInput(storageInfo);
                        completableFuture = factory.getInvoker(WorkerType.AGGREGATION).invoke(input);
                        break;
                    case "BroadcastChainJoin":
                        input = Utils.genBroadcastChainJoinInput(storageInfo);
                        completableFuture = factory.getInvoker(WorkerType.BROADCAST_CHAIN_JOIN).invoke(input);
                        break;
                    case "BroadcastJoin":
                        input = Utils.genBroadcastJoinInput(storageInfo);
                        completableFuture = factory.getInvoker(WorkerType.BROADCAST_JOIN).invoke(input);
                        break;
                    case "PartitionChainJoin":
                        input = Utils.genPartitionedChainJoinInput(storageInfo);
                        completableFuture = factory.getInvoker(WorkerType.PARTITIONED_CHAIN_JOIN).invoke(input);
                        break;
                    case "PartitionJoin":
                        input = Utils.genPartitionedJoinInput(storageInfo);
                        completableFuture = factory.getInvoker(WorkerType.PARTITIONED_JOIN).invoke(input);
                        break;
                    case "Partition":
                        assert Utils.genPartitionInput("order") != null;
                        input = Utils.genPartitionInput("order").apply(storageInfo, 0);
                        completableFuture = factory.getInvoker(WorkerType.PARTITION).invoke(input);
                        break;
                    case "Scan":
                        input = Utils.genScanInput(storageInfo, 0);
                        completableFuture = factory.getInvoker(WorkerType.SCAN).invoke(input);
                        break;
                    case "Hello":
                        input = new HelloInput(-1, String.valueOf(UUID.randomUUID()));
                        completableFuture = factory.getInvoker(WorkerType.HELLO).invoke(input);
                        break;
                    default:
                        throw new ParseException("invalid function name");
                }
                if (completableFuture != null)
                {
                    long invokeEnd = System.nanoTime();
                    Thread futureThread = new Thread(() -> {
                        try
                        {
                            Output output = completableFuture.get();
                            long endTime = System.nanoTime();
                            synchronized (System.out) {
                                System.out.println("Input: " + JSON.toJSONString(input));
                                System.out.println("Output: " + JSON.toJSONString(output));
                                System.out.println("Invoke time(MS): " + (invokeEnd - startTime) / 1000000);
                                System.out.println("Entire round trip time(MS): " + (endTime - startTime) / 1000000);
                                System.out.println();

                                invokeTimes.add((invokeEnd - startTime) / 1000000);
                                rtts.add((endTime - startTime) / 1000000);
                            }
                            countDownLatch.countDown();
                        } catch (InterruptedException e)
                        {
                            throw new RuntimeException(e);
                        } catch (ExecutionException e)
                        {
                            throw new RuntimeException(e);
                        }
                    });
                    futureThread.start();
                }
            }
            countDownLatch.await(200, TimeUnit.SECONDS);
            LongSummaryStatistics invokeStat = invokeTimes.stream().mapToLong((x) -> x).summaryStatistics();
            LongSummaryStatistics rttStat = rtts.stream().mapToLong((x) -> x).summaryStatistics();
            System.out.println("Invoke summary: " + invokeStat);
            System.out.println("RTT summary: " + rttStat);
        } catch (ParseException pe)
        {
            System.out.println("Error parsing command-line arguments!");
            System.out.println("Please, follow the instructions below:");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Log messages to sequence diagrams converter", options);
            System.exit(1);
        }
    }
}
