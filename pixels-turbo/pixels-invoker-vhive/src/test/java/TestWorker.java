import com.google.common.base.Joiner;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.invoker.vhive.Utils;
import io.pixelsdb.pixels.invoker.vhive.WorkerSyncClient;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestWorker {
    private static final String HOST = "localhost";
    private static final int PORT = 50051;
    private final WorkerSyncClient client = new WorkerSyncClient(HOST, PORT);

    @Test
    public void testHello() {
        String username = "zhaoshihan";

        assertEquals("Hello, " + username, client.hello(username));
    }

//    @Test
//    public void testAggregation() {
//        AggregationOutput output = client.aggregation(Utils.genAggregationInput());
//        System.out.println(output.getOutputs().size());
//        System.out.println(Joiner.on(",").join(output.getOutputs()));
//        System.out.println(Joiner.on(",").join(output.getRowGroupNums()));
//    }
//
//    @Test
//    public void testBroadcastChainJoin() {
//        JoinOutput output = client.broadcastChainJoin(Utils.genBroadcastChainJoinInput());
//        System.out.println(output.getOutputs().size());
//        System.out.println(Joiner.on(",").join(output.getOutputs()));
//        System.out.println(Joiner.on(",").join(output.getRowGroupNums()));
//    }
//
//    @Test
//    public void testBroadcastJoin() {
//        JoinOutput output = client.broadcastJoin(Utils.genBroadcastJoinInput());
//        System.out.println(output.getOutputs().size());
//        System.out.println(Joiner.on(",").join(output.getOutputs()));
//        System.out.println(Joiner.on(",").join(output.getRowGroupNums()));
//    }
//
//    @Test
//    public void testPartitionChainJoin() {
//        JoinOutput output = client.partitionChainJoin(Utils.genPartitionedChainJoinInput());
//        System.out.println(output.getOutputs().size());
//        System.out.println(Joiner.on(",").join(output.getOutputs()));
//        System.out.println(Joiner.on(",").join(output.getRowGroupNums()));
//    }
//
//    @Test
//    public void testPartitionJoin() {
//        JoinOutput output = client.partitionJoin(Utils.genPartitionedJoinInput());
//        System.out.println(output.getOutputs().size());
//        System.out.println(Joiner.on(",").join(output.getOutputs()));
//        System.out.println(Joiner.on(",").join(output.getRowGroupNums()));
//    }
//
//    @Test
//    public void testPartition() {
//        PartitionInput input = Utils.genPartitionInput("order").apply(0);
//        System.out.println(JSON.toJSONString(input));
//        PartitionOutput orderOutput = client.partition(input);
//        System.out.println(orderOutput.getPath());
//        System.out.println(Joiner.on(",").join(orderOutput.getHashValues()));
//
////        PartitionOutput lineitemOutput = client.partition(io.pixelsdb.pixels.invoker.vhive.Utils.genPartitionInput("lineitem"));
////        System.out.println(lineitemOutput.getPath());
////        System.out.println(Joiner.on(",").join(lineitemOutput.getHashValues()));
//    }

    @Test
    public void testScan() {
        ScanOutput output = client.scan(Utils.genScanInput(Storage.Scheme.minio, 0));
        System.out.println(output.getOutputs().size());
        System.out.println(Joiner.on(",").join(output.getOutputs()));
        System.out.println(Joiner.on(",").join(output.getRowGroupNums()));
    }
}
