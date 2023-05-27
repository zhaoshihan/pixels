package io.pixelsdb.pixels.worker.vhive;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.planner.plan.physical.input.*;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import io.pixelsdb.pixels.worker.common.WorkerProto;
import io.pixelsdb.pixels.worker.common.WorkerServiceGrpc;
import io.pixelsdb.pixels.worker.vhive.utils.ServiceImpl;
import org.slf4j.LoggerFactory;

public class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {
    public WorkerServiceImpl() {
    }

    @Override
    public void aggregation(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        AggregationWorker aggregationWorker = new AggregationWorker(
                new WorkerContext(
                        LoggerFactory.getLogger(AggregationWorker.class),
                        new WorkerMetrics(),
                        request.getRequestID()
                )
        );

        ServiceImpl<AggregationInput, AggregationOutput> service = new ServiceImpl<>(aggregationWorker, AggregationInput.class);
        service.execute(request, responseObserver);
    }

    @Override
    public void broadcastChainJoin(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        BroadcastChainJoinWorker broadcastChainJoinWorker = new BroadcastChainJoinWorker(
                new WorkerContext(
                        LoggerFactory.getLogger(BroadcastChainJoinWorker.class),
                        new WorkerMetrics(),
                        request.getRequestID()
                )
        );

        ServiceImpl<BroadcastChainJoinInput, JoinOutput> service = new ServiceImpl<>(broadcastChainJoinWorker, BroadcastChainJoinInput.class);
        service.execute(request, responseObserver);
    }

    @Override
    public void broadcastJoin(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        BroadcastJoinWorker broadcastJoinWorker = new BroadcastJoinWorker(
                new WorkerContext(
                        LoggerFactory.getLogger(BroadcastJoinWorker.class),
                        new WorkerMetrics(),
                        request.getRequestID()
                )
        );

        ServiceImpl<BroadcastJoinInput, JoinOutput> service = new ServiceImpl<>(broadcastJoinWorker, BroadcastJoinInput.class);
        service.execute(request, responseObserver);
    }

    @Override
    public void partitionChainJoin(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        PartitionedChainJoinWorker partitionedChainJoinWorker = new PartitionedChainJoinWorker(
                new WorkerContext(
                        LoggerFactory.getLogger(PartitionedChainJoinWorker.class),
                        new WorkerMetrics(),
                        request.getRequestID()
                )
        );

        ServiceImpl<PartitionedChainJoinInput, JoinOutput> service = new ServiceImpl<>(partitionedChainJoinWorker, PartitionedChainJoinInput.class);
        service.execute(request, responseObserver);
    }

    @Override
    public void partitionJoin(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        PartitionedJoinWorker partitionedJoinWorker = new PartitionedJoinWorker(
                new WorkerContext(
                        LoggerFactory.getLogger(PartitionedJoinWorker.class),
                        new WorkerMetrics(),
                        request.getRequestID()
                )
        );

        ServiceImpl<PartitionedJoinInput, JoinOutput> service = new ServiceImpl<>(partitionedJoinWorker, PartitionedJoinInput.class);
        service.execute(request, responseObserver);
    }

    @Override
    public void partition(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        PartitionWorker partitionWorker = new PartitionWorker(
                new WorkerContext(
                        LoggerFactory.getLogger(PartitionWorker.class),
                        new WorkerMetrics(),
                        request.getRequestID()
                )
        );

        ServiceImpl<PartitionInput, PartitionOutput> service = new ServiceImpl<>(partitionWorker, PartitionInput.class);
        service.execute(request, responseObserver);
    }


    @Override
    public void scan(WorkerProto.WorkerRequest request, StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        ScanWorker scanWorker = new ScanWorker(
                new WorkerContext(
                        LoggerFactory.getLogger(ScanWorker.class),
                        new WorkerMetrics(),
                        request.getRequestID()
                )
        );

        ServiceImpl<ScanInput, ScanOutput> service = new ServiceImpl<>(scanWorker, ScanInput.class);
        service.execute(request, responseObserver);
    }

    @Override
    public void hello(WorkerProto.HelloRequest request, StreamObserver<WorkerProto.HelloResponse> responseObserver) {
        String output = "Hello, " + request.getName();

        WorkerProto.HelloResponse response = WorkerProto.HelloResponse.newBuilder().setOutput(output).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


    @Override
    public void getMemory(WorkerProto.GetMemoryRequest request, StreamObserver<WorkerProto.GetMemoryResponse> responseObserver) {
        // return the MB(1024 * 1024) size
        int dataSize = 1024 * 1024;
        WorkerProto.GetMemoryResponse response = WorkerProto.GetMemoryResponse.newBuilder()
                .setMemoryMB(Runtime.getRuntime().totalMemory() / dataSize)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
