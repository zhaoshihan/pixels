package io.pixelsdb.pixels.worker.vhive;

import com.alibaba.fastjson.JSON;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.turbo.HelloInput;
import io.pixelsdb.pixels.common.turbo.HelloOutput;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.planner.plan.physical.input.*;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.turbo.TurboProto;
import io.pixelsdb.pixels.turbo.vHiveWorkerServiceGrpc;
import io.pixelsdb.pixels.worker.vhive.utils.ServiceImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;

public class WorkerServiceImpl extends vHiveWorkerServiceGrpc.vHiveWorkerServiceImplBase
{
    private static final String SERVICE_ID = String.valueOf(UUID.randomUUID());

    private static final Logger log = LogManager.getLogger(WorkerServiceImpl.class);
    public WorkerServiceImpl()
    {
    }

    @Override
    public void process(TurboProto.WorkerRequest request, StreamObserver<TurboProto.WorkerResponse> responseObserver)
    {
        WorkerType workerType = WorkerType.from(request.getWorkerType());
        switch (workerType)
        {
            case AGGREGATION:
            {
                ServiceImpl<AggregationWorker, AggregationInput, AggregationOutput> service = new ServiceImpl<>(AggregationWorker.class, AggregationInput.class);
                service.execute(request, responseObserver);
                break;
            }
            case BROADCAST_CHAIN_JOIN:
            {
                ServiceImpl<BroadcastChainJoinWorker, BroadcastChainJoinInput, JoinOutput> service = new ServiceImpl<>(BroadcastChainJoinWorker.class, BroadcastChainJoinInput.class);
                service.execute(request, responseObserver);
                break;
            }
            case BROADCAST_JOIN:
            {
                ServiceImpl<BroadcastJoinWorker, BroadcastJoinInput, JoinOutput> service = new ServiceImpl<>(BroadcastJoinWorker.class, BroadcastJoinInput.class);
                service.execute(request, responseObserver);
                break;
            }
            case PARTITIONED_CHAIN_JOIN:
            {
                ServiceImpl<PartitionedChainJoinWorker, PartitionedChainJoinInput, JoinOutput> service = new ServiceImpl<>(PartitionedChainJoinWorker.class, PartitionedChainJoinInput.class);
                service.execute(request, responseObserver);
                break;
            }
            case PARTITIONED_JOIN:
            {
                ServiceImpl<PartitionedJoinWorker, PartitionedJoinInput, JoinOutput> service = new ServiceImpl<>(PartitionedJoinWorker.class, PartitionedJoinInput.class);
                service.execute(request, responseObserver);
                break;
            }
            case PARTITION:
            {
                ServiceImpl<PartitionWorker, PartitionInput, PartitionOutput> service = new ServiceImpl<>(PartitionWorker.class, PartitionInput.class);
                service.execute(request, responseObserver);
                break;
            }
            case SCAN:
            {
                ServiceImpl<ScanWorker, ScanInput, ScanOutput> service = new ServiceImpl<>(ScanWorker.class, ScanInput.class);
                service.execute(request, responseObserver);
                break;
            }
            case HELLO: {
                log.info(String.format("get input successfully: %s", request.getJson()));
                HelloInput input = JSON.parseObject(request.getJson(), HelloInput.class);

                log.info("try to sleep");
                try {
                    Thread.sleep(60 * 1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                log.info("wake up");
                HelloOutput output = new HelloOutput();
                output.setContent(String.format("%s,%s", "hello", input.getContent()));
                output.setRequestId(SERVICE_ID);
                TurboProto.WorkerResponse response = TurboProto.WorkerResponse.newBuilder()
                        .setJson(JSON.toJSONString(output))
                        .build();
                log.info(String.format("get output successfully: %s", response.getJson()));
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                break;
            }
            default:
                throw new RuntimeException("Receive invalid function type");
        }
    }

    @Override
    public void getMemory(TurboProto.GetMemoryRequest request, StreamObserver<TurboProto.GetMemoryResponse> responseObserver)
    {
        // return the MB(1024 * 1024) size
        int dataSize = 1024 * 1024;
        TurboProto.GetMemoryResponse response = TurboProto.GetMemoryResponse.newBuilder()
                .setMemoryMB(Runtime.getRuntime().totalMemory() / dataSize)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
