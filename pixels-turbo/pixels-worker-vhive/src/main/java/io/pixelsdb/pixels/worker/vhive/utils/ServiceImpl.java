package io.pixelsdb.pixels.worker.vhive.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import io.pixelsdb.pixels.worker.common.WorkerProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;

public class ServiceImpl<T extends RequestHandler<I, O>, I extends Input, O extends Output> {
    private static final Logger log = LogManager.getLogger(ServiceImpl.class);

    final Class<T> handlerClass;
    final Class<I> typeParameterClass;

    public ServiceImpl(
            Class<T> handlerClass,
            Class<I> typeParameterClass) {
        this.handlerClass = handlerClass;
        this.typeParameterClass = typeParameterClass;
    }

    public void execute(WorkerProto.WorkerRequest request,
                        StreamObserver<WorkerProto.WorkerResponse> responseObserver) {
        I input = JSON.parseObject(request.getJson(), typeParameterClass);
        O output;

        boolean isProfile = Boolean.parseBoolean(System.getenv("PROFILING_ENABLED"));
        try {
            String requestId = String.format("%d_%s", input.getQueryId(), UUID.randomUUID());
            WorkerContext context = new WorkerContext(LogManager.getLogger(handlerClass), new WorkerMetrics(), requestId);
            RequestHandler<I, O> handler = handlerClass.getConstructor(WorkerContext.class).newInstance(context);

            String JSONFilename = String.format("%s.json", handler.getRequestId());
            if (isProfile) {
                log.info(String.format("enable profile to execute input: %s", JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect)));

                String JFRFilename = String.format("%s.jfr", handler.getRequestId());
                String event = System.getenv("PROFILING_EVENT");

                Utils.startProfile(event, JFRFilename);
                output = handler.handleRequest(input);
                Utils.stopProfile(JFRFilename);

                Utils.upload(JFRFilename, "experiments/" + JFRFilename);
            } else {
                log.info(String.format("disable profile to execute input: %s", JSON.toJSONString(input, SerializerFeature.DisableCircularReferenceDetect)));
                output = handler.handleRequest(input);
            }
            Utils.dump(JSONFilename, JSON.toJSONString(input, SerializerFeature.PrettyFormat, SerializerFeature.DisableCircularReferenceDetect));
            Utils.upload(JSONFilename, "experiments/" + JSONFilename);

            log.info(String.format("get output successfully: %s", JSON.toJSONString(output)));
        } catch (Exception e) {
            throw new RuntimeException("Exception during process: ", e);
        }
        WorkerProto.WorkerResponse response = WorkerProto.WorkerResponse.newBuilder()
                .setJson(JSON.toJSONString(output))
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


}
