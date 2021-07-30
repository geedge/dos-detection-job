package com.zdjizhi.etl;

import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.common.DosSketchLog;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;

import static com.zdjizhi.sink.OutputStreamSink.outputTag;

/**
 * @author 94976
 */
public class EtlProcessFunction extends ProcessWindowFunction<DosSketchLog, DosSketchLog, Tuple4<String,String,String,String>, TimeWindow> {

    private static final Logger logger = LoggerFactory.getLogger(EtlProcessFunction.class);
    @Override
    public void process(Tuple4<String,String, String, String> keys,
                        Context context, Iterable<DosSketchLog> elements,
                        Collector<DosSketchLog> out) {
        DosSketchLog middleResult = getMiddleResult(keys, elements);
        try {
            if (middleResult != null){
                out.collect(middleResult);
                logger.info("获取中间聚合结果：{}",middleResult.toString());
                context.output(outputTag,TrafficServerIpMetrics.getOutputMetric(keys, middleResult));
            }
        }catch (Exception e){
            logger.error("获取中间聚合结果失败,middleResult: {}\n{}",middleResult.toString(),e);
        }
    }

    private DosSketchLog getMiddleResult(Tuple4<String,String, String, String> keys,Iterable<DosSketchLog> elements){

        DosSketchLog midResuleLog = new DosSketchLog();
        Tuple6<Long, Long, Long,String,Long,Long> values = sketchAggregate(elements);
        try {
            if (values != null){
                midResuleLog.setCommon_sled_ip(keys.f0);
                midResuleLog.setCommon_data_center(keys.f1);
                midResuleLog.setDestination_ip(keys.f3);
                midResuleLog.setAttack_type(keys.f2);
                midResuleLog.setSketch_start_time(values.f4);
                midResuleLog.setSketch_duration(values.f5);
                midResuleLog.setSource_ip(values.f3);
                midResuleLog.setSketch_sessions(values.f0);
                midResuleLog.setSketch_packets(values.f1);
                midResuleLog.setSketch_bytes(values.f2);
                return midResuleLog;
            }
        } catch (Exception e){
            logger.error("加载中间结果集失败,keys: {} values: {}\n{}",keys,values,e);
        }
        return null;
    }

    private Tuple6<Long, Long, Long,String,Long,Long> sketchAggregate(Iterable<DosSketchLog> elements){
        int cnt = 1;
        long sessions = 0;
        long packets = 0 ;
        long bytes = 0;
        long startTime = 0;
        long duration = 0;
        HashSet<String> sourceIpSet = new HashSet<>();
        try {
            for (DosSketchLog newSketchLog : elements){
                sessions += newSketchLog.getSketch_sessions();
                packets += newSketchLog.getSketch_packets();
                bytes += newSketchLog.getSketch_bytes();
                startTime = newSketchLog.getSketch_start_time();
                duration = newSketchLog.getSketch_duration();
                cnt += 1;
                if (sourceIpSet.size() < CommonConfig.SOURCE_IP_LIST_LIMIT){
                    sourceIpSet.add(newSketchLog.getSource_ip());
                }
            }
            String sourceIpList = StringUtils.join(sourceIpSet, ",");
            return Tuple6.of(sessions/cnt,packets/cnt,bytes/cnt,sourceIpList,startTime,duration);
        }catch (Exception e){
            logger.error("聚合中间结果集失败 {}",e);
        }
        return null;
    }

}
