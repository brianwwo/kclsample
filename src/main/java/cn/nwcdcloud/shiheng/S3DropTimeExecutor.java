package cn.nwcdcloud.shiheng;

import com.github.wnameless.json.flattener.JsonFlattener;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;

public class S3DropTimeExecutor {

    private static final long duration = 10 * 1000;

    private static final int initial = 10 * 1024;

    private final String bucket;

    private Timer timer;

    private Map<String, ByteBuf> bufferMap;

    private final S3Client s3;

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHH");

    public S3DropTimeExecutor(String region, String bucket) {
        this.s3 = S3Client.builder().region(Region.of(region)).build();
        this.bucket = bucket;
    }

    public void put(ByteBuffer input) {
        if (timer == null) {
            startTimer();
        }
        if (bufferMap == null) {
            bufferMap = new HashMap();
        }
        String strval = Charset.forName("utf-8").decode(input).toString();
        Map<String, Object> jsonMap = JsonFlattener.flattenAsMap(strval);
        BigDecimal orderTime = (BigDecimal) jsonMap.get("ordertime");
        BigDecimal v = orderTime.multiply(BigDecimal.valueOf(1000));
        String key = "scenario2/raw/ordertime="+dateFormat.format(new Date(v.longValue()));
        if (!bufferMap.containsKey(key)) {
            bufferMap.put(key, Unpooled.buffer(initial));
        }
        String flattenVal = JsonFlattener.flatten(strval);
        bufferMap.get(key).writeCharSequence(flattenVal+"\n", Charset.forName("utf-8"));

    }

    void startTimer() {
        timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                if (bufferMap != null) {
                    synchronized (bufferMap) {
                        for (Map.Entry<String, ByteBuf> entry : bufferMap.entrySet()) {
                            try {
                                s3.putObject(PutObjectRequest.builder().bucket(bucket).key(entry.getKey() + "/" + UUID.randomUUID() + ".txt").build(), RequestBody.fromByteBuffer(entry.getValue().nioBuffer()));
                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                        }
                        bufferMap = null;
                    }
                }
            }
        }, duration, duration);
    }
}
