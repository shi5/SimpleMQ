package org.yahaha.mq.broker.store;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TestComsumerOffset {
    private String storePathConsumeOffset = "D:\\Temp" + File.separator + "store"
            + File.separator + "consumeoffset.json";

    private ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer, Long>> offsetTable
            = new ConcurrentHashMap<String, ConcurrentMap<Integer, Long>>();

    public void getSize() {
        System.out.println(offsetTable.size());
    }

    public void addOffset(String topic, String group, int queueId, long offset) {
        ConcurrentMap<Integer, Long> queueOffset = offsetTable.get(topic + "@" + group);
        if (queueOffset == null) {
            queueOffset = new ConcurrentHashMap<Integer, Long>();
            offsetTable.put(topic + "@" + group, queueOffset);
        }
        queueOffset.put(queueId, offset);
    }

    public void persist() {
        try {
            String json = JSON.toJSONString(offsetTable, SerializerFeature.PrettyFormat);
            try(FileOutputStream fileOutputStream = new FileOutputStream(storePathConsumeOffset)) {
                fileOutputStream.write(json.getBytes());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean load() {
        File file = new File(storePathConsumeOffset);
        if (!file.exists()) {
            return true;
        }
        try {
            byte[] data = new byte[(int) file.length()];
            boolean result;
            try(FileInputStream fileInputStream = new FileInputStream(file)) {
                int len = fileInputStream.read(data);
                result = len == data.length;
            }
            if (!result) {
                return false;
            }
            String json = new String(data);
            offsetTable = JSON.parseObject(json, new TypeReference<ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer, Long>>>(){});
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public static void main(String[] args) {
        TestComsumerOffset testComsumerOffset = new TestComsumerOffset();
        testComsumerOffset.load();
        testComsumerOffset.getSize();
//        testComsumerOffset.addOffset("topic1", "group2", 1, 0);
//        testComsumerOffset.addOffset("topic1", "group2", 2, 0);
        testComsumerOffset.persist();
    }

}
