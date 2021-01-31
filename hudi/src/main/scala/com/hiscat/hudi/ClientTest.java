package com.hiscat.hudi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.HoodieJsonPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ClientTest {
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "hdp");
        Configuration conf = new Configuration();
        String basePath = "/student";
        String tableName = "student";
        initTable(conf, basePath, tableName);
        HoodieJavaWriteClient<HoodieJsonPayload> client = new HoodieJavaWriteClient<>(
                new HoodieJavaEngineContext(conf), getHoodieWriteConfig(basePath, tableName)
        );
//        upsert(client);

        client.close();

    }

    private static void upsert(HoodieJavaWriteClient<HoodieJsonPayload> client) throws IOException {
        String instantTime = client.startCommit();
        System.out.println(instantTime);
        String json = "{\n" +
                "  \"id\": \"1\",\n" +
                "  \"name\": \"hello\",\n" +
                "  \"ts\": \"100000000\"\n" +
                "}";
        List<HoodieRecord<HoodieJsonPayload>> records =
                Collections.singletonList(
                        new HoodieRecord<>(
                                new HoodieKey("1", "2021/01/30"),
                                new HoodieJsonPayload(json)
                        )
                );
        client.upsert(records, instantTime);
    }

    private static void initTable(Configuration conf, String basePath, String tableName) throws IOException {
        Path path = new Path(basePath);
        FileSystem fs = path.getFileSystem(conf);
        if (!fs.exists(path)) {
            HoodieTableMetaClient.initTableType(
                    conf, basePath, HoodieTableType.COPY_ON_WRITE, tableName,
                    HoodieJsonPayload.class.getName()
            );
        }
    }

    private static HoodieWriteConfig getHoodieWriteConfig(String basePath, String tableName) {
        String schemaStr = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"test\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"id\",\n" +
                "      \"type\": \"long\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"name\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"ts\",\n" +
                "      \"type\": \"long\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        return HoodieWriteConfig.newBuilder()
                .withPath(basePath)
                .forTable(tableName)
                .withSchema(schemaStr)
                .withDeleteParallelism(2)
                .withParallelism(2, 2)
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
                .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(20, 30).build()).build();
    }
}
