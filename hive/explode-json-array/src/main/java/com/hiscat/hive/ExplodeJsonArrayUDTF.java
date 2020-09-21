package com.hiscat.hive;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.json.JSONArray;

import static java.util.Collections.singletonList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

/**
 * @author hiscat
 */
public class ExplodeJsonArrayUDTF extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(StructObjectInspector ois) {
        if (ois.getAllStructFieldRefs().isEmpty()) {
            throw new IllegalArgumentException("arg is empty");
        }
        if (!"string".equals(ois.getAllStructFieldRefs().get(0).getFieldObjectInspector().getTypeName())) {
            throw new IllegalArgumentException("arg type is not string");
        }
        return getStandardStructObjectInspector(
                singletonList("items"),
                singletonList(javaStringObjectInspector)
        );
    }

    @Override
    public void process(Object[] args) throws HiveException {
        final String jsonStr = args[0].toString();
        if (StringUtils.isBlank(jsonStr)) {
            return;
        }
        JSONArray events = new JSONArray(jsonStr);
        for (int i = 0; i < events.length(); i++) {
            try {
                forward(singletonList(events.getJSONObject(i).toString()));
            } catch (Exception ignored) {
            }
        }

    }

    @Override
    public void close() throws HiveException {

    }
}
