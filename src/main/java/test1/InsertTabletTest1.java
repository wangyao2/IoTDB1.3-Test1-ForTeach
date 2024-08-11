package test1;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.Random;

/**
 * ClassName:InsertTabletTest1
 * Description:
 *
 * @Create:2024/8/5 -9:50
 */

/*
 * A Tablet example:
 *      device1
 * time s1,   s2,   s3
 * 1,   null, 1,    1
 * 2,   2,    null, 2
 * 3,   3,    3,    null
 */
public class InsertTabletTest1 {
    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {

        Session session = new Session.Builder()
                .host("127.0.0.1")
                .port(6667)
                .username("root")
                .password("root")
                .build();
        session.open(false);

        // 1 创建 tablet，定义表名称，定义序列名称
        ArrayList<MeasurementSchema> schemaList = new ArrayList<>();
        schemaList.add(new MeasurementSchema("s1", TSDataType.DOUBLE));
        schemaList.add(new MeasurementSchema("s2", TSDataType.DOUBLE));
        schemaList.add(new MeasurementSchema("s3", TSDataType.DOUBLE));
        Tablet tablet = new Tablet("root.bj.hd.d1",schemaList,100);

        Random random = new Random();
        //2 向tablet里面填入，一行一行的数据，包含时间戳，包含列的数值。记得更新行号
        long timeMillis = System.currentTimeMillis();
        for (int row = 0; row < 100; row++) {

            tablet.addTimestamp(row,timeMillis);
            timeMillis+=100;
            for (int s = 0; s < 3; s++) {
                double value = random.nextInt(50);
                tablet.addValue(schemaList.get(s).getMeasurementId(),row,value);

            }
            tablet.rowSize++;

        }
        if (tablet.rowSize == tablet.getMaxRowNumber()){
            session.insertTablet(tablet);
            tablet.reset();
        }

        session.close();
    }
}
