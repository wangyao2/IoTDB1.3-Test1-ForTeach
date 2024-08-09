package test1;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * ClassName:InsertTabletTest1
 * Description:
 *
 * @Create:2024/8/5 -9:50
 */
public class InsertTabletTest1 {
    /*
     * A Tablet example:
     *      device1
     * time s1, s2, s3
     * 1,   1,  1,  1
     * 2,   2,  2,  2
     * 3,   3,  3,  3
     */
    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        //一次性地，写入一个设备的多行数据
        Session session = new Session.Builder()
                .host("127.0.0.1")
                .port(6667)
                .username("root")
                .password("root")
                .build();
        session.open(false);

        List<MeasurementSchema> schemaList = new ArrayList<>();
        schemaList.add(new MeasurementSchema("s1", TSDataType.DOUBLE));
        schemaList.add(new MeasurementSchema("s2", TSDataType.DOUBLE));
        schemaList.add(new MeasurementSchema("s3", TSDataType.DOUBLE));
        Tablet tablet2 = new Tablet("root.bj.hd.d1", schemaList);

        Tablet tablet = new Tablet("root.bj.hd.d1", schemaList, 100);

        //
        long timestamp = System.currentTimeMillis();

        Random random = new Random();
        //1 循环 为每一行填充数据
        for (long row = 0; row < 10; row++) {
            //
            int rowIndex = tablet.rowSize++;
            tablet.addTimestamp(rowIndex, timestamp);
            for (int s = 0; s < 3; s++) {
                double value = random.nextInt(100);
                tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
            }

            timestamp-=1000;

//            if (tablet.rowSize == tablet.getMaxRowNumber()) {
//                session.insertTablet(tablet);
//                tablet.reset();
//            }
        }
        tablet.addTimestamp(0, timestamp);


        if (tablet.rowSize != 0) {

            session.insertTablet(tablet);
            tablet.reset();
        }
        session.insertTablet(tablet);

        //一次性插入三行数据
        //session.insertRecord();
        //session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
        List<String> measurements = new ArrayList<>();
        session.close();
    }

}
