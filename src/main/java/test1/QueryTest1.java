package test1;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.util.List;

/**
 * ClassName:QueryTest1
 * Description:
 *
 * @Create:2024/8/11 -17:59
 */
public class QueryTest1 {
    public static void main(String[] args) {
        Session session = new Session.Builder()
                .host("127.0.0.1")
                .port(6667)
                .username("root")
                .password("root")
                .build();
        try {
            session.open(false);

            SessionDataSet sessionDataSet = session.executeQueryStatement("select * from root.bj.hd.d1");
            // 获取列名，并输出
            List<String> columnNames = sessionDataSet.getColumnNames();
            System.out.println(columnNames);

            // 不断地循环，遍历每一行的内容
            while (sessionDataSet.hasNext()){
                RowRecord oneRowData = sessionDataSet.next();

                long timestamp = oneRowData.getTimestamp();
                List<Field> fields = oneRowData.getFields();
                Field field = fields.get(0);
                field.getDoubleV();
                field.getIntV();


                System.out.println(oneRowData);
            }

        } catch (IoTDBConnectionException e) {
            System.out.println("链接发生异常！");
            throw new RuntimeException(e);
        } catch (StatementExecutionException e) {
            System.out.println("查询时发生异常！");
            throw new RuntimeException(e);
        } finally {
            try {
                session.close();
            } catch (IoTDBConnectionException e) {
                throw new RuntimeException(e);
            }
        }


    }
}
