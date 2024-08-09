package test2Query;

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
 * @Create:2024/8/9 -20:05
 */
public class QueryTest1 {


    public static void main(String[] args){

        Session session = new Session.Builder()
                .host("127.0.0.1")
                .port(6667)
                .username("root")
                .password("root")
                .build();
        try {
            session.open(false);
            session.setFetchSize(10000);

            SessionDataSet dataSet = session.executeQueryStatement("select * from root.bj.hd.d1");

            session.executeQueryStatement("select * from root.bj.hd.d1");
            System.out.println(dataSet.getColumnNames());

            dataSet.setFetchSize(1024); // default is 10000
            while (dataSet.hasNext()) {
                RowRecord oneRowData = dataSet.next();

                List<Field> fields = oneRowData.getFields();
                Field field = fields.get(0);
                double intV = fields.get(0).getDoubleV();
                System.out.println(intV);
                System.out.println(fields);
                System.out.println(oneRowData);
            }

        } catch (IoTDBConnectionException e) {
            throw new RuntimeException(e);
        } catch (StatementExecutionException e) {
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
