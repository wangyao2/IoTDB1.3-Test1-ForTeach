package test1;

import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.HashMap;

/**
 * ClassName:ConnectIoTDB
 * Description:
 *
 * @Create:2024/5/23 -18:49
 */
public class ConnectIoTDB {
    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session.Builder()
                .host("127.0.0.1")
                .port(6667)
                .username("root")
                .password("root")
                .build();
        session.open(false);

        // set session fetchSize
        session.setFetchSize(10000);
        //session.createDatabase("root.qd");
        // create timeseries root.qd.laoshan.tbm1.speed double
        // create timeseries root.qd.laoshan.tbm1.speed with  double encodng = palin compreesor = uncompressor
//        HashMap<String, String> tasg1 = new HashMap<>();
//        tasg1.put("k1","vv1");
//        HashMap<String, String> attrt1 = new HashMap<>();
//        attrt1.put("att1","aaa1");
//        session.createTimeseries("root.qd.laoshan.tbm2.speed",TSDataType.DOUBLE,TSEncoding.PLAIN
//        ,CompressionType.UNCOMPRESSED,null,tasg1,attrt1,"sudu");
        session.close();
    }

}
