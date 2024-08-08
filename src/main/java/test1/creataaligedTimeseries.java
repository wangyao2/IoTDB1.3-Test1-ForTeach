package test1;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.ArrayList;

/**
 * ClassName:creataaligedTimeseries
 * Description:
 *
 * @Create:2024/5/25 -10:51
 */
public class creataaligedTimeseries {
    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session.Builder()
                .host("127.0.0.1")
                .port(6667)
                .username("root")
                .password("root")
                .build();
        session.open(false);

        // create aligned timeseries root.qd.laoshan.tbm3 (speed doule ecnoding = plain compre = gzip, tor doule ,dist doule)
//        ArrayList<String> meaus = new ArrayList<>();
//        meaus.add("speed");
//        meaus.add("tor");
//        meaus.add("dist");
//
//        ArrayList<TSDataType> dataTypes = new ArrayList<>();
//        dataTypes.add(TSDataType.DOUBLE);
//        dataTypes.add(TSDataType.DOUBLE);
//        dataTypes.add(TSDataType.DOUBLE);
//
//        ArrayList<TSEncoding> encodings = new ArrayList<>();
//        encodings.add(TSEncoding.PLAIN);
//        encodings.add(TSEncoding.PLAIN);
//        encodings.add(TSEncoding.PLAIN);
//        ArrayList<CompressionType> compressionTypes = new ArrayList<>();
//        compressionTypes.add(CompressionType.UNCOMPRESSED);
//        compressionTypes.add(CompressionType.GZIP);
//        compressionTypes.add(CompressionType.LZ4);
//
//        session.createAlignedTimeseries("root.qd.laoshan.tbm3",meaus,dataTypes,encodings,compressionTypes,null);

        ArrayList<String> measue = new ArrayList<>();
        measue.add("root.qd.laoshan.tbm4.speed");
        measue.add("root.qd.laoshan.tbm4.tor");
        measue.add("root.qd.laoshan.tbm4.dist");
        ArrayList<TSDataType> dataTypes2 = new ArrayList<>();
        dataTypes2.add(TSDataType.DOUBLE);
        dataTypes2.add(TSDataType.DOUBLE);
        dataTypes2.add(TSDataType.DOUBLE);
        ArrayList<TSEncoding> encoding2 = new ArrayList<>();
        encoding2.add(TSEncoding.PLAIN);
        encoding2.add(TSEncoding.PLAIN);
        encoding2.add(TSEncoding.PLAIN);
        ArrayList<CompressionType> compressionTypes2 = new ArrayList<>();
        compressionTypes2.add(CompressionType.UNCOMPRESSED);
        compressionTypes2.add(CompressionType.GZIP);
        compressionTypes2.add(CompressionType.LZ4);
        session.createMultiTimeseries(measue,dataTypes2,encoding2,compressionTypes2,null,null,null,null);
        session.close();

    }
}
