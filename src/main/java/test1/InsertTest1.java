package test1;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

/**
 * ClassName:InsertTest1
 * Description:
 *
 * @Create:2024/5/25 -14:03
 */
public class InsertTest1 {
    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session.Builder()
                .host("127.0.0.1")
                .port(6667)
                .username("root")
                .password("root")
                .build();
        session.open(false);
        // insert into root.qd.laoshan.tbm (time, speed) values (now(),25.5)
        long currentTimeMillis = System.currentTimeMillis();
        ArrayList<String> meause = new ArrayList<>();
        meause.add("speed");
        meause.add("tor");
        ArrayList<String> vlause = new ArrayList<>();
        vlause.add("25.5");
        vlause.add("68.7");

        session.insertRecord("root.qd.laoshan.tbm1",currentTimeMillis,meause,vlause);

        String dateTimeString = "2024-05-25 12:00:00";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dateTime = LocalDateTime.parse(dateTimeString, formatter);
        // 获取时间戳，单位是毫秒
        long timestampMillis = dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        session.insertRecord("root.qd.laoshan.tbm1",timestampMillis,meause,vlause);
        session.close();
    }
}
