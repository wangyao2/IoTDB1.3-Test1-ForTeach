package test1;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

/**
 * ClassName:InsertTest2
 * Description: root.qd.laoshan.tbm1 speed ,tor ,dist
 * 2024年1月1日，2024年8月31日，244天
 *
 * @Create:2024/7/3 -10:25
 */
public class InsertTest2 {

    public static void main(String[] args) throws IoTDBConnectionException, ParseException, StatementExecutionException {
        Session session = new Session.Builder()
                .host("127.0.0.1")
                .port(6667)
                .username("root")
                .password("root")
                .build();
        session.open(false);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Random random = new Random();
        int alldays = 0;
        ArrayList<String> measue = new ArrayList<>();
        measue.add("speed");
        measue.add("tor");
        measue.add("dist");

        for (int month = 1; month <= 8; month++) {
            if (month == 2){
                alldays = 29;
            }else if (month ==1 || month == 3|| month ==5 || month ==7|| month ==8){
                alldays = 31;
            }else {
                alldays = 30;
            }

            for (int i = 1; i <= alldays; i++) {
                int hour = random.nextInt(24);
                int min = random.nextInt(60);
                int second = random.nextInt(60);
                String currenttime = "2024-"+month+"-"+i + " "+hour+":"+min+":"+second;
                Date data = simpleDateFormat.parse(currenttime);
                long time = data.getTime();
                ArrayList<String> values = new ArrayList<>();
                int speed = 10 + random.nextInt(51);
                values.add(Integer.toString(speed));
                int tor = 20 + random.nextInt(21);
                values.add(Integer.toString(tor));

                if ( month == 1|| month == 3|| month ==5){
                    int dist = random.nextInt(5);
                    values.add(Integer.toString(dist)); // 0-5
                } else if (month == 2|| month ==4|| month ==6) {
                    int dist = random.nextInt(10) + 5;
                    values.add(Integer.toString(dist)); //5-15
                }else {
                    int dist = random.nextInt(10) + 20;
                    values.add(Integer.toString(dist)); //20-30
                }

                session.insertRecord("root.qd.laoshan.tbm1",time,measue,values);
            }
        }
        session.close();
    }
}
