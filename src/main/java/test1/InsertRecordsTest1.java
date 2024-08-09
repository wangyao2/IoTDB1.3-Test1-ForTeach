package test1;


import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * ClassName:InsertRecordsTest1
 * Description:
 *
 * @Create:2024/8/5 -9:50
 */
public class InsertRecordsTest1 {
    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException, InterruptedException {
        Session session = new Session.Builder()
                .host("127.0.0.1")
                .port(6667)
                .username("root")
                .password("root")
                .build();
        session.open(false);

        ArrayList<String> deviceIDs = new ArrayList<>();
        ArrayList<Long> times = new ArrayList<>();
        List<List<String>> measurements = new ArrayList<>();
        List<List<String>> valuesList = new ArrayList<>();

        //session.insertRecord();
        /*
        一次写入多行数据，每一行都要填写设备名，时间戳，列名，对应的值
        填写50行，奇数行填写到设备1，偶数行填写到设备2
        每个设备有3个序列分别是，s1,s2,s3
         */
        ArrayList<String> measure = new ArrayList<>();
        measure.add("s1");
        measure.add("s2");
        measure.add("s3");

        for (int i = 0; i < 50; i++) {//循环50次对应50行数据
            //1 为每一行指定设备名称
            if (i%2 ==0){
                deviceIDs.add("root.bj.haidian.d2");
            }else {
                deviceIDs.add("root.bj.haidian.d1");
            }
            //2 为每一行指定时间戳
            Thread.sleep(30);
            times.add(System.currentTimeMillis());//每一行插入数据的时间戳，都是当前系统的时间戳

            //3 为每一行指定传感器测点
            measurements.add(measure);//插入的每一行都有3个序列s1~s3，一共50行

            //4 为每一行指定插入的数据值，替换为自己的数据来源
            ArrayList<String> Values = new ArrayList<>();//为一行数据填充数值
            Random random = new Random();
            for (int ii = 0; ii < 3; ii++) {
                int randomNumber = random.nextInt(31) + 20; // Generates a number between 20 and 50
                Values.add(String.valueOf(randomNumber));
            }
            valuesList.add(Values);
        }
        session.insertRecords(deviceIDs,times,measurements,valuesList);
        session.close();

    }
}
