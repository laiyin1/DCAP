package iscas.GroupWindowUserPattern;

import iscas.Common.FileUtil;
import iscas.database.DataBaseHelper;
import iscas.util.DateUtil;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class WindowPartition {

    //windows窗口内部消息的时间间隔
    private int timegap =2;
    //
    private int minWindowNum =2;
    //
    private int minDatelistNum = 3;

        public static void main(String[] args) {
            int setid = 33;  //数据集id   
        int taskid = 65; //任务id
//        String name = "Hillary";
//        String name = "enron";
        String base = System.getProperty("user.dir") + "\\src\\main\\webapp\\data\\";

        WindowPartition sdt = new WindowPartition();
        sdt.script(setid,taskid,name,base);
    }

    public void script(int setid,int taskid,String name,String basepath)
    {
        /**
         * 按照格式获取数据
         * itemId,topicId,senderId,receiverId,datalist
         */

        //sql enron
        String strSql = " select t4.id node1Id,t5.id node2Id,date reltime,topic from t_topic_detail t1 ,message t2, recipientinfo t3, accountlist t4 , accountlist t5\n" +
                " where setid =33 and t1.yysp_id = t2.mid and t2.mid = t3.mid and topic!=-1\n" +
                " and t2.sender= t4.account and t3.rvalue = t5.account\n" +
                " order by node1Id,node2Id,topic,reltime ";
        String filename = "windowmessage.txt";

        // TODO Auto-generated method stub

        String thsj,ddfhm,dfhm;
        Date date = null;
        Date beforeDate = null;
        int topic;
        String strOut = "";
        String lastaccount="";

        String filePath;
        String fileName;
        String id;
        int num;
        int windowId=0;
        int windowCount=0;

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        filePath = basepath + "TopicDetail\\"+name+"\\";

        if (!new File(filePath).exists())
            FileUtil.mkdir(new File(filePath));

        int count=0;
        try {
            DataBaseHelper dbr = new DataBaseHelper();
//            DataBaseSqlite dbr = new DataBaseSqlite();
            dbr.prepareSQL(strSql);

            ResultSet rs =  dbr.pst.executeQuery();
            fileName = filePath + filename;
            File file = new File(fileName);

            strOut = "";
            num = 0;

            int accountid=-1;

            if (file.exists()) {
                file.delete();
            }

            file.createNewFile();

            FileWriter fileWriter = new FileWriter(fileName);
            BufferedWriter bufferWriter = new BufferedWriter(fileWriter);

            while (rs.next()) {
                thsj = rs.getString("reltime");
                ddfhm = rs.getString("node1Id");
                dfhm = rs.getString("node2Id");
                topic = rs.getInt("topic");

                if(thsj.length()<16)
                    continue;
                try {
                    date = format.parse(thsj);
                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    System.out.println(thsj);
                }

                if (accountid==9)
                {
                    System.out.println(9);
                }
                if(!lastaccount.equals(ddfhm+","+dfhm+","+topic))
                {
                    //限制Item中的消息数量
//                    count = 0;
//                    strOut = "";
//                    if(count>minDatelistNum)
//                    {
//                        bufferWriter.write(strOut);
//                        System.out.println("处理完" + num + "行数据");
//                    }
                    //限制窗口中的消息数量
                    if(windowCount>=minWindowNum)
                    {
                        bufferWriter.write(strOut);
                        System.out.println("处理完" + num + "行数据");
                    }
                    strOut = "";
                    windowId = 0;
                    count = 0;
                    windowCount=0;
                    accountid++;
                    lastaccount = ddfhm+","+dfhm+","+topic;
                }
                //判断窗口是否增加
                else
                {
                    if (beforeDate!=null
                    && beforeDate.before(DateUtil.diffHour(date,timegap)))
                    {
                        //限制窗口中的消息数量
                        if(windowCount>=minWindowNum)
                        {
                            bufferWriter.write(strOut);
                            windowId ++;
                            System.out.println("处理完" + num + "行数据");
                        }
                        strOut = "";
                        windowCount =0;
                    }
                }
                num++;
                count++;
                windowCount++;

//                thsj = thsj.replace("T"," ");
//                thsj = thsj.replace("+00:00","");
//                strOut += accountid+","+ddfhm + "," + dfhm  + "," + thsj +","+topic+"\r\n";

                strOut += accountid+","+windowId+","+ddfhm + "," + dfhm  + "," + thsj +","+topic+"\r\n";
                beforeDate =date;

//                if (count == 1000) {
//                    bufferWriter.write(strOut);
//                    count = 0;
//                    strOut = "";
//                    System.out.println("处理完" + num + "行数据");
//                }
            }
            bufferWriter.write(strOut);
            bufferWriter.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
