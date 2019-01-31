package iscas.GroupWindowUserPattern;

import iscas.util.DateUtil;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * YANG YI
 * 2018.09.19
 */
public class SupportCalculation {

    //路径
    private String base ;
    //案例名称
    private String name ;
    //输入文件
    private String inputfile;
    //输出文件
    private String outputpath ;
    //windows窗口内部消息的时间间隔
    private int timegap =1;
    //前后item的时间差
    private int windowgap=1;
    //主题数量
    private int topicNum ;
    //
    private int minWindowNum = 3;
    //输出实例的top
    private int topNumber = 10;

    //模式的最大长度
    private int patternMaxLength ;
    //记录所有pattern的数组，list中存储每个长度的pattern集合
    //HashMap中存储所有相同长度的pattern
    private List<HashMap<String,UWindowPattern>> patternList = new ArrayList<>();

    //按照主题存储不同的PairItem集合
    //主题(list)->senderid(map)->itemid(map)
    private List<HashMap<Integer,List<Integer>>> pairItemMapList = new ArrayList<>(topicNum);
    //itemid(map)->item(map)
    private HashMap<Integer,UPairItem> pairItemHashMap = new HashMap<>();


    public static void main(String[] args) {

        String base = System.getProperty("user.dir") + "/src/main/webapp/data/";

        int timegap =2;
        //前后item的时间差
        int windowgap=1;


        String name = "Hillary";
        int topicNum =30;
        int patternMaxLength=3;

//        String name = "enron";
//        int topicNum =30;
//        int patternMaxLength=4;

//        String name = "Paper2";
//        int topicNum =20;
//        int patternMaxLength=2;

//        SupportCalculation gup = new SupportCalculation();
//        gup.script(name,base,topicNum,patternMaxLength,timegap,windowgap);

        long startTime=System.currentTimeMillis();   //获取开始时间
        SupportCalculation gup = new SupportCalculation();
        gup.script(name,base,topicNum,patternMaxLength,timegap,windowgap);
        long endTime=System.currentTimeMillis(); //获取结束时间
        System.out.println("程序运行时间： "+(endTime-startTime)+"s");
    }

    public void script(String name,String base, int topicNum, int patternMaxLength
            ,int timegap,int windowgap)  {

        InitParam(name,base,topicNum,patternMaxLength,timegap,windowgap);        // 初始化参数
//        DataLoader(); 		// 将所有数据加载进PairItem集合中
        DataLoaderWindow();   // 将所有数据加载进PairItem集合中
//        GapCount gc = new GapCount();
//        gc.WindowsCount(pairItemHashMap);
//        gc.TimeGapCount(pairItemHashMap);    //统计时间间隔
        System.out.println("begin to InitWindowPattern");
        InitWindowPattern();      //初始化长度为1的pattern
        System.out.println("begin to ComputSeqPattern");
        for (int i = 1; i <= 1; i++) {
            windowgap = 13;
            InitParam(name,base,topicNum,patternMaxLength,timegap,windowgap);        // 初始化参数
            ComputeBreadthSeqPattern();  // 开始广度优先循环计算所有长度的模式
//            System.out.println("begin to OutToExcel");
            System.out.println(timegap+","+windowgap);
//            OutToExcel();
//          OutToFile();        //讲结果生成文件
        }
    }

    private void DataLoaderWindow() {

        int line = 0;
        String inputLine = null;
        String time;
        Date date = null;
        Date beforeDate = null;
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        int pairItemId = 0,topicId,senderId,receiverId;
        int windowId = 0;
        int beforWindowId = 0;
        int lastItemId = 0;
        int accountid=-1;

        FileReader frInput = null;
        UPairItem uPairItem = null;
        UPairItem lastPairItem = null;

        UDateWindow uDateWindow = null;
        List<Date> windowDateList = null;

        int count = 0;
        try {
            frInput = new FileReader(inputfile);
            BufferedReader brInput = new BufferedReader(frInput);

            while ((inputLine = brInput.readLine()) != null) {
                line++;

                String[] attr = inputLine.split(",");
                if(attr.length<5)
                    continue;

                pairItemId = Integer.parseInt(attr[0]);		   //itemID
                windowId = Integer.parseInt(attr[1]);          //窗口ID
                senderId = Integer.parseInt(attr[2]);          //消息发送方
                receiverId = Integer.parseInt(attr[3]);		   //消息接收方
                time = attr[4];				                   //发送时间
                topicId = Integer.parseInt(attr[5]);		   //发送内容主题

                if(time.length()<16)
                    continue;
                try {
                    beforeDate = date;
                    date = format.parse(time);
                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    System.out.println(time);
                }
                count++;
                if (count%100000==0&&count>0)
                    System.out.println(count);

                //如果pairItemId不等之前的itemId
                if (accountid!=pairItemId)
                {
                    //并且uDateWindow不为空
                    if (uDateWindow!=null)
                    {
                        uDateWindow.endDate = beforeDate;
                    }
                    //
                    accountid = pairItemId;
                    uPairItem = new UPairItem(pairItemId,topicId,senderId,receiverId);
                    //-----------增加窗口--------------------------------
                    beforWindowId=0;
                    uDateWindow = new UDateWindow();
                    windowDateList = new ArrayList<>();
                    uDateWindow.beginDate = date;
                    uDateWindow.endDate = date;
                    uDateWindow.num++;
                    windowDateList.add(date);
                    uPairItem.dateWindowList.add(uDateWindow);
                    pairItemHashMap.put(pairItemId,uPairItem);
                    //----记录所有的item，按照主题不同存储---------------------------------
                    //主题(list)->senderid(map)->itemid(map)->item(map)
                    if (pairItemMapList.get(topicId)==null)
                    {
                        List<Integer> itemIdList = new ArrayList<>();
                        itemIdList.add(pairItemId);

                        HashMap<Integer,List<Integer>> senderMap = new HashMap<>();
                        senderMap.put(senderId,itemIdList);

                        pairItemMapList.set(topicId,senderMap);
                    }
                    else
                    {
                        HashMap<Integer,List<Integer>> senderMap = pairItemMapList.get(topicId);
                        if (senderMap.get(senderId)!=null)
                        {
                            List<Integer> itemIdList = senderMap.get(senderId);
                            itemIdList.add(pairItemId);
                        }
                        else
                        {
                            List<Integer> itemIdList = new ArrayList<>();
                            itemIdList.add(pairItemId);
                            senderMap.put(senderId,itemIdList);
                        }
                        //pairItemMapList.set(topicId,senderMap);
                    }
                }
                else if(uPairItem!=null)
                {
                    if (beforWindowId!=windowId) {
                        beforWindowId = windowId;
                        //-----------增加窗口--------------------------------
                        uDateWindow = new UDateWindow();
                        windowDateList = new ArrayList<>();
                        uDateWindow.beginDate = date;
                        uDateWindow.endDate = date;
                        uDateWindow.num++;
                        windowDateList.add(date);
                        uPairItem.dateWindowList.add(uDateWindow);
                        //---------------------------------------------------
                    }
                    else
                    {
                        uDateWindow.endDate = date;
                        uDateWindow.num++;
                        windowDateList.add(date);
                    }
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //读取输入文件的每行
    }


    /**
     * 生成Tsmin，数据结构对应userInfo
     * 生成Xmin，数据结构对应itemInfo
     * 生成pattern，需要在发送的senderUserInfo的基础上，生成receiverUserInfo
     * 生成长度为1的pattern
     */
    private void InitWindowPattern() {

        //HashMap<Integer,HashMap<Integer,UPairItem>>
        //主题(list)->senderid(map)->itemid(map)->item(map)

        //    HashMap中存储所有相同长度的pattern
        //    private List<HashMap<String,UWindowPattern>> patternList = new ArrayList<>();
        for (int i = 0; i < pairItemMapList.size(); i++) {
            HashMap<Integer,List<Integer>> itemSetMap = pairItemMapList.get(i);
            if (itemSetMap==null)
                continue;
            List<UWindowUserInfo> userInfoList = new ArrayList<>();
            List<UWindowItemInfo> itemList = new ArrayList<>();

            //新建一个长度为1的pattern，其主题为i
            UWindowPattern uPattern = new UWindowPattern(1);
            uPattern.topics.add(i);
            //构建pattern中的用户信息UserInfo，将receiverUser列存入pattern中
            HashMap<Integer, UWindowUserInfo> userInfoHashMap = new HashMap<>();
            if (userInfoHashMap==null)
                continue;
            for(Map.Entry<Integer,List<Integer>> entry: itemSetMap.entrySet())
            {
                List<Integer> itemIdList = entry.getValue();
                //对于当前senderUser在topic i的所有消息
                for (int j = 0; j < itemIdList.size(); j++) {

                    int itemId = itemIdList.get(j);
                    UPairItem uPairItem = pairItemHashMap.get(itemId);
                    int receiverId  = uPairItem.receiverId;
                    List<UDateWindow> uDateWindowList = uPairItem.dateWindowList;

                    for (int k = 0; k < uDateWindowList.size(); k++) {
                        UDateWindow uDateWindow = uDateWindowList.get(k);
                        if (uDateWindow.endDate==null)
                            System.out.println(1);
                        UWindowItemInfo uItemInfo = new UWindowItemInfo(itemId,k,uDateWindow);
                        //新建或获得receiverId对应的userInfo
                        UWindowUserInfo userInfo = null;
                        if (userInfoHashMap.containsKey(receiverId))
                        {
                            userInfo = userInfoHashMap.get(receiverId);
                        }
                        else
                        {
                            userInfo = new UWindowUserInfo(receiverId);
                        }
                        userInfo.receiverItemList.add(uItemInfo);
                        userInfoHashMap.put(receiverId,userInfo);
                    }
                }
            }
            uPattern.userList.add(userInfoHashMap);
            if (patternList.get(0) == null)
            {
                HashMap<String,UWindowPattern> uPatternHashMap = new HashMap<>();
                uPatternHashMap.put(String.valueOf(i),uPattern);
                patternList.set(0,uPatternHashMap);
            }
            else
            {
                HashMap<String,UWindowPattern> uPatternHashMap = patternList.get(0);
                uPatternHashMap.put(String.valueOf(i),uPattern);
            }
        }
    }

    private void InitParam(String name,String base, int topicNum, int patternMaxLength
            ,int timegap,int windowgap) {
        this.base = base;
        this.name = name;
        this.topicNum = topicNum;
        this.timegap = timegap;
        this.windowgap = windowgap;
        this.patternMaxLength = patternMaxLength;
        inputfile = base + "/TopicDetail/" + name
                + "/windowmessage2.txt";
        outputpath = base + "/UserTopicSequensce/" + name+"/"+timegap+","+windowgap;
        File dir = new File(outputpath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        outputpath = outputpath + "/result";
        //初始化pairItemMapList数组
        for (int i = 0; i < topicNum ; i++) {
            pairItemMapList.add(null);
        }
        //初始化patternList数组
        for (int i = 0; i < patternMaxLength; i++) {
            patternList.add(null);
        }
    }

    //FeaBreadthCal,开始计算序列
    private void ComputeBreadthSeqPattern() {
        for (int i = 2; i <= patternMaxLength; i++) {
            FeatureCal(i);
        }
    }

    // 计算长度为i的模式
    private void FeatureCal(int i) {
        i=i-1;
        HashMap<String,UWindowPattern> uPatternHashMap =  patternList.get(i-1);
        for(Map.Entry<String,UWindowPattern> entry: uPatternHashMap.entrySet())
        {
            UWindowPattern uPattern = entry.getValue();

            BlockingQueue<Runnable> workingQueue = new ArrayBlockingQueue<Runnable>(5);
            RejectedExecutionHandler rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
                ExecutorService pool = new ThreadPoolExecutor(5, 5, 0L, TimeUnit.MILLISECONDS,
                    workingQueue, rejectedExecutionHandler);

            // 创建多个有返回值的任务
            List<Future> resultlist = new ArrayList<>();
            for (int j = 0; j < topicNum; j++) {
                String patternStr = entry.getKey();
                //限制一：主题不相同
                if(uPattern.topics.contains(j))
                {
                    continue;
                }
                //按子句过算法
                Callable c = new AddWindowCallable(uPattern, j);
                // 执行任务并获取Future对象,返回值的类型是List<ErrorText>
                Future f = pool.submit(c);
                resultlist.add(f);
            }
            // 关闭线程池
            pool.shutdown();

            //遍历并发任务结果集
            for (int k = 0; k < resultlist.size(); k++) {
                UWindowPattern extPattern=null;
                try {
                    extPattern = (UWindowPattern) resultlist.get(k).get();
                    if (extPattern==null)
                        continue;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                //将新计算出来的pattern增加到patternList中
                String patternStr = extPattern.getSeqStr();
//                System.out.println(patternStr);
                if (patternList.get(i) == null)
                {
                    HashMap<String,UWindowPattern> extPatternHashMap = new HashMap<>();
                    extPatternHashMap.put(patternStr,extPattern);
                    patternList.set(i,extPatternHashMap);
                }
                else
                {
                    HashMap<String,UWindowPattern> extPatternHashMap = patternList.get(i);
                    extPatternHashMap.put(patternStr,extPattern);
                }
            }
        }
    }

    /*
     * 多线程访问纠错算法资源
     * */
    class AddWindowCallable implements Callable<UWindowPattern> {
        private UWindowPattern uPattern;//字句文本
        private int topicId;//错误索引

        AddWindowCallable(UWindowPattern uPattern, int topicId) {
            this.uPattern = uPattern;
//            this.indexid = indexid;
            this.topicId = topicId;
        }

        @Override
        public UWindowPattern call() {
            UWindowPattern extPattern = AddTotalWindowPatternTopic(uPattern, topicId);//调用纠错算法，传入值（子句，编号）
            return extPattern;
        }

        //窗口与窗口接，
        private UWindowPattern AddTotalWindowPatternTopic(UWindowPattern uPattern, int topicId) {

            //主题(list)->senderid(map)->itemid(map)->item(map)
            //获得新的topic的所有的sender用户集合
            HashMap<Integer,List<Integer>> uSenderPairSetHashMap = pairItemMapList.get(topicId);
            if (uSenderPairSetHashMap==null)
                return null;

            if(uPattern.getSeqStr().equals("15")&&topicId==25)
                System.out.println("15,25");
            //复制uPattern 到 extPattern
            UWindowPattern extPattern = new UWindowPattern(uPattern.len);
            CopyPattern(uPattern,extPattern);
            extPattern.InitFValue();
            //receiverUser用户id->UWindowUserInfo
            HashMap<Integer,UWindowUserInfo> userInfoHashMap = extPattern.userList.get(extPattern.len-1);
            //创建一个临时变量，记录用户的最大发送时间
            HashMap<Integer,Date> userTsmaxMap = new HashMap<>();
            //模式需要新增的用户列
            HashMap<Integer,UWindowUserInfo> extUserInfoHashMap = new HashMap<>();
            //计算扩展的新列的特征值
            //----正向遍历-----------------------------------------------------
            //遍历所有的receiverUser
            Iterator<Map.Entry<Integer, UWindowUserInfo>> it = userInfoHashMap.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry<Integer, UWindowUserInfo> entry = it.next();

                //获取当前的receiverUser
                Integer receiverUserId  = entry.getKey();
                UWindowUserInfo receiverUserIdInfo = entry.getValue();

                List<UWindowItemInfo> uWindowItemInfoList = receiverUserIdInfo.receiverItemList;
                //获取当前receiverUser的所有发送的消息
                List<Integer> uItemIdList = uSenderPairSetHashMap.get(receiverUserId);
                if (uItemIdList==null) {
                    continue;
                }

                //遍历当前用户作为发送方的所有的UPairItem
                for (int i = 0; i < uItemIdList.size(); i++) {
                    //获取当前发送的pairItem
                    Integer itemId = uItemIdList.get(i);
                    UPairItem sendPairItem = pairItemHashMap.get(itemId);
                    List<UDateWindow> uDateWindowList = sendPairItem.dateWindowList;

                    int newReceiverId = sendPairItem.receiverId;

                    //窗口的endDate为新增Item窗口的Tsmax,该值通过反向比较时修正
                    for (int j = 0; j < uDateWindowList.size(); j++)
                    {
                        UDateWindow uDateWindow = uDateWindowList.get(j);

                        Date tsmin = null;
                        //记录当前新增窗口item的所有前置item
                        List<UWindowItemInfo> preWindowItemInfoList = new ArrayList<>();
                        //遍历窗口中的所有时间

                        Date afterdate = uDateWindow.beginDate;
                        //遍历所有的前置窗口item
                        for (int l = 0; l < uWindowItemInfoList.size(); l++) {
                            UWindowItemInfo uWindowItemInfo = uWindowItemInfoList.get(l);
                            //---增加限制二，发送方不能等于接收方--------------------
//                            int preItemId = uWindowItemInfo.getItemId();
//                            UPairItem uPairItem = pairItemHashMap.get(preItemId);
//                            int senderId = uPairItem.senderId;
//                            if (senderId == newReceiverId)
//                                continue;
                            //------判断该item能够和该窗口接上，如果可以接上，则增加----------
                            //如果后面窗口的开始时间   前窗口beginDate <tsmin < 前窗口endDate + windowgap
                            //则认为该窗口可以接上
                            if (afterdate.after(uWindowItemInfo.getTsmin())
                                    &&DateUtil.diffHour(afterdate,windowgap).before(uWindowItemInfo.getTsmax()))
                            {
                                //新增窗口item增加可以连接上的前序窗口item
                                preWindowItemInfoList.add(uWindowItemInfo);
                                //赋予后续窗口的开始值
                                if(tsmin==null || afterdate.before(tsmin))
                                    tsmin = afterdate;
                                //记录前序窗口的最大值,如果后续窗口的最大时间大于前序窗口的最大时间
                                //则前序窗口的最大时间不需要调整
                            }
                        }
                        //如果有该后续窗口是有效的，则增加后续窗口
                        if (tsmin!=null)
                        {
                            UWindowItemInfo uItemInfo = new UWindowItemInfo(sendPairItem.itemId,j,tsmin,uDateWindow.endDate);
                            uItemInfo.preWindowItemList = preWindowItemInfoList;
                            //开始保存Ts（UWindowUserInfo）,扩展主题新增的userInfo
                            //新建或获得receiverId对应的userInfo
                            UWindowUserInfo  userInfo = null;
                            if (extUserInfoHashMap.containsKey(sendPairItem.receiverId))
                            {
                                userInfo = extUserInfoHashMap.get(sendPairItem.receiverId);
                                //对于新增用户，如何合并其item
                            }
                            else
                            {
                                userInfo = new UWindowUserInfo(sendPairItem.receiverId);
                            }
                            userInfo.receiverItemList.add(uItemInfo);
                            //反向更新，根据新item发送方的最大发送时间Tsmax
//                        UpdateReceiverMaxDate(userTsmaxMap,receiverUserId,uDateWindow.endDate);
                            extUserInfoHashMap.put(sendPairItem.receiverId,userInfo);
                        }
                    }
                }
            }
            if (extUserInfoHashMap.size()==0)
                return null;

            extPattern.len++;
            extPattern.topics.add(topicId);

            extPattern.userList.add(extUserInfoHashMap);

            CalcPatternUserValueNew(extPattern);

            return extPattern;
        }


        //根据用户三元组来计算值v1-5
        private void CalcPatternUserValue(UWindowPattern extPattern) {
            //receiverUser用户id->UWindowUserInfo
            if(extPattern.getSeqStr().equals("15,25"))
                System.out.println("15,25");
            float m1,m2,m3;
            int fnum=0;
            List<Integer> allUserList = new ArrayList<>();
            List<Integer> preUserList = new ArrayList<>();
            List<Integer> validUserList = new ArrayList<>();

            List<UWindowItemInfo> preWindowList = new ArrayList<>();
            List<UWindowItemInfo> validWindowList = new ArrayList<>();

            List<Integer> itemList = new ArrayList<>();

            for (int i = extPattern.len-1; i >=0 ; i--) {
                HashMap<Integer, UWindowUserInfo> userInfoHashMap = extPattern.userList.get(i);
                preUserList.clear();
                m1=0.0f;
                m2=0.0f;
                m3=0.0f;
                int itemId;

                itemList.clear();

                Iterator<Map.Entry<Integer, UWindowUserInfo>> it = userInfoHashMap.entrySet().iterator();
                while(it.hasNext()){
                    Map.Entry<Integer, UWindowUserInfo> entry = it.next();
                    //获取当前的receiverUser
                    Integer receiverUserId =  entry.getKey();
                    if (validUserList.size()>0 && !validUserList.contains(receiverUserId)) {
                        it.remove();
                        continue;
                    }

                    UWindowUserInfo receiverUserIdInfo = entry.getValue();
                    if (!allUserList.contains(receiverUserId))
                        allUserList.add(receiverUserId);

                    List<UWindowItemInfo> uWindowItemInfoList = receiverUserIdInfo.receiverItemList;
                    for (int j = 0; j < uWindowItemInfoList.size(); j++) {
                        UWindowItemInfo uWindowItemInfo = uWindowItemInfoList.get(j);
                        if (validWindowList.size()>0 && !validWindowList.contains(uWindowItemInfo))
                        {
                            uWindowItemInfoList.remove(j);
                            j--;
                            continue;
                        }
                        else
                        {
                            m1 ++;
                            m3 += CalMessageNum(uWindowItemInfo);
                            List<UWindowItemInfo> preList =uWindowItemInfo.preWindowItemList;
                            preWindowList.addAll(preList);
                        }
                        itemId = uWindowItemInfo.getItemId();
                        if (!itemList.contains(itemId))
                            itemList.add(itemId);
                        UPairItem uPairItem = pairItemHashMap.get(itemId);
                        int senderId = uPairItem.senderId;
                        if (!preUserList.contains(senderId)) {
                            preUserList.add(senderId);
                        }
                    }
                }
                validUserList.clear();
                validUserList.addAll(preUserList);
                validWindowList.clear();
                validWindowList.addAll(preWindowList);
                extPattern.itemnum.add(m1);
                extPattern.f4+=itemList.size();
                extPattern.f5+=m1;
                extPattern.f6+=m3;
                if (extPattern.f2 <m1)
                    extPattern.f2 = m1;
                if (extPattern.f3==0 || extPattern.f3>m1)
                    extPattern.f3 = m1;

                if (extPattern.u2 <itemList.size())
                    extPattern.u2 = itemList.size();
                if (extPattern.u3==0 || extPattern.u3>itemList.size())
                    extPattern.u3 = itemList.size();
            }
            for (int i = 0; i < preUserList.size(); i++) {
                if (!allUserList.contains(preUserList.get(i)))
                {
                    allUserList.add(preUserList.get(i));
                }
            }
            extPattern.f1=allUserList.size();
        }


        //根据用户三元组来计算值v1-5
        private void CalcPatternUserValueNew(UWindowPattern extPattern) {
            //receiverUser用户id->UWindowUserInfo
            float m1,m2,m3;
            int fnum=0;
            HashSet<Integer> allUserList = new HashSet<>();

            //
            HashMap<Integer,List<UWindowItemInfo>> preWindowList = new HashMap<>();
            HashMap<Integer,List<UWindowItemInfo>> validWindowMap = new HashMap<>();

            HashSet<Integer> itemList = new HashSet<>();

            for (int i = extPattern.len-1; i >=0 ; i--) {
                HashMap<Integer, UWindowUserInfo> userInfoHashMap = extPattern.userList.get(i);
                m1=0.0f;
                m2=0.0f;
                m3=0.0f;
                int itemId;

                itemList.clear();
                preWindowList = new HashMap<>();

                Iterator<Map.Entry<Integer, UWindowUserInfo>> it = userInfoHashMap.entrySet().iterator();
                while(it.hasNext()){
                    Map.Entry<Integer, UWindowUserInfo> entry = it.next();
                    //获取当前的receiverUser
                    Integer receiverUserId =  entry.getKey();
                    if (validWindowMap.size()>0 && !validWindowMap.containsKey(receiverUserId)) {
                        it.remove();
                        continue;
                    }

                    UWindowUserInfo receiverUserIdInfo = entry.getValue();
                    if (!allUserList.contains(receiverUserId))
                        allUserList.add(receiverUserId);

                    if (validWindowMap!=null && validWindowMap.size()>0)
                        receiverUserIdInfo.receiverItemList = validWindowMap.get(receiverUserId);
                    List<UWindowItemInfo> uWindowItemInfoList = receiverUserIdInfo.receiverItemList;
                    for (int j = 0; j < uWindowItemInfoList.size(); j++) {
                        UWindowItemInfo uWindowItemInfo = uWindowItemInfoList.get(j);

                        m1 ++;
                        m3 += CalMessageNum(uWindowItemInfo);
                        List<UWindowItemInfo> preList =uWindowItemInfo.preWindowItemList;
                        int tmpItemId = uWindowItemInfo.getItemId();
                        int userId = pairItemHashMap.get(tmpItemId).getSenderId();
                        if (!allUserList.contains(userId))
                        {
                            allUserList.add(userId);
                        }
                        if (i>0) {
                            if (preWindowList.containsKey(userId)) {
                                List<UWindowItemInfo> exList = preWindowList.get(userId);
                                for (int k = 0; k < preList.size(); k++) {
                                    if (!exList.contains(preList.get(k)))
                                        exList.add(preList.get(k));
                                }
                            } else {
                                preWindowList.put(userId, preList);
                            }
                        }

                        itemId = uWindowItemInfo.getItemId();
                        if (!itemList.contains(itemId))
                            itemList.add(itemId);
                    }
                }
                validWindowMap.clear();
                validWindowMap = preWindowList;
                extPattern.itemnum.add(m1);
                extPattern.f4+=itemList.size();
                extPattern.f5+=m1;
                extPattern.f6+=m3;
                if (extPattern.f2 <m1)
                    extPattern.f2 = m1;
                if (extPattern.f3==0 || extPattern.f3>m1)
                    extPattern.f3 = m1;

                if (extPattern.u2 <itemList.size())
                    extPattern.u2 = itemList.size();
                if (extPattern.u3==0 || extPattern.u3>itemList.size())
                    extPattern.u3 = itemList.size();
            }

            extPattern.f1=allUserList.size();
        }

        private int CalMessageNum(UWindowItemInfo uWindowItemInfo) {
            int fnum=0;

            int itemId =  uWindowItemInfo.getItemId();
            int windowId = uWindowItemInfo.getWindowId();

            UPairItem uPairItem = pairItemHashMap.get(itemId);
            fnum = uPairItem.dateWindowList.get(windowId).num;
//        for (int i = 0; i < dateList.size(); i++) {
//            Date date = dateList.get(i);
//            if (uWindowItemInfo.getTsmin().after(date))
//                continue;
//            else if (!date.after(uWindowItemInfo.getTsmin()))
//                fnum++;
//        }

            return fnum;
        }

        private void CopyPattern(UWindowPattern uPattern, UWindowPattern extPattern) {
            //深层拷贝，extPattern.topics = uPattern.topics;
            List<Integer> exttopics = new ArrayList<>();
            List<Integer> topics = uPattern.topics;
            for (int i = 0; i < topics.size(); i++) {
                exttopics.add(topics.get(i));
            }
            extPattern.topics = exttopics;
            //深层拷贝，List<HashMap<Integer,UWindowUserInfo>> userList = new ArrayList<>();
            List<HashMap<Integer,UWindowUserInfo>> extUserList = new ArrayList<>();
            List<HashMap<Integer,UWindowUserInfo>> userList = uPattern.userList;
            //由于UWindowItemInfo也重新copy了，那么对应的前置窗口和后置窗口的也变了
            //存储旧->新由于UWindowItemInfo的对应关系
            HashMap<UWindowItemInfo, UWindowItemInfo> itemInfoMap = new HashMap<>();
            for (int i = 0; i < userList.size(); i++) {
                HashMap<Integer,UWindowUserInfo> extUserInfoHashMap = new HashMap<>();
                HashMap<Integer,UWindowUserInfo> userInfoHashMap = userList.get(i);
                for(Map.Entry<Integer,UWindowUserInfo> entry: userInfoHashMap.entrySet())
                {
                    //复制userInfo
                    Integer userId = entry.getKey();
                    UWindowUserInfo userInfo = entry.getValue();
                    UWindowUserInfo extUserInfo = new UWindowUserInfo(userId);
                    List<UWindowItemInfo> itemList = userInfo.receiverItemList;
                    List<UWindowItemInfo> extItemList = new ArrayList<>();
                    for (int j = 0; j < itemList.size(); j++) {
                        UWindowItemInfo itemInfo = itemList.get(j);
                        UWindowItemInfo extItemInfo = new UWindowItemInfo(itemInfo.getItemId(),itemInfo.getWindowId(),
                                itemInfo.getTsmin(),itemInfo.getTsmax());
                        extItemList.add(extItemInfo);
                        itemInfoMap.put(itemInfo,extItemInfo );
                    }
                    extUserInfo.receiverItemList = extItemList;
                    extUserInfoHashMap.put(userId,extUserInfo);
                }
                extUserList.add(extUserInfoHashMap);
            }
            extPattern.userList = extUserList;
            //增加每个新item的前置窗口和后置窗口
            for(Map.Entry<UWindowItemInfo,UWindowItemInfo> entry: itemInfoMap.entrySet() ) {
                UWindowItemInfo itemInfo = entry.getKey();
                UWindowItemInfo extItemInfo = entry.getValue();

                for (int k = 0; k < itemInfo.preWindowItemList.size(); k++) {
                    UWindowItemInfo uWindowItemInfo = itemInfo.preWindowItemList.get(k);
                    UWindowItemInfo preWindowItemInfo = itemInfoMap.get(uWindowItemInfo);
                    extItemInfo.preWindowItemList.add(preWindowItemInfo);
                }
            }
        }
    }

    private void OutSubPatternToExcel(int patternlen) {

        XSSFWorkbook  wb = new XSSFWorkbook ();
        int i=patternlen;

        HashMap<String,UWindowPattern> patternHashMap = patternList.get(i);
        Sheet  sheet = wb.createSheet("sheet"+i);
        //设置列名
        Row row = sheet.createRow(0);
        Cell cell = row.createCell(0);
        cell.setCellValue("pattern");
        //窗口item
        cell = row.createCell(1);
        cell.setCellValue("total");
        int k=1;
        for (; k <= 6; k++) {
            cell = row.createCell(k+1);
            cell.setCellValue("f"+k);
        }
        cell = row.createCell(k+1);
        cell.setCellValue("u2");
        cell = row.createCell(k+2);
        cell.setCellValue("u3");
        int l=1;
        for (; l <= i+1; l++) {
            cell = row.createCell(l+9);
            cell.setCellValue("m"+(i+2-l));
        }
        int col = 1;
        if (patternHashMap==null)
            return;
        List<String> topPatternList = CalTopPattern(patternHashMap);

        for(Map.Entry<String,UWindowPattern> entry: patternHashMap.entrySet()) {
            String patternStr = entry.getKey();
            UWindowPattern uPattern = entry.getValue();

            float total = uPattern.f1 + uPattern.f2 + uPattern.f3 + uPattern.f4;
            //输出模式的实例
            if(topPatternList.contains(patternStr))
            {
                OutPatternDetailExcel(patternStr,uPattern);
            }
            row = sheet.createRow(col);
            col++;
            cell = row.createCell(0);
            cell.setCellValue(patternStr);
            cell = row.createCell(1);
            cell.setCellValue(total);
            cell = row.createCell(2);
            cell.setCellValue(uPattern.f1);
            cell = row.createCell(3);
            cell.setCellValue(uPattern.f2);
            cell = row.createCell(4);
            cell.setCellValue(uPattern.f3);
            cell = row.createCell(5);
            cell.setCellValue(uPattern.f4);
            cell = row.createCell(6);
            cell.setCellValue(uPattern.f5);
            cell = row.createCell(7);
            cell.setCellValue(uPattern.f6);
            cell = row.createCell(8);
            cell.setCellValue(uPattern.u2);
            cell = row.createCell(9);
            cell.setCellValue(uPattern.u3);
            int len = uPattern.itemnum.size();
            int j = 0;
            for (; j <len ; j++) {
                float m =  uPattern.itemnum.get(j);
                cell = row.createCell(10+j);
                cell.setCellValue(m);
            }
        }

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try
        {
            wb.write(os);
        } catch (IOException e){
            e.printStackTrace();
        }
        byte[] content = os.toByteArray();
        String path = outputpath + ".xlsx";
        File file = new File(path);//Excel文件生成后存储的位置。
        OutputStream fos  = null;
        try
        {
            fos = new FileOutputStream(file);
            fos.write(content);
            os.close();
            fos.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    private void OutToExcel() {

        XSSFWorkbook  wb = new XSSFWorkbook ();
        for (int i = 1; i < patternMaxLength ; i++) {
            HashMap<String,UWindowPattern> patternHashMap = patternList.get(i);
            Sheet  sheet = wb.createSheet("sheet"+i);
            //设置列名
            Row row = sheet.createRow(0);
            Cell cell = row.createCell(0);
            cell.setCellValue("pattern");
            //窗口item
            cell = row.createCell(1);
            cell.setCellValue("total");
            int k=1;
            for (; k <= 6; k++) {
                cell = row.createCell(k+1);
                cell.setCellValue("f"+k);
            }
            cell = row.createCell(k+1);
            cell.setCellValue("u2");
            cell = row.createCell(k+2);
            cell.setCellValue("u3");
            int l=1;
            for (; l <= i+1; l++) {
                cell = row.createCell(l+9);
                cell.setCellValue("m"+(i+2-l));
            }
            int col = 1;
            if (patternHashMap==null)
                return;
            List<String> topPatternList = CalTopPattern(patternHashMap);

            for(Map.Entry<String,UWindowPattern> entry: patternHashMap.entrySet()) {
                String patternStr = entry.getKey();
                UWindowPattern uPattern = entry.getValue();

                float total = uPattern.f1 + uPattern.f2 + uPattern.f3 + uPattern.f4;
                //输出模式的实例
                if(topPatternList.contains(patternStr))
                {
                    OutPatternDetailExcel(patternStr,uPattern);
                }
                row = sheet.createRow(col);
                col++;
                cell = row.createCell(0);
                cell.setCellValue(patternStr);
                cell = row.createCell(1);
                cell.setCellValue(total);
                cell = row.createCell(2);
                cell.setCellValue(uPattern.f1);
                cell = row.createCell(3);
                cell.setCellValue(uPattern.f2);
                cell = row.createCell(4);
                cell.setCellValue(uPattern.f3);
                cell = row.createCell(5);
                cell.setCellValue(uPattern.f4);
                cell = row.createCell(6);
                cell.setCellValue(uPattern.f5);
                cell = row.createCell(7);
                cell.setCellValue(uPattern.f6);
                cell = row.createCell(8);
                cell.setCellValue(uPattern.u2);
                cell = row.createCell(9);
                cell.setCellValue(uPattern.u3);
                int len = uPattern.itemnum.size();
                int j = 0;
                for (; j <len ; j++) {
                    float m =  uPattern.itemnum.get(j);
                    cell = row.createCell(10+j);
                    cell.setCellValue(m);
                }
            }
        }
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try
        {
            wb.write(os);
        } catch (IOException e){
            e.printStackTrace();
        }
        byte[] content = os.toByteArray();
        String path = outputpath + ".xlsx";
        File file = new File(path);//Excel文件生成后存储的位置。
        OutputStream fos  = null;
        try
        {
            fos = new FileOutputStream(file);
            fos.write(content);
            os.close();
            fos.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private List<String> CalTopPattern(HashMap<String, UWindowPattern> patternHashMap) {
        List<String> topPatternList = new ArrayList<>();
        List<Float> topValue = new ArrayList<>();
        for (int i = 0; i < topNumber; i++) {
            topPatternList.add(null);
            topValue.add(0.0f);
        }
        String patternStr;
        UWindowPattern uPattern;
        float total;
        float tempValue = 0.0f;
        String tempStr = null;
        for(Map.Entry<String,UWindowPattern> entry: patternHashMap.entrySet()) {

            tempValue = 0.0f;
            tempStr = null;

            patternStr = entry.getKey();
            uPattern = entry.getValue();
            if(patternStr.equals("0,16"))
                System.out.println("0,16");
            total = uPattern.f1 + uPattern.f2 + uPattern.f3 + uPattern.f4;
            for (int i = 0; i < topNumber; i++) {
                if (tempValue!=0)
                {
                    total = tempValue;
                    patternStr = tempStr;
                }

                if (total>topValue.get(i))
                {
                    tempValue = topValue.get(i);
                    tempStr = topPatternList.get(i);

                    topValue.set(i,total);
                    topPatternList.set(i,patternStr);

                    if(tempValue == 0)
                        break;
                }
            }
        }
        return topPatternList;
    }

    private void OutToFile() {
        for (int i = 1; i < patternMaxLength ; i++) {
            HashMap<String,UWindowPattern> patternHashMap = patternList.get(i);
            try {
                BufferedWriter writer = new BufferedWriter
                        (new OutputStreamWriter (new FileOutputStream (outputpath+String.valueOf(i+1)+".txt"), "UTF-8"));
                for(Map.Entry<String,UWindowPattern> entry: patternHashMap.entrySet()) {
                    String patternStr = entry.getKey();
                    UWindowPattern uPattern = entry.getValue();
                    String itemnumStr = "";
                    for (int j = 0; j <uPattern.itemnum.size() ; j++) {
                        float m =  uPattern.itemnum.get(j);
                        itemnumStr +=  " "+m ;
                    }
                    float total = uPattern.f1 + uPattern.f2 + uPattern.f3 + uPattern.f4;
                    writer.write( patternStr + " " +total
                            + " " + uPattern.f1
                            + " " + uPattern.f2
                            + " " + uPattern.f3
                            + " " + uPattern.f4
                            +itemnumStr
                            + "\r\n"
                    );
                }
                writer.close();
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
//        for(Map.Entry<String,UWindowPattern> entry: patternHashMap.entrySet()) {
//            String patternStr = entry.getKey();
//            UWindowPattern uPattern = entry.getValue();
//            float total = uPattern.f1 + uPattern.f2 +uPattern.f3 +uPattern.f4;
//                    System.out.println("pattern:" + patternStr+" "+ "f1:" + uPattern.f1
//                    +" "+ "f2:" + uPattern.f2
//                    +" "+ "f3:" + uPattern.f3
//                    +" "+ "f4:" + uPattern.f4
//            );
//            System.out.println("pattern:" + patternStr);
//            System.out.println("f1:" + uPattern.f1);
//            System.out.println("f2:" + uPattern.f2);
//            System.out.println("f3:" + uPattern.f3);
//            System.out.println("f4:" + uPattern.f4);

        //OutPatternDetail(uPattern);
//        }
    }

    private void OutPatternDetailExcel(String patternStr,UWindowPattern uPattern) {

        XSSFWorkbook wb = new XSSFWorkbook();
        List<HashMap<Integer,UWindowUserInfo>>  userInfoList = uPattern.userList;
        for (int i = 0; i < userInfoList.size(); i++) {
            Sheet sheet = wb.createSheet("pairItem"+i);
            //设置列名
            Row row = sheet.createRow(0);
            Cell cell = row.createCell(0);
            cell.setCellValue("topic");
            cell = row.createCell(1);
            cell.setCellValue("sender");
            cell = row.createCell(2);
            cell.setCellValue("receiver");
            cell = row.createCell(3);
            cell.setCellValue("beginTime");
            HashMap<Integer,UWindowUserInfo> userMap = userInfoList.get(i);
            int col=1;
            for(Map.Entry<Integer,UWindowUserInfo> entry: userMap.entrySet()) {
                int userId = entry.getKey();
                UWindowUserInfo userInfo = entry.getValue();
                List<UWindowItemInfo>  itemInfoList = userInfo.receiverItemList;
                for (int j = 0; j < itemInfoList.size(); j++) {
                    UWindowItemInfo itemInfo = itemInfoList.get(j);
                    UPairItem uPairItem = pairItemHashMap.get(itemInfo.getItemId());
                    row = sheet.createRow(col++);
                    cell = row.createCell(0);
                    cell.setCellValue(uPairItem.topicId);
                    cell = row.createCell(1);
                    cell.setCellValue(uPairItem.senderId);
                    cell = row.createCell(2);
                    cell.setCellValue( uPairItem.receiverId);
                    cell = row.createCell(3);
                    cell.setCellValue(itemInfo.getTsmin().toString());
                }
            }
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            try
            {
                wb.write(os);
            } catch (IOException e){
                e.printStackTrace();
            }
            byte[] content = os.toByteArray();
            String path = outputpath + "detail"+patternStr+".xlsx";
            File file = new File(path);//Excel文件生成后存储的位置。
            OutputStream fos  = null;
            try
            {
                fos = new FileOutputStream(file);
                fos.write(content);
                os.close();
                fos.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    private void OutPatternDetail(UWindowPattern uPattern) {

        List<HashMap<Integer,UWindowUserInfo>>  userInfoList = uPattern.userList;
        for (int i = 0; i < userInfoList.size(); i++) {

            HashMap<Integer,UWindowUserInfo> userMap = userInfoList.get(i);
            for(Map.Entry<Integer,UWindowUserInfo> entry: userMap.entrySet()) {
                int userId = entry.getKey();
                UWindowUserInfo userInfo = entry.getValue();
                List<UWindowItemInfo>  itemInfoList = userInfo.receiverItemList;
                for (int j = 0; j < itemInfoList.size(); j++) {
                    UWindowItemInfo itemInfo = itemInfoList.get(j);
                    UPairItem uPairItem = pairItemHashMap.get(itemInfo.getItemId());
                    System.out.println(uPairItem.topicId+","+uPairItem.senderId + "," + uPairItem.receiverId+","+uPairItem.dateWindowList.get(0).beginDate);
                }
            }

        }
    }
}
