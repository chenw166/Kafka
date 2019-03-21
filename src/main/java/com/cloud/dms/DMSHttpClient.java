package com.cloud.dms;

import static com.cloud.dms.ApiUtils.acknowledgeMessages;
import static com.cloud.dms.ApiUtils.constructTempMessages;
import static com.cloud.dms.ApiUtils.consumeMessages;
import static com.cloud.dms.ApiUtils.createGroup;
import static com.cloud.dms.ApiUtils.createQueue;
import static com.cloud.dms.ApiUtils.deleteGroup;
import static com.cloud.dms.ApiUtils.deleteQueue;
import static com.cloud.dms.ApiUtils.getGroups;
import static com.cloud.dms.ApiUtils.getQuota;
import static com.cloud.dms.ApiUtils.listQueues;
import static com.cloud.dms.ApiUtils.parseHandlerIds;
import static com.cloud.dms.ApiUtils.parseQueueGroupId;
import static com.cloud.dms.ApiUtils.parseQueueId;
import static com.cloud.dms.ApiUtils.retrieveQueue;
import static com.cloud.dms.ApiUtils.sendMessages;

import java.net.URL;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cloud.dms.access.AccessServiceUtils;

/**
 * 
 * <一句话功能简述>
 * <功能详细描述>
 * 
 * @author  jWX421652
 * @version  [版本号, 2017年3月11日]
 * @see  [相关类/方法]
 * @since  [产品/模块版本]
 */
@RestController
@RequestMapping("DMSHttpClient")
public class DMSHttpClient
{
    private static final int TIME_OUT_200 = 2000;

    private static final int NUM_MSG_10 = 10;

    private static String endpointUrl = "";

    private static String region = "";

    private static String serviceName = "dms";

    private static String aKey = "";

    private static String sKey = "";

    private static String projectId = "";

    private static String queueId = "";

    private static String queueGroupId = "";

    private static ArrayList<String> handlerIds;

    /*
     * Read Configure File And Initialize Variables
     */
    static
    {
        URL configPath = ClassLoader.getSystemResource("dms-service-config.properties");
        Properties prop = AccessServiceUtils.getPropsFromFile(configPath.getFile());
        region = prop.getProperty(Constants.DMS_SERVICE_REGION);
        aKey = prop.getProperty(Constants.DMS_SERVICE_AK);
        sKey = prop.getProperty(Constants.DMS_SERVICE_SK);
        endpointUrl = prop.getProperty(Constants.DMS_SERVICE_ENDPOINT_URL);
        if (endpointUrl.endsWith("/"))
        {
            endpointUrl = endpointUrl + "v1.0/";
        }
        else
        {
            endpointUrl = endpointUrl + "/v1.0/";
        }
        projectId = prop.getProperty(Constants.DMS_SERVICE_PROJECT_ID);
    }

    /**
     * main entry port
     * Run all api methods if no passing args, delete group or delete queue if pass
     *  args like 'rm-g-{group id}' or 'rm-q-{queue id}'
     * User can pass args to delete group or delete queue when quotas is not enough
     *
     * @param args      args can be like 'rm-g-{group id}' or 'rm-q-{queue id}', only one of them for one time.
     */
    public static void main(String[] args)
    {
        if (args.length == 1)
        {
            //pass parameter to delete group: rm-g-{group id}
            if (args[0].startsWith("rm-g-"))
            {
                queueGroupId = args[0].split("rm-g-")[1].toString();
                System.out.println("Start to delete group....");
                deleteQueueAndGroup("", queueGroupId);
            }

            //if quota is not enough, user can delete queue first
            //pass parameter to delete queue: rm-q-{queue id}
            if (args[0].startsWith("rm-q-"))
            {
                queueId = args[0].split("rm-q-")[1].toString();
                System.out.println("Start to delete queue....");
                deleteQueueAndGroup(queueId, "");
            }
        }
        else
        {
            runAllApiMethods();
        }
    }

    /**
     * run all api methods
     */
    @RequestMapping("runAllApiMethods")
    public static void runAllApiMethods()
    {
        String[] groupNames = {"gName_" + System.currentTimeMillis()};

        //List All Queues
        listQueues(projectId, endpointUrl, serviceName, region, aKey, sKey);

        //Get Quota
        getQuota(projectId, endpointUrl, serviceName, region, aKey, sKey);

        //Create Queue
        String queueName = "qName" + System.currentTimeMillis();
        String queueDes = "qDes_" + System.currentTimeMillis();
        ResponseMessage createQueueResMsg =
            createQueue(queueName, queueDes, projectId, endpointUrl, serviceName, region, aKey, sKey);
        queueId = parseQueueId(createQueueResMsg);
        if ("".equals(queueId.trim()))
        {
            System.exit(1);
        }

        //Create Group
        ResponseMessage createGroupResMsg =
            createGroup(groupNames, queueId, projectId, endpointUrl, serviceName, region, aKey, sKey);
        queueGroupId = parseQueueGroupId(createGroupResMsg);

        //Retrieve Specific Queue
        retrieveQueue(queueId, projectId, endpointUrl, serviceName, region, aKey, sKey);

        //Wait 2 seconds, then send,consume and acknowledge consume message,etc..
        Timer timer = new Timer();
        timer.schedule(new TimerTask()
        {
            @Override
            public void run()
            {
                //Send Message
                String messages = constructTempMessages(NUM_MSG_10);
                sendMessages(messages, queueId, projectId, endpointUrl, serviceName, region, aKey, sKey);

                //Consume Message
                ResponseMessage consumeMessagesResMsg = consumeMessages(queueId,
                        queueGroupId,
                        NUM_MSG_10,
                        projectId,
                        endpointUrl,
                        serviceName,
                        region,
                        aKey,
                        sKey);
                handlerIds = parseHandlerIds(consumeMessagesResMsg);

                //Acknowledge Message
                if (handlerIds.size() > 0)
                {
                    acknowledgeMessages(handlerIds,
                            queueGroupId,
                            queueId,
                            projectId,
                            endpointUrl,
                            serviceName,
                            region,
                            aKey,
                            sKey);
                }

                //Get All Groups
                getGroups(queueId, projectId, endpointUrl, serviceName, region, aKey, sKey);

                if (!"".equals(queueId.trim()))
                {
                    //User Timer to delete the created group and queue
                    Timer deleteTimer = new Timer();
                    deleteTimer.schedule(new TimerTask()
                    {
                        @Override
                        public void run()
                        {
                            deleteQueueAndGroup(queueId, queueGroupId);
                        }
                    }, TIME_OUT_200);// wait for 2 second and delete
                }
                else
                {
                    System.exit(0);
                }
            }
        }, TIME_OUT_200);
    }

    /**
     * delete queue or consumer group
     *
     * @param queueID           the delete queue id
     * @param queueGroupID      the delete group id
     */
    private static void deleteQueueAndGroup(String queueID, String queueGroupID)
    {
        if (!"".equals(queueGroupID.trim()))
        {
            //Delete Group
            deleteGroup(queueGroupID, queueID, projectId, endpointUrl, serviceName, region, aKey, sKey);
        }

        if (!"".equals(queueID.trim()))
        {
            //Delete Queue
            deleteQueue(queueID, projectId, endpointUrl, serviceName, region, aKey, sKey);
        }

        System.exit(0);
    }

}
