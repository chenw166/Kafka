package com.cloud.dms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.cloud.dms.access.AccessServiceUtils;

import java.io.IOException;
import java.util.ArrayList;

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
public class ApiUtils
{
    /**
     * List All Queues
     * GET /v1.0/{project_id}/queues
     *
     * @param projectId         Your Project ID
     * @param dmsUrl            The DMS Request URL
     * @param serviceName       Service Name
     * @param region            Region
     * @param ak                Your Access Key ID
     * @param sk                Your Secret Access Key
     * @return ResponseMessage  The response content
     *
     */
    public static ResponseMessage listQueues(String projectId, String dmsUrl, String serviceName, String region,
            String ak, String sk)
    {
        ResponseMessage resMsg;
        String url = dmsUrl + projectId + Constants.SYMBOL_SLASH + Constants.DMS_SERVICE_QUEUES;
        resMsg = AccessServiceUtils.get(serviceName, region, ak, sk, url, projectId);

        if (!resMsg.isSuccess())
        {
            System.out.println("List all queues fail: " + resMsg.getStatusCode());
        }
        return resMsg;
    }

    /**
     * Retrieve Specific Queue
     * GET /v1.0/{project_id}/queues/{queue_id}
     *
     * @param qId               Queue Id
     * @param projectId         Your Project ID
     * @param dmsUrl            The DMS Request URL
     * @param serviceName       Service Name
     * @param region            Region
     * @param ak                Your Access Key ID
     * @param sk                Your Secret Access Key
     * @return ResponseMessage  The response content
     */
    public static ResponseMessage retrieveQueue(String qId, String projectId, String dmsUrl, String serviceName,
            String region, String ak, String sk)
    {
        ResponseMessage resMsg;

        String url = dmsUrl + projectId + Constants.SYMBOL_SLASH + Constants.DMS_SERVICE_QUEUES
                + Constants.SYMBOL_SLASH + qId;
        resMsg = AccessServiceUtils.get(serviceName, region, ak, sk, url, projectId);
        if (!resMsg.isSuccess())
        {
            System.out.println("Retrieve queue '" + qId + "' fail: " + resMsg.getStatusCode());
        }
        return resMsg;
    }

    /**
     * Create Queue
     * POST /v1.0/{project_id}/queues
     *
     * @param qName             The name of create Queue
     * @param qDescription      The description of create Queue
     * @param projectId         Your Project ID
     * @param dmsUrl            The DMS Request URL
     * @param serviceName       Service Name
     * @param region            Region
     * @param ak                Your Access Key ID
     * @param sk                Your Secret Access Key
     * @return ResponseMessage  The response content
     */
    public static ResponseMessage createQueue(String qName, String qDescription, String projectId, String dmsUrl,
            String serviceName, String region, String ak, String sk)
    {
        ResponseMessage resMsg = new ResponseMessage();

        String url = dmsUrl + projectId + Constants.SYMBOL_SLASH + Constants.DMS_SERVICE_QUEUES;
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode objectNode1 = mapper.createObjectNode();
        objectNode1.put(Constants.KEY_NAME, qName);
        objectNode1.put(Constants.KEY_DES, qDescription);
        String bodyJson = null;
        try
        {
            bodyJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode1);
        }
        catch (JsonProcessingException e)
        {
            e.printStackTrace();
        }
        if (bodyJson != null)
        {
            resMsg = AccessServiceUtils.post(serviceName, region, ak, sk, url, bodyJson, projectId);
            if (!resMsg.isSuccess())
            {
                System.out.println("Create queue with name '" + qName + "' fail: " + resMsg.getStatusCode());
            }
        }
        else
        {
            System.out.println("Miss the required queue body.");
        }
        return resMsg;
    }

    /**
     * Delete Queue
     * DELETE /v1.0/{project_id}/queues/{queue_id}
     *
     * @param qId               The Queue Id which require to delete
     * @param projectId         Your Project ID
     * @param dmsUrl            The DMS Request URL
     * @param serviceName       Service Name
     * @param region            Region
     * @param ak                Your Access Key ID
     * @param sk                Your Secret Access Key
     * @return ResponseMessage  The response content
     */
    public static ResponseMessage deleteQueue(String qId, String projectId, String dmsUrl, String serviceName,
            String region, String ak, String sk)
    {
        ResponseMessage resMsg;

        String url = dmsUrl + projectId + Constants.SYMBOL_SLASH + Constants.DMS_SERVICE_QUEUES
                + Constants.SYMBOL_SLASH + qId;
        resMsg = AccessServiceUtils.delete(serviceName, region, ak, sk, url, projectId);
        if (resMsg.isSuccess())
        {
            System.out.println("Delete queue '" + qId + "' success.");
        }
        else
        {
            System.out.println("Delete queue '" + qId + "' fail: " + resMsg.getStatusCode());
        }
        return resMsg;
    }

    /**
     * Create Consume Group
     * POST /v1.0/{project_id}/queues/{queue_id}/groups
     *
     * @param gNames            The group names which require to create
     * @param qId               The Queue Id, create new group must based this queue id
     * @param projectId         Your Project ID
     * @param dmsUrl            The DMS Request URL
     * @param serviceName       Service Name
     * @param region            Region
     * @param ak                Your Access Key ID
     * @param sk                Your Secret Access Key
     * @return ResponseMessage  The response content
     */
    public static ResponseMessage createGroup(String[] gNames, String qId, String projectId, String dmsUrl,
            String serviceName, String region, String ak, String sk)
    {
        ResponseMessage resMsg = new ResponseMessage();

        String url = dmsUrl + projectId + Constants.SYMBOL_SLASH + Constants.DMS_SERVICE_QUEUES
                + Constants.SYMBOL_SLASH + qId + Constants.SYMBOL_SLASH + Constants.DMS_SERVICE_GROUPS;

        ObjectMapper mapper = new ObjectMapper();
        ArrayNode groupsArray = mapper.createArrayNode();
        for (String gName : gNames)
        {
            ObjectNode groupNode = mapper.createObjectNode();
            groupNode.put(Constants.KEY_NAME, gName);
            groupsArray.add(groupNode);
        }
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.putPOJO(Constants.DMS_SERVICE_GROUPS, groupsArray);

        String bodyJson = null;
        try
        {
            bodyJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
        }
        catch (JsonProcessingException e)
        {
            e.printStackTrace();
        }
        if (bodyJson != null)
        {
            resMsg = AccessServiceUtils.post(serviceName, region, ak, sk, url, bodyJson, projectId);
            if (!resMsg.isSuccess())
            {
                System.out.println("Create group fail: " + resMsg.getStatusCode());
            }
        }
        else
        {
            System.out.println("Miss the required groups body.");
        }

        return resMsg;
    }

    /**
     * Get All Groups
     * GET /v1.0/{project_id}/queues/{queue_id}/groups
     *
     * @param qId               The Queue Id
     * @param projectId         Your Project ID
     * @param dmsUrl            The DMS Request URL
     * @param serviceName       Service Name
     * @param region            Region
     * @param ak                Your Access Key ID
     * @param sk                Your Secret Access Key
     * @return ResponseMessage  The response content
     */
    public static ResponseMessage getGroups(String qId, String projectId, String dmsUrl, String serviceName,
            String region, String ak, String sk)
    {
        ResponseMessage resMsg;

        String url = dmsUrl + projectId + Constants.SYMBOL_SLASH + Constants.DMS_SERVICE_QUEUES
                + Constants.SYMBOL_SLASH + qId + Constants.SYMBOL_SLASH + Constants.DMS_SERVICE_GROUPS;
        resMsg = AccessServiceUtils.get(serviceName, region, ak, sk, url, projectId);
        if (!resMsg.isSuccess())
        {
            System.out.println("Get all groups fail: " + resMsg.getStatusCode());
        }
        return resMsg;
    }

    /**
     * Delete Group
     * DELETE /v1.0/{project_id}/queues/{queue_id}/groups/{consumer_group_id}
     *
     * @param groupId           The consume group id which require to delete
     * @param qId               The Queue Id which require to delete
     * @param projectId         Your Project ID
     * @param rootUrl            The DMS Request URL
     * @param serviceName       Service Name
     * @param region            Region
     * @param ak                Your Access Key ID
     * @param sk                Your Secret Access Key
     * @return ResponseMessage  The response content
     */
    public static ResponseMessage deleteGroup(String groupId, String qId, String projectId, String rootUrl,
            String serviceName, String region, String ak, String sk)
    {
        ResponseMessage resMsg;

        String url = rootUrl + projectId + Constants.SYMBOL_SLASH + Constants.DMS_SERVICE_QUEUES
                + Constants.SYMBOL_SLASH + qId + Constants.SYMBOL_SLASH + Constants.DMS_SERVICE_GROUPS
                + Constants.SYMBOL_SLASH + groupId;
        resMsg = AccessServiceUtils.delete(serviceName, region, ak, sk, url, projectId);
        if (resMsg.isSuccess())
        {
            System.out.println("Delete group '" + groupId + "' success.");
        }
        else
        {
            System.out.println("Delete group '" + groupId + "' fail: " + resMsg.getStatusCode());
        }
        return resMsg;
    }

    /**
     * Send Message
     * POST /v1.0/{project_id}/queues/{queue_id}/messages
     *
     * @param messagesJson           messagesJson  Json String
     * @param qId               The Queue Id
     * @param projectId         Your Project ID
     * @param dmsUrl            The DMS Request URL
     * @param serviceName       Service Name
     * @param region            Region
     * @param ak                Your Access Key ID
     * @param sk                Your Secret Access Key
     * @return ResponseMessage  The response content
     */
    public static ResponseMessage sendMessages(String messagesJson, String qId, String projectId, String dmsUrl,
            String serviceName, String region, String ak, String sk)
    {
        ResponseMessage resMsg = new ResponseMessage();

        String url = dmsUrl + projectId + Constants.SYMBOL_SLASH + Constants.DMS_SERVICE_QUEUES
                + Constants.SYMBOL_SLASH + qId + Constants.SYMBOL_SLASH + Constants.DMS_SERVICE_MESSAGES;

        if (messagesJson != null && !"".equals(messagesJson))
        {
            resMsg = AccessServiceUtils.post(serviceName, region, ak, sk, url, messagesJson, projectId);
            if (!resMsg.isSuccess())
            {
                System.out.println("Send message fail: " + resMsg.getStatusCode());
            }
            else
            {
                System.out.println("Send message success!");
            }
        }
        else
        {
            System.out.println("Miss the required message body.");
        }
        return resMsg;
    }

    /**
     * Consume Message
     * GET /v1.0/{project_id}/queues/{queue_id}/groups/{consumer_group_id}/messages
     *
     * @param qId               The Queue Id
     * @param qGroupId          Consume group Id
     * @param msgLimit          number of consumed message
     * @param projectId         Your Project ID
     * @param dmsUrl            The DMS Request URL
     * @param serviceName       Service Name
     * @param region            Region
     * @param ak                Your Access Key ID
     * @param sk                Your Secret Access Key
     * @return ResponseMessage  The response content
     */
    public static ResponseMessage consumeMessages(String qId, String qGroupId, int msgLimit,
            String projectId, String dmsUrl, String serviceName, String region, String ak, String sk)
    {
        ResponseMessage resMsg;

        String url = dmsUrl + projectId + Constants.SYMBOL_SLASH + Constants.DMS_SERVICE_QUEUES
                + Constants.SYMBOL_SLASH + qId + Constants.SYMBOL_SLASH + Constants.DMS_SERVICE_GROUPS
                + Constants.SYMBOL_SLASH + qGroupId + Constants.SYMBOL_SLASH + Constants.DMS_SERVICE_MESSAGES
                + Constants.SYMBOL_QUES_MARK + Constants.DMS_SERVICE_LIMIT + Constants.SYMBOL_EQUAL + msgLimit;
        resMsg = AccessServiceUtils.get(serviceName, region, ak, sk, url, projectId);
        if (!resMsg.isSuccess())
        {
            System.out.println("Consume message fail: " + resMsg.getStatusCode());
        }
        return resMsg;
    }

    /**
     * Acknowledge The Consumed Message
     * POST /v1.0/{project_id}/queues/{queue_id}/groups/{consumer_group_id}/ack
     *
     * @param msgIds            The collection msg ids which confirmed consumed
     * @param qGroupId          Consume group Id
     * @param qId               The Queue Id
     * @param projectId         Your Project ID
     * @param dmsUrl            The DMS Request URL
     * @param serviceName       Service Name
     * @param region            Region
     * @param ak                Your Access Key ID
     * @param sk                Your Secret Access Key
     * @return ResponseMessage  The response content
     */
    public static ResponseMessage acknowledgeMessages(ArrayList<String> msgIds, String qGroupId, String qId,
            String projectId, String dmsUrl,
            String serviceName, String region, String ak, String sk)
    {
        ResponseMessage resMsg = new ResponseMessage();
        String url = dmsUrl + projectId + Constants.SYMBOL_SLASH + Constants.DMS_SERVICE_QUEUES
                + Constants.SYMBOL_SLASH + qId + Constants.SYMBOL_SLASH + Constants.DMS_SERVICE_GROUPS
                + Constants.SYMBOL_SLASH + qGroupId + Constants.SYMBOL_SLASH + Constants.DMS_SERVICE_ACK;
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode messagesArray = mapper.createArrayNode();
        for (String msgId : msgIds)
        {
            ObjectNode msgNode = mapper.createObjectNode();
            msgNode.put(Constants.DMS_SERVICE_MESSAGE_ID, msgId);
            msgNode.put("status", "success");
            messagesArray.add(msgNode);
        }

        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.putPOJO(Constants.DMS_SERVICE_MESSAGE, messagesArray);

        String bodyJson = null;
        try
        {
            bodyJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
        }
        catch (JsonProcessingException e)
        {
            e.printStackTrace();
        }
        if (bodyJson != null)
        {
            resMsg = AccessServiceUtils.post(serviceName, region, ak, sk, url, bodyJson, projectId);
            if (!resMsg.isSuccess())
            {
                System.out.println("Acknowledge Message fail: " + resMsg.getStatusCode());
            }
        }
        else
        {
            System.out.println("Miss the required message body.");
        }
        return resMsg;
    }

    /**
     *  Get Quota
     *  GET /v1.0/quotas/{project_id}/mqs
     *
     * @param projectId     Your Project ID
     * @param dmsUrl    The DMS Request URL
     * @param serviceName   Service Name
     * @param region    Region
     * @param ak    Your Access Key ID
     * @param sk    Your Secret Access Key
     * @return ResponseMessage  The response content
     */
    public static ResponseMessage getQuota(String projectId, String dmsUrl, String serviceName, String region,
            String ak, String sk)
    {
        ResponseMessage resMsg;

        String url = dmsUrl + projectId + Constants.SYMBOL_SLASH + Constants.DMS_SERVICE_QUOTAS
                + Constants.SYMBOL_SLASH + Constants.DMS_SERVICE_DMS;
        resMsg = AccessServiceUtils.get(serviceName, region, ak, sk, url, projectId);
        if (!resMsg.isSuccess())
        {
            System.out.println("Get quota fail: " + resMsg.getStatusCode());
        }
        return resMsg;
    }

    /**
     * Construct temp message
     * @param numMsg    The number of messages
     * @return String   The response bodyJson content
     */
    public static String constructTempMessages(int numMsg)
    {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode topObjNode = mapper.createObjectNode();
        ArrayNode msgArray = mapper.createArrayNode();

        for (int i = 0; i < numMsg; i++)
        {
            //attribute Node
            ObjectNode attributesNode = mapper.createObjectNode();
            attributesNode.put(Constants.KEY_NAME, "ignore");
            attributesNode.put(Constants.DMS_SERVICE_TYPE, "string.start");
            attributesNode.put(Constants.DMS_SERVICE_VALUE, "pink");

            //message node
            ObjectNode msgObjectNode = mapper.createObjectNode();
            msgObjectNode.put(Constants.DMS_SERVICE_BODY, "msg_body_" + System.currentTimeMillis());
            msgObjectNode.putPOJO(Constants.DMS_SERVICE_ATTRIBUTES, attributesNode);
            msgArray.add(msgObjectNode);
        }

        topObjNode.put(Constants.DMS_SERVICE_MESSAGES, msgArray);
        String bodyJson = "";
        try
        {
            bodyJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(topObjNode);
        }
        catch (JsonProcessingException e)
        {
            e.printStackTrace();
        }
        return bodyJson;
    }

    /**
     * Parse Queue Id
     * @param createQueueResMsg     the response content after create queue
     * @return queueId  queue Id
     */
    public static String parseQueueId(ResponseMessage createQueueResMsg)
    {
        String queueId = "";
        if (createQueueResMsg.isSuccess() && createQueueResMsg.getBody() != null)
        {
            // parse out queue id
            try
            {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readTree(createQueueResMsg.getBody());
                if (jsonNode.has("id"))
                {
                    queueId = jsonNode.get("id").textValue();
                    if ("".equals(queueId.trim()))
                    {
                        System.out.println("Can not get queue id from response body");
                    }
                }
                else
                {
                    System.out.println("Can not get queue id from response body");
                }
            }
            catch (JsonProcessingException e)
            {
                System.out.println("Catch JsonProcessingException when create queue!");
                e.printStackTrace();
            }
            catch (IOException e)
            {
                System.out.println("Catch IOException when create queue!");
                e.printStackTrace();
            }
        }
        return queueId;
    }

    /**
     * Parse Queue Group Id
     * @param createGroupResMsg     the response content after create group
     * @return queueGroupId queue group Id
     */
    public static String parseQueueGroupId(ResponseMessage createGroupResMsg)
    {
        String queueGroupId = "";
        if (createGroupResMsg.isSuccess() && createGroupResMsg.getBody() != null)
        {
            try
            {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readTree(createGroupResMsg.getBody());
                if (jsonNode.has("groups"))
                {
                    JsonNode firstGroupNode = jsonNode.get("groups").get(0);
                    queueGroupId = firstGroupNode.get("id").textValue();
                    if ("".equals(queueGroupId.trim()))
                    {
                        System.out.println("Can not get group id from response body");
                    }
                }
                else
                {
                    System.out.println("Can not get group id from response body");
                }
            }
            catch (JsonProcessingException e)
            {
                System.out.println("Catch JsonProcessingException when create group fail!");
                e.printStackTrace();
            }
            catch (IOException e)
            {
                System.out.println("Catch IOException when create group fail!");
                e.printStackTrace();
            }
        }

        return queueGroupId;
    }

    /**
     * Parse Handler Ids
     * @param consumeMessagesResMsg     the response content after consume message
     * @return handlerIds   handler Ids
     */
    public static ArrayList<String> parseHandlerIds(ResponseMessage consumeMessagesResMsg)
    {
        ArrayList<String> handlerIds = new ArrayList<String>();
        if (consumeMessagesResMsg.isSuccess())
        {
            if (consumeMessagesResMsg.getBody() != null)
            {
                try
                {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode jsonArrayNode = mapper.readTree(consumeMessagesResMsg.getBody());
                    if (jsonArrayNode.isArray())
                    {
                        for (final JsonNode objNode : jsonArrayNode)
                        {
                            String handlerId = objNode.findValue("handler").textValue().toString();
                            if (!handlerIds.contains(handlerId))
                            {
                                handlerIds.add(handlerId);
                            }
                        }
                        if (handlerIds.size() <= 0)
                        {
                            System.out.println(
                                    "Warning!! Can not get handler ids from response body, "
                                            + "the ack message will be ignored");
                        }
                    }
                }
                catch (JsonProcessingException e)
                {
                    System.out.println("Catch JsonProcessingException when create group fail!");
                    e.printStackTrace();
                }
                catch (IOException e)
                {
                    System.out.println("Catch IOException when create group fail!");
                    e.printStackTrace();
                }

            }
        }
        return handlerIds;
    }

}
