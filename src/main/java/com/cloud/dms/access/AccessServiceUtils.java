package com.cloud.dms.access;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.http.HttpResponse;

import com.cloud.dms.ResponseMessage;
import com.cloud.sdk.http.HttpMethodName;

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
public class AccessServiceUtils
{

    private static final int CODE_300 = 300;

    private static final int CODE_200 = 200;

    /**
     * DMS HTTP PUT Method
     *
     * @param serviceName       Service Name
     * @param region            Region
     * @param ak                Your Access Key ID
     * @param sk                Your Secret Access Key
     * @param requestUrl        DMS Request full URL
     * @param putBody            The body content of sending to DMS service
     * @return ResponseMessage    The response content
     */
    public static ResponseMessage put(String serviceName, String region, String ak, String sk, String requestUrl,
            String putBody)
    {
        ResponseMessage resMsg = new ResponseMessage();
        AccessService accessService = null;
        try
        {
            accessService = new AccessServiceImpl(serviceName, region, ak, sk);
            URL url = new URL(requestUrl);
            HttpMethodName httpMethod = HttpMethodName.PUT;

            InputStream content = new ByteArrayInputStream(putBody.getBytes());
            HttpResponse response = accessService.access(url, content, (long)putBody.getBytes().length, httpMethod);

            if (response.getEntity() != null && response.getEntity().getContent() != null)
            {
                resMsg.setBody(convertStreamToString(response.getEntity().getContent()));
            }

            int statusCode = response.getStatusLine().getStatusCode();
            resMsg.setStatusCode(statusCode);
            if (statusCode >= CODE_200 && statusCode < CODE_300)
            {
                resMsg.setSuccess(true);
            }

            System.out.println("Response Status Code: " + response.getStatusLine().getStatusCode());
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (accessService != null)
            {
                accessService.close();
            }
        }
        return resMsg;
    }

    /**
     * DMS HTTP DELETE
     *
     * @param serviceName   Service Name
     * @param region    Region
     * @param ak    Your Access Key ID
     * @param sk    Your Secret Access Key
     * @param requestUrl    DMS Request full URL
     * @return ResponseMessage  The response content
     */
    public static ResponseMessage delete(String serviceName, String region, String ak, String sk, String requestUrl, String projectId)
    {
        ResponseMessage resMsg = new ResponseMessage();
        Map<String, String> header = new HashMap<String, String>();
        header.put("x-project-id", projectId);
        AccessService accessService = null;
        try
        {
            accessService = new AccessServiceImpl(serviceName, region, ak, sk);
            URL url = new URL(requestUrl);
            HttpMethodName httpMethod = HttpMethodName.DELETE;

            HttpResponse response = accessService.access(url, header, httpMethod);
            if (response.getEntity() != null && response.getEntity().getContent() != null)
            {
                resMsg.setBody(convertStreamToString(response.getEntity().getContent()));
            }

            int statusCode = response.getStatusLine().getStatusCode();
            resMsg.setStatusCode(statusCode);
            if (statusCode >= CODE_200 && statusCode < CODE_300)
            {
                resMsg.setSuccess(true);
            }

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (accessService != null)
            {
                accessService.close();
            }
        }
        return resMsg;
    }

    /**
     * DMS HTTP GET Method
     *
     * @param serviceName        Service Name
     * @param region            Region
     * @param ak                Your Access Key ID
     * @param sk                Your Secret Access Key
     * @param requestUrl        The DMS Request full URL
     * @return ResponseMessage    The response content
     */
    public static ResponseMessage get(String serviceName, String region, String ak, String sk, String requestUrl, String projectId)
    {
        ResponseMessage resMsg = new ResponseMessage();
        Map<String, String> header = new HashMap<String, String>();
        header.put("x-project-id", projectId);
        AccessService accessService = null;
        try
        {
            accessService = new AccessServiceImpl(serviceName, region, ak, sk);
            URL url = new URL(requestUrl);
            HttpMethodName httpMethod = HttpMethodName.GET;
            HttpResponse response = accessService.access(url, header, httpMethod);
            if (response.getEntity() != null && response.getEntity().getContent() != null)
            {
                resMsg.setBody(convertStreamToString(response.getEntity().getContent()));
            }

            int statusCode = response.getStatusLine().getStatusCode();
            resMsg.setStatusCode(statusCode);
            if (statusCode >= CODE_200 && statusCode < CODE_300)
            {
                resMsg.setSuccess(true);
            }

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (accessService != null)
            {
                accessService.close();
            }
        }
        return resMsg;
    }

    /**
     * DMS HTTP POST Method
     *
     * @param serviceName        Service Name
     * @param region            Region
     * @param ak                Your Access Key ID
     * @param sk                Your Secret Access Key
     * @param requestUrl        The DMS Request full URL
     * @param postbody            The body content of sending to DMS service
     * @return ResponseMessage    The response content
     */
    public static ResponseMessage post(String serviceName, String region, String ak, String sk, String requestUrl,
            String postbody, String projectId)
    {
        ResponseMessage resMsg = new ResponseMessage();
        Map<String, String> header = new HashMap<String, String>();
        header.put("x-project-id", projectId);
        AccessService accessService = new AccessServiceImpl(serviceName, region, ak, sk);
        URL url = null;
        try
        {
            url = new URL(requestUrl);
        }
        catch (MalformedURLException e)
        {
            e.printStackTrace();
        }
        InputStream content = new ByteArrayInputStream(postbody.getBytes());
        HttpMethodName httpMethod = HttpMethodName.POST;
        try
        {
            HttpResponse response = accessService.access(url, header, content, (long)postbody.getBytes().length, httpMethod);
            if (response.getEntity() != null && response.getEntity().getContent() != null)
            {
                resMsg.setBody(convertStreamToString(response.getEntity().getContent()));
            }

            int statusCode = response.getStatusLine().getStatusCode();
            resMsg.setStatusCode(statusCode);
            if (statusCode >= CODE_200 && statusCode < CODE_300)
            {
                resMsg.setSuccess(true);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            accessService.close();
        }
        return resMsg;
    }

    /**
     *
     * Convert Http response steam to String
     * @param is        InputStream
     * @return The converted String
     */
    private static String convertStreamToString(InputStream is)
    {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        StringBuilder sb = new StringBuilder();

        String line = null;
        try
        {
            while ((line = reader.readLine()) != null)
            {
                sb.append(line + "\n");
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            try
            {
                is.close();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        System.out.println(sb.toString());
        return sb.toString();
    }

    /**
     * Get Properties from loaded file
     * @param propsFile        File path
     * @return Properties    Properties Object
     */
    public static Properties getPropsFromFile(String propsFile)
    {
        Properties props = new Properties();
        if (propsFile == null)
        {
            return props;
        }
        FileInputStream propStream = null;
        try
        {
            propStream = new FileInputStream(propsFile);
            props.load(propStream);
        }
        catch (IOException e)
        {
            System.out.println("Couldn't load properties from " + propsFile);
        }
        finally
        {
            if (propStream != null)
            {
                try
                {
                    propStream.close();
                    propStream = null;
                }
                catch (IOException e)
                {

                    e.printStackTrace();
                }
            }
        }
        return props;
    }
}
