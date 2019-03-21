package com.cloud.dms.access;

import com.cloud.sdk.http.HttpMethodName;
import org.apache.http.HttpResponse;

import java.io.InputStream;
import java.net.URL;
import java.util.Map;

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
public abstract class AccessService
{

    protected String serviceName = null;

    protected String region = null;

    protected String ak = null;

    protected String sk = null;

    /**
     * <默认构造函数>
     * @param serviceName serviceName
     * @param region region
     * @param ak ak
     * @param sk sk
     */
    public AccessService(String serviceName, String region, String ak, String sk)
    {
        this.region = region;
        this.serviceName = serviceName;
        this.ak = ak;
        this.sk = sk;
    }

    /**
     * Access DMS Service,create trust connection
     *
     * @param url                The DMS Request full URL
     * @param header            Http request header
     * @param content            The sending content to DMS service server
     * @param contentLength        The length of sending content
     * @param httpMethod        The method type http: like get, put, delete, post.
     * @throws Exception Exception
     * @return Response Http content
     */
    public abstract HttpResponse access(URL url, Map<String, String> header, InputStream content, Long contentLength,
            HttpMethodName httpMethod) throws Exception;

    /**
     * Access DMS Service,create trust connection
     *
     * @param url                The DMS Request full URL
     * @param header            Http request header
     * @param httpMethod        The method type http: like get, put, delete, post.
     * @throws Exception Exception
     * @return Response Http content
     */
    public HttpResponse access(URL url, Map<String, String> header, HttpMethodName httpMethod) throws Exception
    {
        return this.access(url, header, null, 0l, httpMethod);
    }

    /**
     * <一句话功能简述>
     * <功能详细描述>
     * @param url url
     * @param content content
     * @param contentLength contentLength
     * @param httpMethod httpMethod
     * @return HttpResponse HttpResponse
     * @throws Exception Exception
     * @see [类、类#方法、类#成员]
     */
    public HttpResponse access(URL url, InputStream content, Long contentLength,
            HttpMethodName httpMethod) throws Exception
    {
        return this.access(url, null, content, contentLength, httpMethod);
    }

   /**
    * <一句话功能简述>
    * <功能详细描述>
    * @param url url
    * @param httpMethod httpMethod
    * @return HttpResponse HttpResponse
    * @throws Exception Exception
    * @see [类、类#方法、类#成员]
    */
    public HttpResponse access(URL url, HttpMethodName httpMethod) throws Exception
    {
        return this.access(url, null, null, 0l, httpMethod);
    }
    /**
     * <一句话功能简述>
     * <功能详细描述>
     * @see [类、类#方法、类#成员]
     */
    public abstract void close();

    public String getServiceName()
    {
        return serviceName;
    }

}
