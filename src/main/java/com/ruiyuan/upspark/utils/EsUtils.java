package com.ruiyuan.upspark.utils;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsUtils {

    private String hostsAndPorts;
    private String index;
    private String type;

    private final Logger logger = LoggerFactory.getLogger(EsUtils.class);

    private RestHighLevelClient client;

    public EsUtils() {
        super();
        HashMap configs = new YamlUtil().yamlRead();
        this.hostsAndPorts = configs.get("esHostports").toString();
        this.index = configs.get("esindex").toString();
        this.type = configs.get("estype").toString();
        if (null == client) {
            client = getClient();
        }

    }

    /**
     * 获取客户端方法
     *
     * @return 创建好的客户端
     */
    private RestHighLevelClient getClient() {

        List<HttpHost> httpHosts = new ArrayList<HttpHost>();
        if (hostsAndPorts.length() > 0) {
            for (String hostsAndPort : hostsAndPorts.split(",")) {
                String[] hp = hostsAndPort.split(":");
                httpHosts.add(new HttpHost(hp[0], Integer.valueOf(hp[1]), "http"));
            }
            client = new RestHighLevelClient(RestClient.builder(httpHosts.toArray(new HttpHost[0])));
            logger.info("连接es服务器：" + hostsAndPorts);
        } else {
            client = new RestHighLevelClient(RestClient.builder(new HttpHost("127.0.0.1", 9200, "http")));
            logger.info("连接本地es服务器");
        }
        return client;
    }

    /**
     * es客户端关闭
     */
    public void clientClose() {

        try {
            this.client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 判断id是否存在
     *
     * @param id es文档的id（即对象的编码）
     * @return 是或否
     * @throws Exception
     */
    public boolean existsId(String id) throws Exception {

        GetRequest request = new GetRequest(index, type, id);
        request.fetchSourceContext(new FetchSourceContext(false)).storedFields("_none_");
        return client.exists(request, RequestOptions.DEFAULT);
    }

    /**
     * 索引一个新的识别对象
     *
     * @param id   对象的编码
     * @param json 识别对象的json
     * @return 1表示索引成功
     * @throws Exception
     */
    public int createObjIndex(String id, String json) throws Exception {

        IndexRequest request = new IndexRequest(index, type, id);
        request.source(json, XContentType.JSON).opType(DocWriteRequest.OpType.CREATE);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            return 1;
        }
        return 0;
    }

    /**
     * 批量索引
     *
     * @param resES 识别对象的json列表
     * @return 1表示索引成功
     * @throws Exception
     */
    public void bulkCreateObjIndex(List<String> resES) throws Exception {
        BulkRequest brequest = new BulkRequest(this.index);

        resES.forEach(res -> {
            brequest.add(new IndexRequest().source(res, XContentType.JSON));
        });
        ActionListener<BulkResponse> bulkResponseActionListener = new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                System.out.println(bulkItemResponses.buildFailureMessage());
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }

        };
        client.bulkAsync(brequest, RequestOptions.DEFAULT, bulkResponseActionListener);
    }


    /**
     * 从ES中获取某个对象的某个字段
     *
     * @param id    对象的id
     * @param field 要返回的字段
     * @return 返回字段的json
     * @throws Exception
     */
    public String getObjIndexField(String id, String field) throws Exception {
        String[] includes = new String[]{field};
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, Strings.EMPTY_ARRAY);
        GetRequest getRequest = new GetRequest(index, type, id);
        getRequest.fetchSourceContext(fetchSourceContext);
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        String res = getResponse.getSourceAsString();
        return res;

    }

    /**
     * 删除目标
     *
     * @param id
     * @return
     */
    public int deleteObj(String id) throws Exception {

        logger.info("删除ES索引" + id);
        DeleteRequest request = new DeleteRequest(index, type, id);
        request.timeout(TimeValue.timeValueSeconds(30));
        DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);

        // 文档不存在
        /**
         if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
         throw new Exception("对象"+id+"不存在");
         }
         */
        if (deleteResponse.getResult() == DocWriteResponse.Result.DELETED) {
            return 1;
        } else {
            return 0;
        }
    }


    public boolean updateByQuery(String MiopictureURL, String SeqpictureURL) throws IOException {
        UpdateByQueryRequest updateByQuery = new UpdateByQueryRequest(index);
        //设置分片并行
        //updateByQuery.setSlices(2);
        //设置版本冲突时继续执行
        updateByQuery.setConflicts("proceed");
        //设置更新完成后刷新索引 ps很重要如果不加可能数据不会实时刷新
        updateByQuery.setRefresh(true);
        //查询条件如果是and关系使用must 如何是or关系使用should
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("pictureURL", MiopictureURL));
        //.should(QueryBuilders.termQuery("name", "张三"));
        //System.out.println(boolQueryBuilder);
        updateByQuery.setQuery(boolQueryBuilder);
        //设置要修改的内容可以多个值多个用；隔开
        HashMap<String, Object> params = new HashMap<>();
        params.put("pictureURL", SeqpictureURL);
        updateByQuery.setScript(new Script(ScriptType.INLINE,
                "painless",
                "ctx._source.pictureURL ="+ "'"+SeqpictureURL+"'", params));


        BulkByScrollResponse response = client.
                updateByQuery(updateByQuery, RequestOptions.DEFAULT);
        return response.getStatus().getUpdated() > 0 ? true : false;

    }
    public Object updateOneESData3(String pURL, String NEWpURL) {

        // UpdateByQueryRequest updateByQueryRequest, RequestOptions options
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.should(QueryBuilders.prefixQuery("pictureURL", pURL));
        //boolQueryBuilder.must(QueryBuilders.matchQuery("ip","192"));
        // boolQueryBuilder.must(QueryBuilders.matchQuery("method","createMethod"));

        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(index);
        /** 需要更新的数据 */
        Map<String, Object> paramsMap = new HashMap<>();
        paramsMap.put("pictureURL", NEWpURL);
        ScriptType type = ScriptType.INLINE;
        String lang = "painless";
        StringBuilder script = new StringBuilder();
        script.append("ctx._source").append("['pictureURL']").append("=").append(NEWpURL);
        String idOrCode = script.toString();
        //System.out.println(idOrCode);
        /** idOrCode 是执行的命令  */
        updateByQueryRequest.setScript(new Script(type, lang, "ctx._source.pictureURL ="+ "'"+NEWpURL+"'", paramsMap));

        /**   默认情况下，版本冲突会中止UpdateByQueryRequest进程  //设置版本冲突时继续 */
        updateByQueryRequest.setConflicts("proceed");
        /** 查询文档  */
        updateByQueryRequest.setQuery(boolQueryBuilder);
        /** 单次处理1000个文档 */
        //updateByQueryRequest.setBatchSize(1000);

        BulkByScrollResponse bulkByScrollResponse = null;
        try {
            bulkByScrollResponse = client.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bulkByScrollResponse;
    }


}
