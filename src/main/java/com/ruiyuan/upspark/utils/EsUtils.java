package com.ruiyuan.upspark.utils;

import org.apache.commons.collections.CollectionUtils;
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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
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
        String escape = EsUtils.escape(MiopictureURL);
        //System.out.println(MiopictureURL+SeqpictureURL);
        //设置分片并行
        //updateByQuery.setSlices(2);
        //设置版本冲突时继续执行
        updateByQuery.setConflicts("proceed");
        //设置更新完成后刷新索引 ps很重要如果不加可能数据不会实时刷新
        updateByQuery.setRefresh(true);
        //查询条件如果是and关系使用must 如何是or关系使用should
        FuzzyQueryBuilder boolQueryBuilder = QueryBuilders.fuzzyQuery("pictureURL", escape);
                //.filter(QueryBuilders.termQuery("pictureURL", MiopictureURL));
        //.should(QueryBuilders.termQuery("name", "张三"));
        //System.out.println(boolQueryBuilder);
        updateByQuery.setQuery(boolQueryBuilder);
        //设置要修改的内容可以多个值多个用；隔开
        HashMap<String, Object> params = new HashMap<>();
        params.put("pictureURL", SeqpictureURL);
        updateByQuery.setScript(new Script(ScriptType.INLINE,
                "painless",
                "ctx._source.pictureURL =" + "'" + SeqpictureURL + "'", params));


        BulkByScrollResponse response = client.
                updateByQuery(updateByQuery, RequestOptions.DEFAULT);
        return response.getStatus().getUpdated() > 0 ? true : false;

    }


    public  SearchHits searchsss(String MiopictureURL) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //关键字匹配
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("uav", MiopictureURL);
        //模糊匹配
        matchQueryBuilder.fuzziness(Fuzziness.AUTO);
        sourceBuilder.query(matchQueryBuilder);
        //第几页
        //sourceBuilder.from(start);
        //第几条
        //sourceBuilder.size(count);

        searchRequest.source(sourceBuilder);
        //匹配度从高到低
        ///sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));

        SearchResponse searchResponse = client.search(searchRequest,RequestOptions.DEFAULT);

        SearchHits hits = searchResponse.getHits();
        System.out.println("took:" + searchResponse.getTook());
        System.out.println("timeout:" + searchResponse.isTimedOut());
        System.out.println("total:" + hits.getTotalHits());
        System.out.println("MaxScore:" + hits.getMaxScore());
        for (SearchHit hit : hits) {
            //输出每条查询的结果信息
            System.out.println(hit.getSourceAsString());
        }
        return hits;
    }


    public void search(String MiopictureURL) throws IOException {

        // 创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices(index);

        // 构建查询的请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("pictureURL", MiopictureURL));
        request.source(sourceBuilder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 查询匹配
        SearchHits hits = response.getHits();
        System.out.println("took:" + response.getTook());
        System.out.println("timeout:" + response.isTimedOut());
        System.out.println("total:" + hits.getTotalHits());
        System.out.println("MaxScore:" + hits.getMaxScore());
        System.out.println("hits========>>");
        for (SearchHit hit : hits) {
            //输出每条查询的结果信息
            System.out.println(hit.getSourceAsString());
        }
        System.out.println("<<========");


    }

    public long updateByQuery2(String MiopictureURL, String SeqpictureURL) throws IOException {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
        updateByQueryRequest.indices(index);
        //搜索条件为id(因为插入时指定doc的id和实体类id一致，这样就保证了搜索结果唯一)
        //如果搜索条件查出的结果很多，使用需谨慎
        updateByQueryRequest.setQuery(new TermQueryBuilder("pictureURL", MiopictureURL));
        //map存储脚本实体参数值
        Map<String, Object> map = new HashMap<>();
        map.put("pictureURL", SeqpictureURL);
        //指定哪些字段需要更新,ctx._source.xxx为es的字段，使用map的值赋值更新
        updateByQueryRequest.setScript(new Script(ScriptType.INLINE,
                "painless",
                "ctx._source.pictureURL =" + "'" + SeqpictureURL + "'"
                , map));
        BulkByScrollResponse bulkByScrollResponse = client.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
        return bulkByScrollResponse.getStatus().getUpdated();
    }

    public static String escape(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' || c == '+' || c == '-' || c == '!' || c == '(' || c == ')' || c == ':'
                    || c == '^' || c == '[' || c == ']' || c == '\"' || c == '{' || c == '}' || c == '~'
                    || c == '*' || c == '?' || c == '|' || c == '&' || c == '/'|| c == '_'|| c == '.') {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.toString();
    }

}
