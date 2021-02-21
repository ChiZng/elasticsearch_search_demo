package com.zchi.search;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.ElasticSearchDruidDataSourceFactory;
import com.alibaba.fastjson.JSON;
import com.zchi.search.entity.es.StuEntity;
import com.zchi.search.repository.StuEntityRepository;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = SearchApplication.class)
public class EsTest {

    @Autowired
    private ElasticsearchRestTemplate elasticsearchRestTemplate;

    @Autowired
    private StuEntityRepository stuEntityRepository;

    @Test
    public void createIndexStu() {
        StuEntity stu = new StuEntity();
        stu.setStuId(1005L);
        stu.setName("iron man");
        stu.setAge(54);
        stu.setSign("I am iron man");
        stu.setDescription("I have a iron army");

        stuEntityRepository.save(stu);
    }

    @Test
    public void deleteIndexStu() {
        StuEntity stu = new StuEntity();
        stu.setStuId(1L);
        stu.setName("iron man");
        stu.setAge(54);
        stu.setSign("I am iron man");
        stu.setDescription("I have a iron army");
        stuEntityRepository.deleteById(1005L);
    }

    @Test
    public void add() {
        StuEntity stu = new StuEntity();
        stu.setStuId(1005L);
        stu.setName("iron man");
        stu.setAge(54);
        stu.setSign("I am iron man");
        stu.setDescription("I have a iron army");
        elasticsearchRestTemplate.save(stu);
    }

    @Test
    public void update() {
        StuEntity stu = new StuEntity();
        stu.setStuId(1005L);
        stu.setName("iron manss");
        stu.setAge(100);
        stu.setSign("I am iron man");
        stu.setDescription("I have a iron army");
        System.out.println(JSON.toJSONString(stu));
        // 创建Document对象
        // 第一种方式
        Document document = Document.create();
        // 将修改的内容塞进去
        document.putAll(JSON.parseObject(JSON.toJSONString(stu), Map.class));

        // 第二种方式
        Document document1 = Document.parse(JSON.toJSONString(stu));

        // 第三种方式
        Document document2 = Document.from(JSON.parseObject(JSON.toJSONString(stu), Map.class));

        // 构造updateQuery
        UpdateQuery updateQuery = UpdateQuery.builder("1")
                .withDocAsUpsert(true)
                .withDocument(Document.parse(JSON.toJSONString(stu)))
        .build();
        elasticsearchRestTemplate.update(updateQuery, IndexCoordinates.of("stu"));
    }

    @Test
    public void delete() {
        StuEntity stu = new StuEntity();
        stu.setStuId(1005L);
        stu.setName("iron man");
        stu.setAge(54);
        stu.setSign("I am iron man");
        stu.setDescription("I have a iron army");
        elasticsearchRestTemplate.delete(stu);
    }

    @Test
    public void search1() {
        Criteria criteria = new Criteria("name").is("iron man");
        Query query = new CriteriaQuery(criteria);
        SearchHits searchHits = elasticsearchRestTemplate.search(query, StuEntity.class);
        System.out.println(searchHits.getSearchHits());
    }

    @Test
    public void search2() {
        Query query = new StringQuery("{\n" +
                "    \"match\": { \n" +
                "      \"age\": { \"query\": \"54\" } \n" +
                "    } \n" +
                "  }");
        SearchHits<StuEntity> searchHits = elasticsearchRestTemplate.search(query, StuEntity.class);
        System.out.println(searchHits.getSearchHits());
    }

    @Test
    public void search3() {
        Query query = new NativeSearchQueryBuilder()
//                .addAggregation(terms("names").field("name").size(10)) //
                .withQuery(QueryBuilders.matchQuery("age", "54"))
                .build();

        SearchHits<StuEntity> searchHits = elasticsearchRestTemplate.search(query, StuEntity.class);
        System.out.println(searchHits.getSearchHits());
    }

    @Test
    public void search4() throws Exception {
        Properties properties = new Properties();
        properties.put("url", "jdbc:elasticsearch://192.168.1.123:9300/");
        DruidDataSource dds = (DruidDataSource) ElasticSearchDruidDataSourceFactory.createDataSource(properties);
        Connection connection = dds.getConnection();
        PreparedStatement ps = connection.prepareStatement("SELECT * from stu");
        ResultSet resultSet = ps.executeQuery();
        ps.close();
        connection.close();
        dds.close();
    }

//    @Test
//    public void search5() throws Exception {
//        Properties properties = new Properties();
//        properties.put("url", "jdbc:elasticsearch://192.168.1.123:9300/");
//        DruidDataSource dds = (DruidDataSource) ElasticSearchDruidDataSourceFactory.createDataSource(properties);
//        NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(dds);
//        String sql = "select * from stu where age = :age";
//        HashMap<String, String> param = new HashMap<>();
//        param.put("age", "54");
//        List<Map<String, Object>> resultList = jdbcTemplate.queryForList(sql, param);
//        System.out.println(resultList);
//        dds.close();
//    }


}
