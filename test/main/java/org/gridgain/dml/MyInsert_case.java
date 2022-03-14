package org.gridgain.dml;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgnitionEx;
import org.junit.Test;
import org.tools.KvSql;
import org.tools.MyConvertUtil;

import java.util.ArrayList;

public class MyInsert_case {

    @Test
    public void insert_case() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyGridGain/my-grid-plus/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        IgniteCache<?, ?> cache = ignite.cache("public_meta");

        String sql = "DROP TABLE IF EXISTS OrderDetails";
        cache.query(new SqlFieldsQuery(sql)).getAll();

        sql = "CREATE TABLE OrderDetails (orderid INTEGER not null,productid INTEGER not null,unitprice DECIMAL(10,4) not null DEFAULT 0,quantity INTEGER(2) not null DEFAULT 1,discount DECIMAL not null DEFAULT 0, orderid_pk INTEGER,productid_pk INTEGER, PRIMARY KEY (orderid_pk,productid_pk)) WITH \"template=partitioned,backups=3,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_name=f_OrderDetails\"";
        cache.query(new SqlFieldsQuery(sql)).getAll();

        //sql = "DROP TABLE IF EXISTS OrderDetails_1";
        //cache.query(new SqlFieldsQuery(sql)).getAll();
    }

    @Test
    public void insert_case_1() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyGridGain/my-grid-plus/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        String cache_name = "f_OrderDetails";

        BinaryObjectBuilder keyBuilder = ignite.binary().builder(KvSql.getKeyType(ignite, cache_name));
        keyBuilder.setField("orderid_pk", 1);
        keyBuilder.setField("productid_pk", 1);
        BinaryObject keyObject = keyBuilder.build();

        BinaryObjectBuilder valueBuilder = ignite.binary().builder(KvSql.getValueType(ignite, cache_name));
        valueBuilder.setField("orderid", 1);
        valueBuilder.setField("productid", 1);
        valueBuilder.setField("orderid_pk", 1);
        valueBuilder.setField("productid_pk", 1);

        //valueBuilder.setField("unitprice", MyConvertUtil.ConvertToDecimal(2D));
        valueBuilder.setField("quantity", 1);
        //valueBuilder.setField("discount", MyConvertUtil.ConvertToDecimal(0.123D));

        BinaryObject valueObject = valueBuilder.build();

        ignite.cache(cache_name).put(keyObject, valueObject);

//        sql = "DROP TABLE IF EXISTS OrderDetails";
//        cache.query(new SqlFieldsQuery(sql)).getAll();
    }
}
