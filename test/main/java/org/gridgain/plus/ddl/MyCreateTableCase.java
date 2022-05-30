package org.gridgain.plus.ddl;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.junit.Test;

public class MyCreateTableCase {
    @Test
    public void test_init_1() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        CacheConfiguration<?, ?> template_cfg = new CacheConfiguration<>("MyMeta_template*").setSqlSchema("MY_META");
        template_cfg.setCacheMode(CacheMode.REPLICATED);
        template_cfg.setReadFromBackup(true);
        ignite.addCacheConfiguration(template_cfg);

        //CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>("public_meta").setSqlSchema("PUBLIC");
        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>("my_meta_table").setSqlSchema("MY_META");
        IgniteCache cache = ignite.getOrCreateCache(cacheCfg);

        //ignite.configuration().isMyLogEnabled();

        String sql = "DROP TABLE IF EXISTS my_meta_tables";
        cache.query(new SqlFieldsQuery(sql)).getAll();

        sql = "CREATE TABLE IF NOT EXISTS my_meta_tables (" +
                "                id BIGINT," +
                "                table_name VARCHAR(50)," +
                "                descrip VARCHAR," +
                "                PRIMARY KEY (id)" +
                ") WITH \"template=MyMeta_template,cache_name=my_meta_tables,VALUE_TYPE=cn.plus.model.ddl.MyTable,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";";
        cache.query(new SqlFieldsQuery(sql)).getAll();

        sql = "DROP TABLE IF EXISTS table_item";
        cache.query(new SqlFieldsQuery(sql)).getAll();

        sql = "CREATE TABLE IF NOT EXISTS table_item (" +
                "                id BIGINT," +
                "                column_name VARCHAR(50)," +
                "                column_len INT," +
                "                scale INT," +
                "                column_type VARCHAR(50)," +
                "                not_null BOOLEAN DEFAULT true," +
                "                pkid BOOLEAN DEFAULT false," +
                "                comment VARCHAR(50)," +
                "                auto_increment BOOLEAN DEFAULT false," +
                "                table_id BIGINT," +
                "                PRIMARY KEY (id, table_id)" +
                ") WITH \"template=MyMeta_template,cache_name=table_item,affinityKey=table_id,KEY_TYPE=cn.plus.model.ddl.MyTableItemPK,VALUE_TYPE=cn.plus.model.ddl.MyTableItem,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";";
        cache.query(new SqlFieldsQuery(sql)).getAll();
    }
}
