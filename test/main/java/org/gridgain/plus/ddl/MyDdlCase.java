package org.gridgain.plus.ddl;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.junit.Test;

public class MyDdlCase {

    @Test
    public void test_init_1() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyGridGain/my-grid-plus/resources/default-config.xml";
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
                "                data_set_id BIGINT," +
                "                PRIMARY KEY (id)" +
                ") WITH \"template=MyMeta_template,cache_name=my_meta_tables,VALUE_TYPE=cn.plus.model.ddl.MyTable,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";";
        cache.query(new SqlFieldsQuery(sql)).getAll();

        sql = "CREATE INDEX IF NOT EXISTS ot_ds_tname_idx ON my_meta_tables (table_name, data_set_id);";
        cache.query(new SqlFieldsQuery(sql)).getAll();

        sql = "CREATE INDEX IF NOT EXISTS my_meta_tables_idx ON my_meta_tables (table_name);";
        cache.query(new SqlFieldsQuery(sql)).getAll();

        sql = "DROP TABLE IF EXISTS my_dataset;";
        cache.query(new SqlFieldsQuery(sql)).getAll();

        sql = "CREATE TABLE IF NOT EXISTS my_dataset (" +
                "                  id BIGINT," +
                "                  is_real BOOLEAN DEFAULT true," +
                "                  dataset_name VARCHAR," +
                "                  PRIMARY KEY (id)" +
                "                ) WITH \"template=MyMeta_template,cache_name=my_dataset,VALUE_TYPE=cn.plus.model.ddl.MyDataSet,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";";
        cache.query(new SqlFieldsQuery(sql)).getAll();

        sql = "DROP TABLE IF EXISTS my_dataset_table;";
        cache.query(new SqlFieldsQuery(sql)).getAll();

        sql = "CREATE TABLE IF NOT EXISTS my_dataset_table (" +
                "                  id BIGINT," +
                "                  table_name VARCHAR," +
                "                  to_real BOOLEAN DEFAULT true," +
                "                  dataset_id BIGINT," +
                "                  PRIMARY KEY (id, dataset_id)" +
                "                ) WITH \"template=MyMeta_template,cache_name=my_dataset_table,KEY_TYPE=cn.plus.model.ddl.MyDatasetTablePK,VALUE_TYPE=cn.plus.model.ddl.MyDatasetTable,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";";
        cache.query(new SqlFieldsQuery(sql)).getAll();

        sql = "DROP TABLE IF EXISTS my_dataset_real_table;";
        cache.query(new SqlFieldsQuery(sql)).getAll();

        sql = "CREATE TABLE IF NOT EXISTS my_dataset_real_table (" +
                "                  id BIGINT," +
                "                  table_name VARCHAR," +
                "                  dataset_id BIGINT," +
                "                  PRIMARY KEY (id, dataset_id)" +
                "                ) WITH \"template=MyMeta_template,cache_name=my_dataset_real_table,KEY_TYPE=cn.plus.model.ddl.MyDatasetTablePK,VALUE_TYPE=cn.plus.model.ddl.MyDatasetRealTable,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";";
        cache.query(new SqlFieldsQuery(sql)).getAll();
    }
}
