package org.gridgain.db;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.junit.Test;
import org.tools.MyTools;

public class MyScenes_case {

    @Test
    public void show() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        IgniteCache<?, ?> cache;

        CacheConfiguration<?, ?> template_cfg = new CacheConfiguration<>("MyMeta_template*").setSqlSchema("MY_META");
        template_cfg.setCacheMode(CacheMode.REPLICATED);
        template_cfg.setReadFromBackup(true);
        ignite.addCacheConfiguration(template_cfg);

        //CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>("public_meta").setSqlSchema("PUBLIC");
        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>("my_meta_table").setSqlSchema("MY_META");

        cache = ignite.getOrCreateCache(cacheCfg);

        String sql = "DROP TABLE IF EXISTS my_scenes";
        cache.query(new SqlFieldsQuery(sql)).getAll();
        
        sql = "CREATE TABLE IF NOT EXISTS my_scenes (" +
                "                  scenes_name VARCHAR(40)," +
                "                  scenes_code VARCHAR," +
                "                  ps_code VARCHAR," +
                "                  descrip VARCHAR," +
                "                  version VARCHAR(20)," +
                "                  is_batch BOOLEAN DEFAULT false," +
                "                  active BOOLEAN DEFAULT false," +
                "                  PRIMARY KEY (scenes_name)" +
                "                ) WITH \"template=MyMeta_template,VALUE_TYPE=cn.plus.model.db.MyScenesCache,cache_name=my_scenes,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";";
        cache.query(new SqlFieldsQuery(sql)).getAll();
    }
}

















































































