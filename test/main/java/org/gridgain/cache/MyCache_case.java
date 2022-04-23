package org.gridgain.cache;

import clojure.lang.PersistentHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgnitionEx;
import org.junit.Test;

/**
 * 测试 cache
 * */
public class MyCache_case {
    /**
     * 1、创建一个 cache 看是否使用内存模式
     * */
    @Test
    public void save() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>();
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cfg.setDataRegionName("40MB_Region_Eviction");
        cfg.setName("my_test_cache");
        cfg.setReadFromBackup(true);

        IgniteCache<Integer, String> cache = ignite.getOrCreateCache(cfg);
        cache.put(1, "a");
        cache.put(2, "a");
        cache.put(3, "a");

        System.out.println("OK");
    }

    @Test
    public void read() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        IgniteCache<Integer, String> cache = ignite.cache("my_test_cache");
        System.out.println(cache.get(2));

    }

    @Test
    public void destroy() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        IgniteCache<Integer, String> cache = ignite.cache("my_test_cache");
        cache.destroy();
        System.out.println("cache.destroy()");
        
        String txt = "" +
                "DROP TABLE IF EXISTS my_cron;" +
                "CREATE TABLE IF NOT EXISTS my_cron (" +
                "                  cron_name VARCHAR(40)," +
                "                  cron VARCHAR," +
                "                  descrip VARCHAR," +
                "                  PRIMARY KEY (scenes_name)" +
                "                ) WITH \"template=MyMeta_template,VALUE_TYPE=cn.plus.model.MyCron,cache_name=my_cron,ATOMICITY=TRANSACTIONAL_SNAPSHOT,cache_group=my_meta\";";
    }
}













































































































