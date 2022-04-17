package org.gridgain.select;

import clojure.lang.LazySeq;
import clojure.lang.Obj;
import clojure.lang.RT;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.gridgain.dml.util.MyCacheExUtil;
import org.gridgain.plus.dml.MySelect;
import org.junit.Test;

import java.util.ArrayList;

public class MyFunc_case {

    @Test
    public void to_ast()
    {
        //LazySeq lazySeq = MySelect.getSqlToAst("select m.name from my_table_a as m where m.id = :id");
        //Object object = lazySeq.first();
        //System.out.println(object);

        ArrayList<String> lst = new ArrayList<>();

    }

    /**
     * 添加 func 到 cache
     * */
    @Test
    public void save_func() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        CacheConfiguration<String, LazySeq> cfg_func = new CacheConfiguration<>();
        cfg_func.setCacheMode(CacheMode.REPLICATED);
        cfg_func.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        //cfg_func.setDataRegionName("Meta_Region_Eviction");
        cfg_func.setName("my_meta_cache_all_func");
        cfg_func.setReadFromBackup(true);

        IgniteCache<String, LazySeq> funcCache = ignite.getOrCreateCache(cfg_func);
        funcCache.remove("my_func");

        //LazySeq lazySeq = MySelect.getSqlToAst("select m.name from my_table_a as m where m.id = :id");
        //byte[] bytes = MyCacheExUtil.objToBytes(lazySeq.first());
        //funcCache.put("my_func", lazySeq);
        //System.out.println(lazySeq.first());
    }

    @Test
    public void read_func() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        RT.init();

        IgniteCache<String, LazySeq> funcCache = ignite.cache("my_meta_cache_all_func");
        //byte[] lazySeq = (byte[])funcCache.get("my_func");
        //Object o = MyCacheExUtil.restore(lazySeq);
        LazySeq lazySeq = funcCache.get("my_func");
        System.out.println(lazySeq);
    }
}































































