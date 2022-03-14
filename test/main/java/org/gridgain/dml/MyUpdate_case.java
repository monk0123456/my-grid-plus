package org.gridgain.dml;

import clojure.lang.LazySeq;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.IgnitionEx;
import org.junit.Test;
import org.tools.KvSql;

import java.util.ArrayList;

public class MyUpdate_case {

    @Test
    public void update_case() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyGridGain/my-grid-plus/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

//        ArrayList lazySeq = MyUpdate.my_update_run_log(ignite, (long) 1, "update categories set categoryname = '瓜子' where description <> ''");
//        System.out.println(lazySeq);
    }

    @Test
    public void update_case_1() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyGridGain/my-grid-plus/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        IgniteCache<Integer, BinaryObject> cache = ignite.cache("f_categories").withKeepBinary();
        BinaryObject binaryObject = cache.get(1);
        BinaryObjectBuilder builder = binaryObject.toBuilder();
        builder.setField("CategoryName", "吴大富");
        cache.replace(1, builder.build());
    }

    @Test
    public void update_case_2() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyGridGain/my-grid-plus/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        BinaryObjectBuilder keyBuilder = ignite.binary().builder(KvSql.getKeyType(ignite, "f_orderdetails"));
        BinaryObject binaryObject = keyBuilder.build();
        System.out.println(binaryObject);
    }
}











































