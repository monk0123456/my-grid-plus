package org.gridgain.insert;

import clojure.lang.APersistentMap;
import clojure.lang.LazySeq;
import clojure.lang.PersistentArrayMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgnitionEx;
import org.junit.Test;

public class MyInsert_case {

    @Test
    public void case_1()
    {
        //MyInsert myInsert = new MyInsert();
//        insert_kv m = new insert_kv("wu", "dafu");
//        System.out.println(m.item_name);
//        System.out.println(m.item_value);
    }

    @Test
    public void test_1() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        /*
        PersistentArrayMap insertObj = MyInsert.getPlusInsert(ignite, 1L, "INSERT INTO Person (id, name, city_id) VALUES (1, 'John Doe', 3)");
        for (Object key : insertObj.keySet())
        {
            System.out.println(key);
            System.out.println(insertObj.get(key));
            System.out.println("***********************");
        }

        LazySeq kv_seq = (LazySeq) insertObj.get("kv_seq");
        for (int i = 0; i < kv_seq.size(); i++)
        {
            PersistentArrayMap m = (PersistentArrayMap) kv_seq.get(i);
            System.out.println(m.get("item_name") + " " + m.get("iten_value"));
        }

        System.out.println(insertObj.get("table_name"));
         */
    }

}

















































