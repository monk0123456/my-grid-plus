package org.gridgain.context;

import clojure.lang.PersistentHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgnitionEx;
import org.junit.Test;

public class MyContext_case {

    @Test
    public void show() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyPlus/my-plus-deploy/src/main/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);
//        MyContextView plusInit = new MyContextView(ignite, 1L);
//        PersistentHashMap persistentHashMap = plusInit.getContext();
//        persistentHashMap.size();
        System.out.println("OK");
    }
}
