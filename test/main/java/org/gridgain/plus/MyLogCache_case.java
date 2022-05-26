package org.gridgain.plus;

import cn.plus.model.MyKeyValue;
import cn.plus.model.MyLogCache;
import cn.plus.model.SqlType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MyLogCache_case {

    @Test
    public void test_1()
    {
        List<MyKeyValue> vs = new ArrayList<>();
        vs.add(new MyKeyValue("categoryname", "吴大富"));
        vs.add(new MyKeyValue("description", "吴大贵"));
        vs.add(new MyKeyValue("picture", ""));
        MyLogCache myLogCache = new MyLogCache("f_public_categories", "public", "categories", 101, vs, SqlType.INSERT);
        System.out.println(myLogCache.toString());
    }
}
