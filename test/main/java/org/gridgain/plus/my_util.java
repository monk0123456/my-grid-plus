package org.gridgain.plus;

import clojure.lang.Obj;
import cn.plus.model.ddl.MyFuncPs;
import org.gridgain.plus.tools.MyUtil;
import org.junit.Test;
import org.tools.MyFunction;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class my_util {
    MyUtil util;

    @Test
    public void test_1()
    {
        List<String> lst = new ArrayList<>();
        lst.add("a");
        //String rs = MyUtil.gson().toJson(lst);
        //System.out.println(rs);
    }

    @Test
    public void test_2()
    {
//        ArrayList<MyScenesPs> lst = new ArrayList<MyScenesPs>();
//        lst.add(new MyScenesPs("S".getClass(), 0L));
//        lst.add(new MyScenesPs("S".getClass(), 1L));

        //String rs = MyUtil.gson().toJson(lst);
        //System.out.println(rs);
    }

    public static Integer getUUIDInOrderId(){
        Integer orderId = UUID.randomUUID().toString().hashCode();
        orderId = orderId < 0 ? -orderId : orderId; //String.hashCode() 值会为空
        return orderId;
    }

    @Test
    public void test_4()
    {
        String name = "root";
        String password = "吴大贵";
        System.out.println(name.hashCode());
        System.out.println(password.hashCode());

        System.out.println(UUID.randomUUID().toString().replace("-", ""));
        System.out.println(UUID.randomUUID().toString().hashCode());

        System.out.println(UUID.randomUUID().getLeastSignificantBits());
        System.out.println(UUID.randomUUID().getMostSignificantBits());
        System.out.println(UUID.fromString("吴大富-wudagui").getLeastSignificantBits());
    }

    public void test_3()
    {
        Map<String, String> map = new HashMap<>();
        map.put("1", "A");
        map.put("2", "B");
        map.put("3", "C");

        //map.keySet()
        //map.containsKey()
    }

    @Test
    public void my_case()
    {
        List<MyFuncPs> lst = new ArrayList<>();
        lst.add(new MyFuncPs(1, "String"));
        lst.add(new MyFuncPs(2, "Long"));
        lst.add(new MyFuncPs(3, "Integer"));
        StringBuilder sb = new StringBuilder();
        List<MyFuncPs> lstFunc = lst.stream().sorted((a, b) -> a.getPs_index() - b.getPs_index()).collect(Collectors.toList());
        for (MyFuncPs m : lstFunc)
        {
            sb.append(String.format("参数 %s: (%s) ", m.getPs_index().toString(), m.getPs_type()));
        }
        System.out.println(sb.toString());
    }

    public int add(final int n)
    {
        int rs = 0;
        for (int i = 0; i <= n; i++)
        {
            rs += i;
        }
        return rs;
    }

    @Test
    public void my_case_1()
    {
        ArrayList<String> lst = new ArrayList<>();
        lst.add("吴大富");
        lst.add("美羊羊");
        lst.add("吴大贵");

        Object my_lst = lst.remove(1);

        System.out.println(my_lst);

        for (String line : lst)
        {
            System.out.println(line);
        }

        System.out.println("OK");
    }

    @Test
    public void my_case_2()
    {
        Hashtable<String, Object> ht = new Hashtable<>();
        ht.put("a", 1);
        ht.put("b", 2);
        ht.put("c", 3);

        ht.remove("a");

        for (String line : ht.keySet())
        {
            System.out.println(line);
        }

        System.out.println("OK");
    }

    public static <T extends Comparable & Serializable> T least(List<T> t) {

        Optional<T> largest = t.stream().min(T::compareTo);
        if (largest.isPresent()) {
            return largest.get();
        } else {
            return null;
        }
    }

    @Test
    public void my_test_3()
    {
        List<Integer> lst = new ArrayList<>();
        lst.add(12);
        lst.add(23);
        lst.add(41);

        //System.out.println(least(lst));

        System.out.println(MyFunction.least("wu", "da", "fu"));
    }
}








































