package org.gridgain.reflect;

import cn.plus.model.ddl.MyFunc;
import cn.plus.model.ddl.MyFuncPs;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgnitionEx;
import org.junit.Test;
import org.tools.MyPlusUtil;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class MyFuncCaseTest {

    /**
     * 通过参数类型的名称获取参数类型
     * */
    public Class<?> getClassByName(final String typeName)
    {
        switch (typeName)
        {
            case "String":
                return String.class;
            case "Double":
                return Double.class;
            case "double":
                return double.class;
            case "Integer":
                return Integer.class;
            case "int":
                return int.class;
            case "Long":
                return Long.class;
            case "long":
                return long.class;
            case "Boolean":
                return Boolean.class;
            case "boolean":
                return boolean.class;
            case "Date":
                return Date.class;
            case "Timestamp":
                return Timestamp.class;
        }

        return null;
    }

    @Test
    public void test1() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InstantiationException, InvocationTargetException {
        Class<?> cls = Class.forName("org.gridgain.reflect.MyFuncCase");

        List<Class<?>> pstypes = new ArrayList<Class<?>>();
        pstypes.add(getClassByName("int"));
        pstypes.add(getClassByName("int"));

        List<Object> psvalue = new ArrayList<>();
        psvalue.add(12345);
        psvalue.add(54321);

        Method method = cls.getMethod("add", pstypes.toArray(new Class[]{}));

        Object rs = method.invoke(cls.newInstance(), psvalue.toArray());
        System.out.println(rs);
    }

    @Test
    public void test_2()
    {
        MyFunc myFunc = new MyFunc();
        List<MyFuncPs> lst = new ArrayList<>();
        lst.add(new MyFuncPs(2, "int"));
        lst.add(new MyFuncPs(3, "String"));
        lst.add(new MyFuncPs(1, "Double"));
        myFunc.setLst(lst);

        List<MyFuncPs> lst_1 = myFunc.getLst().stream().sorted((a, b) -> a.getPs_index() - b.getPs_index()).collect(Collectors.toList());
        for (MyFuncPs m : lst_1)
        {
            System.out.println(String.valueOf(m.getPs_index()) + " " + m.getPs_type());
        }
    }

    @Test
    public void add_to_cache() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyGridGain/my-grid-plus/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        MyFunc myFunc = new MyFunc("add", "add", "org.gridgain.reflect.MyFuncCase", "int", "");
        List<MyFuncPs> lst = new ArrayList<>();
        lst.add(new MyFuncPs(0, "int"));
        lst.add(new MyFuncPs(1, "int"));
        myFunc.setLst(lst);

        ignite.cache("my_func").put("add", myFunc);
    }

    @Test
    public void call_to_cache() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyGridGain/my-grid-plus/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        int rs = (int) MyPlusUtil.invokeFunc(ignite, "add", 123456, 654321);
        System.out.println(rs);
    }
}


































