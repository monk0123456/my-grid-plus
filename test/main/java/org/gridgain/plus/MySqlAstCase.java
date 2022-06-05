package org.gridgain.plus;

import cn.mysuper.service.IMySqlAst;
import org.gridgain.dml.util.MyCacheExUtil;
import org.gridgain.plus.dml.MyLexical;
import org.gridgain.plus.dml.MySelectPlus;
import org.junit.Test;
import org.tools.MyLineToBinary;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MySqlAstCase {

    @Test
    public void myAstCase_func()
    {
        Object ast = MySelectPlus.sqlToAst((ArrayList) MyLexical.lineToList("f(a, b+c, g(q-w, e-r))"));
        System.out.println(ast);
    }

    @Test
    public void my_str()
    {
        List<String> lst = MyLexical.lineToList("loadFromNative(\"wudafu\", '吴大富')");
        List<String> lst_1 = lst.subList(2, lst.size() - 1);
        String txt = MyLexical.getStrValue(lst_1.get(0));

        List<String> lst_2 = Arrays.asList(new String[] {"a", "b", "c"});
        System.out.println(lst_2);

        System.out.println(txt);
    }

    @Test
    public void my_str_1()
    {
        List<String> lst = MyLexical.lineToList("loadFromNative(\"wudafu\")");
        List<String> lst_1 = lst.subList(2, lst.size() - 1);
        String code = MyLexical.getStrValue(lst_1.get(0));

        String[] myLoadFromNative = new String[]{"loadCode", "(", code, ")"};

        List<String> lst_2 = Arrays.asList(myLoadFromNative);
        System.out.println(lst_2);
    }

    @Test
    public void my_str_2()
    {
        byte[] bt_str = MyCacheExUtil.objToBytes("wudafu");
        Object obj = MyCacheExUtil.restore(bt_str);
        if (obj instanceof String)
        {
            System.out.println(obj);
        }

        List<Object> lst = new ArrayList<>();
        lst.add("吴大富");
        lst.add("吴大贵");
        lst.add("美羊羊");

        List<String> lst_1 = new ArrayList<>();
        lst_1.add("a");
        lst_1.add("b");
        lst.add(lst_1);

        byte[] bt_lst = MyCacheExUtil.objToBytes(lst);
        Object obj_lst = MyCacheExUtil.restore(bt_lst);
        if (obj_lst instanceof List)
        {
            System.out.println(obj_lst);
        }
    }
}
