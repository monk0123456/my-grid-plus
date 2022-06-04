package org.gridgain.plus;

import cn.mysuper.service.IMySqlAst;
import org.gridgain.plus.dml.MyLexical;
import org.gridgain.plus.dml.MySelectPlus;
import org.junit.Test;

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
}
