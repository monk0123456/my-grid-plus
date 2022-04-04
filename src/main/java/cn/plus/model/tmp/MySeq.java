package cn.plus.model.tmp;

import clojure.lang.LazySeq;
import clojure.lang.PersistentList;
import clojure.lang.PersistentVector;
import cn.plus.model.MyLogCache;

import java.util.List;
import java.util.stream.Collectors;

public class MySeq {

    public void myTrans(final PersistentVector lstLogCache)
    {
        List<String> lst = (List<String>) lstLogCache.stream().map(m -> "吴大富：" + m).collect(Collectors.toList());
        lst.stream().forEach(m -> System.out.println(m));
    }

    public void myTrans(final PersistentList lstLogCache)
    {
        List<String> lst = (List<String>) lstLogCache.stream().map(m -> "吴大富：" + m).collect(Collectors.toList());
        lst.stream().forEach(m -> System.out.println(m));
    }

    public void myTrans(final LazySeq lstLogCache)
    {
        List<String> lst = (List<String>) lstLogCache.stream().map(m -> "吴大富：" + m).collect(Collectors.toList());
        lst.stream().forEach(m -> System.out.println(m));
    }
}
