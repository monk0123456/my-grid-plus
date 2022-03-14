package org.gridgain.trans;

import clojure.lang.Keyword;
import cn.plus.model.MyCacheEx;
import cn.plus.model.ddl.MyDataSet;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

import java.util.ArrayList;

public class MyTrans_case {

    @Test
    public void trans_case() throws IgniteCheckedException {
        String springCfgPath = "/Users/chenfei/Documents/Java/MyGridGain/my-grid-plus/resources/default-config.xml";
        Ignite ignite = IgnitionEx.start(springCfgPath);

        IgniteCache<Long, MyDataSet> dsCache = ignite.cache("my_dataset");

        IgniteTransactions transactions = ignite.transactions();
        Transaction tx = null;

        try {
            tx = transactions.txStart(PESSIMISTIC, REPEATABLE_READ);

            dsCache.put(1L, new MyDataSet(1L, "myy"));

            tx.commit();

        } catch (Exception ex)
        {
            if (tx != null)
            {
                tx.rollback();
            }
            System.out.println(ex.getMessage());
        }
        finally {
            if (tx != null) {
                tx.close();
            }
        }
    }
}
































































