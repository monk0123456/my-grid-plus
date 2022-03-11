(ns org.gridgain.plus.ddl.my-drop-dataset
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (cn.myservice MyInitFuncService)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyDropDataSet
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

; 删除 dataset
(defn drop-data-set [^Ignite ignite ^Long group-id ^String sql]
    (if (= group-id 0)
        (if (re-find #"^(?i)DROP\s+DATASET\s+IF\s+EXISTS\s+|^(?i)DROP\s+DATASET\s+" sql)
            (let [data_set_name (str/replace sql #"^(?i)DROP\s+DATASET\s+IF\s+EXISTS\s+|^(?i)DROP\s+DATASET\s+" "")]
                (if (empty? (.getAll (.query (.cache ignite "my_dataset") (.setArgs (SqlFieldsQuery. "select mt.id from my_dataset as m, my_meta_tables as mt where m.id = mt.data_set_id and m.dataset_name = ? limit 0, 1") (to-array [(str/upper-case data_set_name)])))))
                    (if-let [tx (.txStart (.transactions ignite))]
                        (try
                            (.destroy (.cache ignite (str (str/upper-case data_set_name) "_meta")))
                            ;(.dropSchemaFunc (MyInitFuncService/getInstance) ignite (str/upper-case data_set_name))
                            (.commit tx)
                            (catch Exception ex
                                (.rollback tx)
                                (.getMessage ex))
                            (finally (.close tx))))
                    ))
            (throw (Exception. "创建数据集语句的错误！")))
        (throw (Exception. "没有执行语句的权限！"))))
