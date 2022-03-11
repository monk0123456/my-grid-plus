(ns org.gridgain.plus.ddl.my-create-dataset
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.ddl.my-create-table :as my-create-table]
        [org.gridgain.plus.dml.my-expression :as my-expression]
        [org.gridgain.plus.context.my-context :as my-context]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.configuration CacheConfiguration)
             (cn.myservice MyInitFuncService)
             (cn.plus.model.ddl MyDataSet)
             (org.apache.ignite.cache CacheMode)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery))
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyCreateDataSet
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

; CREATE DATASET CRM_DATA_SET
(defn create_data_set [^Ignite ignite ^Long group_id ^String sql]
    (if (= group_id 0)
        (if (re-find #"^(?i)CREATE\s+DATASET\s+IF\s+NOT\s+EXISTS\s+|^(?i)CREATE\s+DATASET\s+" sql)
            (let [data_set_name (str/replace sql #"^(?i)CREATE\s+DATASET\s+IF\s+NOT\s+EXISTS\s+|^(?i)CREATE\s+DATASET\s+" "")]
                (if (empty? (.getAll (.query (.cache ignite "my_dataset") (.setArgs (SqlFieldsQuery. "select m.id from my_meta.my_dataset as m where m.dataset_name = ?") (to-array [(str/upper-case data_set_name)])))))
                    (if-let [tx (.txStart (.transactions ignite))]
                        (try
                            (let [id (.incrementAndGet (.atomicSequence ignite "my_dataset" 0 true)) data_set_name_u (str/upper-case data_set_name)]
                                (.put (.cache ignite "my_dataset") id (MyDataSet. id data_set_name_u))
                                (.getOrCreateCache ignite (doto (CacheConfiguration. (str data_set_name_u "_meta"))
                                                              (.setSqlSchema data_set_name_u)))
                                (.initSchemaFunc (MyInitFuncService/getInstance) ignite data_set_name_u))
                            (.commit tx)
                            (catch Exception ex
                                (.rollback tx)
                                (.getMessage ex))
                            (finally (.close tx))))
                    (throw (Exception. "该数据集已经存在了！")))
                )
            (throw (Exception. "创建数据集语句的错误！")))
        (throw (Exception. "没有执行语句的权限！"))))













































