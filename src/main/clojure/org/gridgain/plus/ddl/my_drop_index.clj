(ns org.gridgain.plus.ddl.my-drop-index
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.ddl.my-create-table :as my-create-table]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache SqlType DdlLog DataSetDdlLog)
             (cn.plus.model.ddl MyDataSet MyDatasetTable MyDatasetTablePK MyDeleteViews MyInsertViews MySelectViews MyTable MyTableIndex MyTableIndexItem MyTableItem MyTableItemPK MyTableObj MyUpdateViews MyViewObj ViewOperateType ViewType)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (org.apache.ignite.binary BinaryObjectBuilder BinaryObject)
             (org.gridgain.ddl MyCreateTableUtil MyDdlUtil)
             (java.util ArrayList Date Iterator)
             (java.sql Timestamp)
             (java.math BigDecimal)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyDropIndex
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

(defn index_exists [^String create_index]
    (if (re-find #"(?i)\sIF\sEXISTS$" create_index)
        true
        false))

(defn get_drop_index_obj [^String sql_line]
    (if-let [sql (my-create-table/get_sql sql_line)]
        (let [drop_index (re-find #"^(?i)DROP\sINDEX\sIF\sEXISTS\s|^(?i)DROP\sINDEX\s" sql) index_name (str/replace sql #"^(?i)DROP\sINDEX\sIF\sEXISTS\s|^(?i)DROP\sINDEX\s" "")]
            (if (some? drop_index)
                {:drop_line (str/trim drop_index) :is_exists (index_exists (str/trim drop_index)) :index_name (str/trim index_name)}
                (throw (Exception. "删除索引语句错误！"))))
        (throw (Exception. "删除索引语句错误！"))))

(defn drop-index-obj [^Ignite ignite ^String data_set_name ^String sql_line]
    (letfn [(get-index-id [^Ignite ignite ^String index_name]
                (loop [[f & r] (.getAll (.query (.cache ignite "table_index") (.setArgs (SqlFieldsQuery. "select m.id, m.table_id from table_index as m where m.index_name = ?") (to-array [index_name])))) lst-rs []]
                    (if (some? f)
                        (recur r (conj lst-rs {:table_id (second f) :index_id (first f)}))
                        lst-rs)))
            (get-index-schema [^Ignite ignite ^Long table_id]
                (let [data_set_id (first (first (.getAll (.query (.cache ignite "my_meta_tables") (.setArgs (SqlFieldsQuery. "select m.data_set_id from my_meta_tables as m where m.id = ?") (to-array [table_id]))))))]
                    (if (= data_set_id 0)
                        "PUBLIC"
                        (first (first (.getAll (.query (.cache ignite "my_dataset") (.setArgs (SqlFieldsQuery. "select m.dataset_name from my_dataset as m where m.id = ?") (to-array [data_set_id])))))))))
            (get-table-indexs-item-id [^Ignite ignite ^Long index_id]
                (loop [[f & r] (.getAll (.query (.cache ignite "table_index_item") (.setArgs (SqlFieldsQuery. "select m.id from table_index_item as m where m.index_no = ?") (to-array [index_id])))) lst-rs []]
                    (if (some? f)
                        (recur r (conj lst-rs {:index_id index_id :index_item_id (first f)}))
                        lst-rs)))
            (get-cachex [^Ignite ignite ^String index_name ^ArrayList lst]
                (let [{table_id :table_id index_id :index_id} (get-index-id ignite index_name)]
                    (let [items (get-table-indexs-item-id ignite index_id)]
                        (loop [[f & r] items]
                            (if (some? f)
                                (do
                                    (doto lst (.add (MyCacheEx. (.cache ignite "table_index_item") (MyTableItemPK. (-> f :index_item_id) (-> f :index_id)) nil (SqlType/DELETE))))
                                    (recur r))))
                        {:schema_name (get-index-schema ignite table_id) :lst_cachex (doto lst (.add (MyCacheEx. (.cache ignite "table_index") (MyTableItemPK. index_id table_id) nil (SqlType/DELETE))))}
                        )))
            ]
        (let [{index_name :index_name} (get_drop_index_obj sql_line)]
            (if-let [{schema_name :schema_name lst_cachex :lst_cachex} (get-cachex ignite index_name (ArrayList.))]
                (if-not (nil? lst_cachex)
                    (cond (and (= schema_name "") (not (= data_set_name ""))) {:sql sql_line :lst_cachex lst_cachex}
                          (or (and (not (= schema_name "")) (my-lexical/is-eq? data_set_name "MY_META")) (and (not (= schema_name "")) (my-lexical/is-eq? schema_name data_set_name))) {:sql sql_line :lst_cachex lst_cachex}
                          :else
                          (throw (Exception. "没有删除索引的权限！"))
                          )
                    (throw (Exception. "没有删除索引的权限！")))
                ))))

; 实时数据集
(defn run_ddl_real_time [^Ignite ignite ^String data_set_name ^String sql_line]
    (let [{sql :sql lst_cachex :lst_cachex} (drop-index-obj ignite data_set_name sql_line)]
        (MyDdlUtil/runDdl ignite {:sql (doto (ArrayList.) (.add sql)) :lst_cachex lst_cachex})))

; 删除表索引
(defn drop_index [^Ignite ignite ^Long group_id ^String dataset_name ^String group_type ^Long dataset_id ^String sql_line]
    (let [sql_code (str/lower-case sql_line)]
        (if (= group_id 0)
            (run_ddl_real_time ignite dataset_name sql_code)
            (if (contains? #{"ALL" "DDL"} (str/upper-case group_type))
                (run_ddl_real_time ignite dataset_name sql_code)
                (throw (Exception. "该用户组没有执行 DDL 语句的权限！"))))))




















































