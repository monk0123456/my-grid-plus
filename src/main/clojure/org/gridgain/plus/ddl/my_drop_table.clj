(ns org.gridgain.plus.ddl.my-drop-table
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
        :name org.gridgain.plus.dml.MyDropTable
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

(defn table_exists [^String create_index]
    (if (some? (re-find #"(?i)\sIF\sEXISTS$" create_index))
        {:create_index_line create_index :exists true}
        {:create_index_line create_index :exists false}))

(defn get_drop_table_obj [^String sql_line]
    (when-let [sql (my-create-table/get_sql sql_line)]
        (let [drop_index (re-find #"^(?i)DROP\sTable\sIF\sEXISTS\s|^(?i)DROP\sTable\s" sql) table_name (str/replace sql #"^(?i)DROP\sTable\sIF\sEXISTS\s|^(?i)DROP\sTable\s" "")]
            (if (some? drop_index)
                {:drop_line (str/trim drop_index) :is_exists (table_exists (str/trim drop_index)) :table_name (str/trim table_name)}
                (throw (Exception. "删除表语句错误！"))))))

(defn get_sql [drop_table_obj]
    (if-let [{drop_line :drop_line table_name :table_name} drop_table_obj]
        (format "%s IF EXISTS %s" drop_line table_name)))

(defn get_sql_ds [^clojure.lang.PersistentArrayMap drop_table_obj ^String data_set_name]
    (if-let [{drop_line :drop_line table_name :table_name} drop_table_obj]
        (format "%s %s_%s" drop_line data_set_name table_name)))

(defn getTablePk [^Ignite ignite ^String table_name ^Long data_set_id]
    (if-let [lst (first (.getAll (.query (.cache ignite "my_meta_tables") (.setArgs (SqlFieldsQuery. "select m.id from my_meta_tables as m where m.data_set_id = ? and m.table_name = ?") (to-array [data_set_id table_name])))))]
        (nth lst 0)))

(defn getTableItemPk [^Ignite ignite ^Long table_id]
    (loop [[f & r] (.getAll (.query (.cache ignite "table_item") (.setArgs (SqlFieldsQuery. "select m.id from table_item as m where m.table_id = ?") (to-array [table_id])))) lst_rs []]
        (if (some? f)
            (recur r (conj lst_rs (MyTableItemPK. (nth f 0) table_id)))
            lst_rs)))

(defn getCacheex [^Ignite ignite ^String table_name ^Long data_set_id]
    (if-let [m (getTablePk ignite table_name data_set_id)]
        (loop [[f & r] (getTableItemPk ignite m) lst []]
            (if (some? f)
                (recur r (conj lst (MyCacheEx. (.cache ignite "table_item") f nil (SqlType/DELETE))))
                (conj lst (MyCacheEx. (.cache ignite "my_meta_tables") m nil (SqlType/DELETE)))))))

; 整个 ddl 的 obj
(defn get_ddl_obj [^Ignite ignite ^String sql_line ^Long data_set_id ^Long group_id]
    (if-let [m (get_drop_table_obj sql_line)]
        (if (true? (.isDataSetEnabled (.configuration ignite)))
            (let [ddl_id (.incrementAndGet (.atomicSequence ignite "ddl_log" 0 true))]
                {:sql (my-lexical/to_arryList (get_sql m)) :lst_cachex (doto (my-lexical/to_arryList (getCacheex ignite (str/lower-case (-> m :table_name)) data_set_id)) (.add (MyCacheEx. (.cache ignite "ddl_log") ddl_id (DdlLog. ddl_id group_id sql_line data_set_id) (SqlType/INSERT))))})
            {:sql (my-lexical/to_arryList (get_sql m)) :lst_cachex (my-lexical/to_arryList (getCacheex ignite (str/lower-case (-> m :table_name)) data_set_id))})
        (throw (Exception. "删除表语句错误！"))))

; run ddl obj
(defn run_ddl [^Ignite ignite ^String sql_line ^Long data_set_id ^Long group_id]
    (if-let [m (get_ddl_obj ignite sql_line data_set_id group_id)]
        (MyDdlUtil/runDdl ignite m)))

(defn get_sql_all
    ([^Ignite ignite ^clojure.lang.PersistentArrayMap m] (if-let [lst_ds (my-lexical/get_all_ds ignite (-> m :table_name))]
                                                             (get_sql_all lst_ds m [(get_sql m)])
                                                             [(get_sql m)]))
    ([[f_data_set_name & r_data_set_name] ^clojure.lang.PersistentArrayMap m ^clojure.lang.PersistentVector lst]
     (if (some? f_data_set_name)
         (recur r_data_set_name m (conj lst (get_sql_ds m f_data_set_name)))
         lst)))

; 实时数据集
(defn run_ddl_real_time [^Ignite ignite ^String sql_line ^Long data_set_id]
    (if-let [m (get_drop_table_obj sql_line)]
        (MyDdlUtil/runDdl ignite {:sql (my-lexical/to_arryList (get_sql_all ignite m)) :lst_cachex (my-lexical/to_arryList (getCacheex ignite (str/lower-case (-> m :table_name)) data_set_id))})))

; 删除表
(defn drop_table [^Ignite ignite ^Long group_id ^String sql_line]
    (let [sql_code (str/lower-case sql_line)]
        (if (= group_id 0)
            (run_ddl_real_time ignite sql_code 0)
            (if-let [my_group (.get (.cache ignite "my_users_group") group_id)]
                (let [group_type (.getGroup_type my_group) dataset (.get (.cache ignite "my_dataset") (.getData_set_id my_group))]
                    (if (contains? #{"ALL" "DDL"} group_type)
                        (if (true? (.getIs_real dataset))
                            (run_ddl_real_time ignite sql_code (.getId dataset))
                            (if-let [m (get_drop_table_obj sql_code)]
                                (if-not (my-lexical/is-eq? (-> (my-lexical/get-schema (-> m :table_name)) :schema_name) "my_meta")
                                    (if-let [tables (first (.getAll (.query (.cache ignite "my_dataset_table") (.setArgs (SqlFieldsQuery. "select COUNT(t.id) from my_dataset_table as t WHERE t.dataset_id = ? and t.table_name = ?") (to-array [(.getData_set_id my_group) (str/trim (-> m :table_name))])))))]
                                        (if (> (first tables) 0)
                                            (throw (Exception. (format "该用户组不能删除实时数据集对应到该数据集中的表：%s！" (str/trim (-> m :table_name)))))
                                            (run_ddl ignite sql_code (.getId dataset) group_id)
                                            ))
                                    (throw (Exception. "没有执行语句的权限！")))
                                (throw (Exception. "删除表语句错误！请仔细检查并参考文档"))))
                        (throw (Exception. "该用户组没有执行 DDL 语句的权限！"))))
                (throw (Exception. "不存在该用户组！"))
                ))))














































