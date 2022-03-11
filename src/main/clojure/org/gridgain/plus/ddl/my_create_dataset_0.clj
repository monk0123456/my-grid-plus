(ns org.gridgain.plus.ddl.my-create-dataset-0
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
             (cn.plus.model.ddl MyDataSet MyDatasetTable MyDatasetRealTable MyDatasetTablePK MyDeleteViews MyInsertViews MySelectViews MyTable MyTableIndex MyTableIndexItem MyTableItem MyTableItemPK MyTableObj MyUpdateViews MyViewObj ViewOperateType ViewType)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (org.apache.ignite.binary BinaryObjectBuilder BinaryObject)
             (org.gridgain.ddl MyCreateTableUtil MyDdlUtil)
             (java.util ArrayList Date Iterator)
             (java.sql Timestamp)
             (java.math BigDecimal)
             (clojure.java.api Clojure))
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyCreateDataSet_0
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

; 获取 dataset name
(defn get_data_set_name [^String sql_line]
    (if-let [data_set_name (re-find #"^(?i)\w+\s\(" sql_line)]
        (let [tables_line (str/replace sql_line #"^(?i)\w+\s\(" "")]
            {:data_set_name (str/trim (subs data_set_name 0 (- (count data_set_name) 2))) :tables (str/split (str/trim (subs tables_line 0 (- (count tables_line) 1))) #"\s*,\s*")})
        (if-let [set_name (re-find #"^(?i)\w+$" sql_line)]
            {:data_set_name set_name}
            (throw (Exception. "创建数据集的语句错误！")))))

; 获取 dataset obj
(defn get_create_data_set_obj [^String sql_line]
    (if-let [sql (my-create-table/get_sql sql_line)]
        (let [create_data_set_line (re-find #"^(?i)CREATE\sREAL\sDATASET\sIF\sNOT\sEXISTS\s|^(?i)CREATE\sREAL\sDATASET\s|^(?i)CREATE\sDATASET\sIF\sNOT\sEXISTS\s|^(?i)CREATE\sDATASET\s" sql) last_line (str/replace sql #"^(?i)CREATE\sREAL\sDATASET\sIF\sNOT\sEXISTS\s|^(?i)CREATE\sREAL\sDATASET\s|^(?i)CREATE\sDATASET\sIF\sNOT\sEXISTS\s|^(?i)CREATE\sDATASET\s" "")]
            (if (some? create_data_set_line)
                (if-let [dic (get_data_set_name last_line)]
                    (if-not (Strings/isNullOrEmpty (re-find #"^(?i)CREATE\sreal\sDATASET\s" create_data_set_line))
                        (assoc dic :is_real true :create_data_set_line create_data_set_line)
                        (assoc dic :is_real false :create_data_set_line create_data_set_line))
                    (throw (Exception. "创建数据集的语句错误！")))
                (throw (Exception. "创建数据集的语句错误！"))))
        (throw (Exception. "创建数据集的语句错误！"))))

; save data set
(defn data_set_cachex [^Ignite ignite ^String data_set_name]
    (if (some? (first (.getAll (.query (.cache ignite "my_dataset") (.setArgs (SqlFieldsQuery. "select m.id from my_dataset as m where m.dataset_name = ? and m.is_real = ?") (to-array [data_set_name false]))))))
        (throw (Exception. (format "已经存在 %s 数据集！" data_set_name)))
        (let [data_set_id (.incrementAndGet (.atomicSequence ignite "my_dataset" 0 true))]
            (MyCacheEx. (.cache ignite "my_dataset") data_set_id (MyDataSet. data_set_id data_set_name) (SqlType/INSERT)))))

(defn data_set_real_cachex [^Ignite ignite ^String data_set_name]
    (if (some? (first (.getAll (.query (.cache ignite "my_dataset") (.setArgs (SqlFieldsQuery. "select m.id from my_dataset as m where m.dataset_name = ? and m.is_real = ?") (to-array [data_set_name true]))))))
        (throw (Exception. (format "已经存在 %s 数据集！" data_set_name)))
        (let [data_set_id (.incrementAndGet (.atomicSequence ignite "my_dataset" 0 true))]
            (MyCacheEx. (.cache ignite "my_dataset") data_set_id (MyDataSet. data_set_id data_set_name) (SqlType/INSERT)))
        ))

(defn get_code [^Ignite ignite id]
    (.getCode (.get (.cache ignite "my_meta_tables") id)))

(defn get_table_ids [^Ignite ignite ^String table_name]
    (if-let [m (first (.getAll (.query (.cache ignite "my_meta_tables") (.setArgs (SqlFieldsQuery. "select m.id from my_meta_tables as m where m.data_set_id = 0 and m.table_name = ?") (to-array [table_name])))))]
        (first m)
        (if-let [ids (first (.getAll (.query (.cache ignite "my_meta_tables") (.setArgs (SqlFieldsQuery. "select m.id from my_meta_tables as m, my_dataset as ds where ds.id = m.data_set_id and ds.is_real = 1 and m.table_name = ?") (to-array [table_name])))))]
            (first ids)
            (throw (Exception. (format "创建数据集的语句中不存在表：%s！" table_name))))))

(defn get_lst_ddl [^Ignite ignite ^String data_set_name ^String table_name]
    (if-let [m (get_table_ids ignite table_name)]
        (if-let [{lst_ddl :lst_ddl} (my-create-table/to_ddl_lst ignite (get_code ignite m) data_set_name)]
            lst_ddl
            (throw (Exception. (format "创建数据集的语句中不存在表：%s！" table_name))))
        (throw (Exception. (format "创建数据集的语句中不存在表：%s！" table_name)))))

(defn my_dataset_table_cachex
    ([^Ignite ignite ^MyCacheEx cache ^clojure.lang.PersistentVector tables]
     (my_dataset_table_cachex ignite cache tables [] []))
    ([^Ignite ignite ^MyCacheEx cache [f & r] lst lst_tables]
     (if (some? f)
         (if-let [lst_ddl (get_lst_ddl ignite (.getDataset_name (.getValue cache)) f)]
             (let [table_id (.incrementAndGet (.atomicSequence ignite "my_dataset_table" 0 true))]
                 (recur ignite cache r (conj lst (MyCacheEx. (.cache ignite "my_dataset_table") (MyDatasetTablePK. table_id (.getKey cache)) (MyDatasetTable. table_id f (.getId (.getValue cache)) true) (SqlType/INSERT))) (concat lst_tables lst_ddl)))
             )
         {:cache (conj lst cache) :ddl lst_tables})))

(defn my_dataset_table_cachex_real
    ([^Ignite ignite ^MyCacheEx cache ^clojure.lang.PersistentVector tables]
     (my_dataset_table_cachex_real ignite cache tables [] []))
    ([^Ignite ignite ^MyCacheEx cache [f & r] lst lst_tables]
     (if (some? f)
         (if-let [lst_ddl (get_lst_ddl ignite (.getDataset_name (.getValue cache)) f)]
             (let [table_id (.incrementAndGet (.atomicSequence ignite "my_dataset_real_table" 0 true))]
                 (recur ignite cache r (conj lst (MyCacheEx. (.cache ignite "my_dataset_real_table") (MyDatasetTablePK. table_id (.getKey cache)) (MyDatasetRealTable. table_id f (.getId (.getValue cache))) (SqlType/INSERT))) (concat lst_tables lst_ddl)))
             )
         {:cache (conj lst cache) :ddl lst_tables})))

; 生成 create table 和 create index 的 MyCacheEx
(defn run_ddl [^Ignite ignite m_obj]
    (let [{data_set_name :data_set_name tables :tables} m_obj]
        (if-not (Strings/isNullOrEmpty data_set_name)
            (my_dataset_table_cachex ignite (data_set_cachex ignite data_set_name) tables)
            (throw (Exception. "创建数据集的语句错误！")))))

; 新建实时数据集
(defn run_ddl_real_time [^Ignite ignite m_obj]
    (if-let [{data_set_name :data_set_name tables :tables} m_obj]
        (if-not (Strings/isNullOrEmpty data_set_name)
            (my_dataset_table_cachex_real ignite (data_set_real_cachex ignite data_set_name) tables)
            (throw (Exception. "创建数据集的语句错误！")))))

(defn get_sql_lst
    ([lst] (get_sql_lst lst [] []))
    ([[f & r] lst_sql lst_un_sql]
     (if (some? f)
         (recur r (concat lst_sql [(-> f :sql)]) (concat lst_un_sql [(-> f :un_sql)]))
         {:sql (my-lexical/to_arryList lst_sql) :un_sql (my-lexical/to_arryList lst_un_sql)})))

(defn get_run_dll [^Ignite ignite ^String sql_line]
    (if-let [m_obj (get_create_data_set_obj sql_line)]
        (if (true? (-> m_obj :is_real))
            (if-let [{cache :cache} (run_ddl_real_time ignite m_obj)]
                (if (true? (.isDataSetEnabled (.configuration ignite)))
                    (let [ds_id (.incrementAndGet (.atomicSequence ignite "dataset_ddl_log" 0 true))]
                        (MyDdlUtil/runDdl ignite {:lst_cachex (doto (my-lexical/to_arryList cache) (.add (MyCacheEx. (.cache ignite "dataset_ddl_log") ds_id (DataSetDdlLog. ds_id (-> m_obj :data_set_name) "create" sql_line) (SqlType/INSERT))))}))
                    (MyDdlUtil/runDdl ignite {:lst_cachex (my-lexical/to_arryList cache)}))
                )
            (if-let [{ddl :ddl cache :cache} (run_ddl ignite m_obj)]
                (let [lst_ddl (get_sql_lst ddl)]
                    (if (true? (.isDataSetEnabled (.configuration ignite)))
                        (let [ds_id (.incrementAndGet (.atomicSequence ignite "dataset_ddl_log" 0 true))]
                            (MyDdlUtil/runDdl ignite (assoc lst_ddl :lst_cachex (doto (my-lexical/to_arryList cache) (.add (MyCacheEx. (.cache ignite "dataset_ddl_log") ds_id (DataSetDdlLog. ds_id (-> m_obj :data_set_name) "create" sql_line) (SqlType/INSERT)))))))
                        (MyDdlUtil/runDdl ignite (assoc lst_ddl :lst_cachex (my-lexical/to_arryList cache)))))
                ))))

; 创建数据集
(defn create_data_set [^Ignite ignite ^Long group_id ^String sql_line]
    (if (= group_id 0)
        (get_run_dll ignite sql_line)
        (if-let [my_group (.get (.cache ignite "my_users_group") group_id)]
            (let [data_set_id (.getData_set_id my_group) group_type (.getGroup_type my_group)]
                (if (and (contains? #{"ALL" "DDL"} group_type) (= data_set_id 0))
                    (get_run_dll ignite sql_line)
                    (throw (Exception. "用户组没有创建数据集的权限，或者用户组不能的数据集不是元数据集！")))))))

































































