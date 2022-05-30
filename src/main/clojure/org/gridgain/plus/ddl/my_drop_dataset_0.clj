(ns org.gridgain.plus.ddl.my-drop-dataset-0
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.ddl.my-create-table :as my-create-table]
        [org.gridgain.plus.ddl.my-create-dataset-0 :as my-create-dataset]
        [org.gridgain.plus.ddl.my-drop-table :as my-drop-table]
        [org.gridgain.plus.ddl.my-alter-dataset :as my-alter-dataset]
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
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyDropDataSet_0
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

(defn dataset_exists [^String sql_line]
    (if (re-find #"(?i)\sIF\sEXISTS$" sql_line)
        true
        false))

(defn get_drop_data_set_obj [^String sql_line]
    (if-let [sql (my-create-table/get_sql sql_line)]
        (let [drop_index (re-find #"^(?i)DROP\sDATASET\sIF\sEXISTS\s|^(?i)DROP\sDATASET\s" sql) dataset_name (str/replace sql #"^(?i)DROP\sDATASET\sIF\sEXISTS\s|^(?i)DROP\sDATASET\s" "")]
            (if (some? drop_index)
                {:drop_line (str/trim drop_index) :is_exists (dataset_exists (str/trim drop_index)) :data_set_name (str/trim dataset_name)}
                (throw (Exception. "删除数据集语句错误！")))
            )
        (throw (Exception. "删除数据集语句错误！"))))

(defn get_ds_id [^Ignite ignite ^String data_set_name]
    (first (.getAll (.query (.cache ignite "my_dataset") (.setArgs (SqlFieldsQuery. "select m.id from my_dataset as m where m.dataset_name = ?") (to-array [data_set_name]))))))

; 删除实时数据集
; 1、删除实时数据集中的表
; 2、删除实时数据集的记录
(defn drop_ds_real [^Ignite ignite ^String data_set_name ^Boolean is_exists ^String sql_line]
    (if-let [data_set_id (get_ds_id ignite data_set_name)]
        (if (some? (first data_set_id))
            (if-let [columns (.getAll (.query (.cache ignite "my_dataset") (.setArgs (SqlFieldsQuery. "SELECT DISTINCT m.table_name FROM my_dataset_real_table AS m INNER JOIN my_dataset AS ds ON m.dataset_id = ds.id WHERE ds.dataset_name = ?") (to-array [data_set_name]))))]
                (let [lst_cache (my-alter-dataset/drop_to_my_dataset_table_real ignite columns data_set_name is_exists)]
                    (if (true? (.isMyLogEnabled (.configuration ignite)))
                        (let [ds_id (.incrementAndGet (.atomicSequence ignite "dataset_ddl_log" 0 true))]
                            (MyDdlUtil/runDdl ignite {:lst_cachex (doto (my-lexical/to_arryList (conj lst_cache (MyCacheEx. (.cache ignite "my_dataset") (first data_set_id) nil (SqlType/DELETE)))) (.add (MyCacheEx. (.cache ignite "dataset_ddl_log") ds_id (DataSetDdlLog. ds_id data_set_name "drop" sql_line) (SqlType/INSERT))))}))
                        (MyDdlUtil/runDdl ignite {:lst_cachex (my-lexical/to_arryList (conj lst_cache (MyCacheEx. (.cache ignite "my_dataset") (first data_set_id) nil (SqlType/DELETE))))}))
                    )
                (if (true? (.isMyLogEnabled (.configuration ignite)))
                    (let [ds_id (.incrementAndGet (.atomicSequence ignite "dataset_ddl_log" 0 true))]
                        (MyDdlUtil/runDdl ignite {:lst_cachex (doto (my-lexical/to_arryList [(MyCacheEx. (.cache ignite "my_dataset") (first data_set_id) nil (SqlType/DELETE))]) (.add (MyCacheEx. (.cache ignite "dataset_ddl_log") ds_id (DataSetDdlLog. ds_id data_set_name "drop" sql_line) (SqlType/INSERT))))}))
                    (MyDdlUtil/runDdl ignite {:lst_cachex (my-lexical/to_arryList [(MyCacheEx. (.cache ignite "my_dataset") (first data_set_id) nil (SqlType/DELETE))])}))
                )
            (throw (Exception. (format "要删除的数据集 %s 不存在！" data_set_name)))))
    )

; 删除批处理数据集
(defn drop_ds [^Ignite ignite ^String data_set_name ^Boolean is_exists ^String sql_line]
    (if-let [data_set_id (get_ds_id ignite data_set_name)]
        (if (some? (first data_set_id))
            (if-let [columns (.getAll (.query (.cache ignite "my_dataset") (.setArgs (SqlFieldsQuery. "SELECT DISTINCT m.table_name FROM my_dataset_table AS m INNER JOIN my_dataset AS ds ON m.dataset_id = ds.id WHERE ds.dataset_name = ?") (to-array [data_set_name]))))]
                (let [{ddl :ddl lst_cache :lst_cache} (my-alter-dataset/drop_to_my_dataset_table ignite columns data_set_name (first data_set_id) is_exists)]
                    (let [lst_ddl (my-create-dataset/get_sql_lst ddl)]
                        (MyDdlUtil/runDdl ignite (assoc lst_ddl :lst_cachex (my-lexical/to_arryList (conj lst_cache (MyCacheEx. (.cache ignite "my_dataset") (first data_set_id) nil (SqlType/DELETE)))))))
                    )
                (MyDdlUtil/runDdl ignite {:lst_cachex (my-lexical/to_arryList [(MyCacheEx. (.cache ignite "my_dataset") (first data_set_id) nil (SqlType/DELETE))])})
                )
            (throw (Exception. (format "要删除的数据集 %s 不存在！" data_set_name))))))

(defn get_run_dll [^Ignite ignite ^String sql_line]
    (if-let [{data_set_name :data_set_name is_exists :is_exists} (get_drop_data_set_obj sql_line)]
        (if-let [m_obj (my-alter-dataset/get_ds_obj ignite (str/trim data_set_name))]
            (if (true? (nth m_obj 1))
                (drop_ds_real ignite data_set_name is_exists sql_line)
                (drop_ds ignite data_set_name is_exists sql_line)))))

; 删除数据集
(defn drop_data_set [^Ignite ignite ^Long group_id ^String sql_line]
    (if (= group_id 0)
        (get_run_dll ignite (str/lower-case sql_line))
        (if-let [my_group (.get (.cache ignite "my_users_group") group_id)]
            (let [data_set_id (.getData_set_id my_group) group_type (.getGroup_type my_group)]
                (if (and (contains? #{"ALL" "DDL"} (str/upper-case group_type)) (= data_set_id 0))
                    (get_run_dll ignite (str/lower-case sql_line))
                    (throw (Exception. "用户组没有删除数据集的权限，或者用户组不能的数据集不是元数据集！")))))))













































