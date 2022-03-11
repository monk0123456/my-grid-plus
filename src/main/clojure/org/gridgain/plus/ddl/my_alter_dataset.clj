(ns org.gridgain.plus.ddl.my-alter-dataset
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.ddl.my-create-table :as my-create-table]
        [org.gridgain.plus.ddl.my-create-dataset-0 :as my-create-dataset]
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
             (org.gridgain.dml.util MyCacheExUtil)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache CacheMode CacheAtomicityMode)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (org.apache.ignite.binary BinaryObjectBuilder BinaryObject)
             (org.gridgain.ddl MyCreateTableUtil MyDdlUtil)
             (java.util ArrayList Date Iterator)
             (java.sql Timestamp)
             (java.math BigDecimal)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyAlterDataSet
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

(defn add_or_drop [^String line]
    (cond (not (nil? (re-find #"^(?i)DROP\s+IF\s+EXISTS\s*" line))) {:line line :is_drop true :is_add false :is_exists true :is_no_exists false}
          (not (nil? (re-find #"^(?i)ADD\s+IF\s+NOT\s+EXISTS\s*" line))) {:line line :is_drop false :is_add true :is_exists false :is_no_exists true}
          (not (nil? (re-find #"^(?i)DROP\s+" line))) {:line line :is_drop true :is_add false :is_exists false :is_no_exists false}
          (not (nil? (re-find #"^(?i)ADD\s+" line))) {:line line :is_drop false :is_add true :is_exists false :is_no_exists false}
          :else
          (throw (Exception. (format "修改数据集的语句错误！位置：%s" line)))
          ))

; 判断 items
(defn show_error [[f & r]]
    (if (some? f)
        (if (not (Strings/isNullOrEmpty (str/trim f)))
            (throw (Exception. "表名必须是字符类型！"))
            (recur r))
        true))

; 获取 alter obj
(defn get_data_set_alter_obj [^String sql_line]
    (if-let [sql (my-create-table/get_sql sql_line)]
        (let [alter_dataset (re-find #"^(?i)ALTER\sDATASET\sIF\sEXISTS\s|^(?i)ALTER\sDATASET\s" sql) last_line (str/replace sql #"^(?i)ALTER\sDATASET\sIF\sEXISTS\s|^(?i)ALTER\sDATASET\s" "")]
            (if (some? alter_dataset)
                (let [data_set_name (re-find #"^(?i)\w+\s" last_line) last_line_1 (str/replace last_line #"^(?i)\w+\s" "")]
                    (if (some? data_set_name)
                        (let [add_or_drop_line (re-find #"^(?i)ADD\sIF\sNOT\sEXISTS\s|^(?i)DROP\sIF\sEXISTS\s|^(?i)ADD\s|^(?i)DROP\s" last_line_1) colums_line (str/replace last_line_1 #"^(?i)ADD\sIF\sNOT\sEXISTS\s|^(?i)DROP\sIF\sEXISTS\s|^(?i)ADD\s|^(?i)DROP\s" "")]
                            (if (show_error (filter #(nil? (re-find #"^\w+$" (str/trim %))) (str/split colums_line #"\s*\(\s*|\s*\)\s*|\s*,\s*")))
                                {:alter_dataset alter_dataset :data_set_name data_set_name :add_or_drop (add_or_drop add_or_drop_line) :colums (filter #(not (nil? (re-find #"^\w+$" (str/trim %)))) (str/split colums_line #"\s*\(\s*|\s*\)\s*|\s*,\s*"))})
                            )
                        (throw (Exception. (format "修改数据集的语句错误！位置：%s" last_line)))))
                (throw (Exception. "修改数据集的语句错误！"))))
        (throw (Exception. "修改数据集的语句错误！"))))

(defn get_ds_obj [^Ignite ignite ^String data_set_name]
    (.getAll (.query (.cache ignite "my_dataset") (.setArgs (SqlFieldsQuery. "select m.id, m.is_real from my_dataset as m where m.dataset_name = ?") (to-array [data_set_name])))))

; 判断是否实时数据集表中是否存在这个表
(defn has_table [^Ignite ignite ^String data_set_table ^Long dataset_id ^String table_name]
    (if-let [m (.getAll (.query (.cache ignite "my_dataset") (.setArgs (SqlFieldsQuery. "select count(m.id) from ? as m where m.table_name = ? and m.dataset_id = ?") (to-array [data_set_table table_name dataset_id]))))]
        (if (zero? (first (first m)))
            false
            true)))

(defn add_to_my_dataset_table_real
    ([^Ignite ignite lst_colums ^Long dataset_id ^Boolean is_exists] (add_to_my_dataset_table_real ignite lst_colums dataset_id is_exists []))
    ([^Ignite ignite [f_colum_name & r_colum_name] ^Long dataset_id ^Boolean is_exists lst_cache]
     (if (some? f_colum_name)
         (if-let [has_table_name (has_table ignite "my_dataset_real_table" dataset_id f_colum_name)]
             (cond (and (true? is_exists) (true? has_table_name)) (recur ignite r_colum_name dataset_id is_exists lst_cache)
                   (and (true? is_exists) (false? has_table_name)) (let [table_id (.incrementAndGet (.atomicSequence ignite "my_dataset_real_table" 0 true))]
                                                                       (recur ignite r_colum_name dataset_id is_exists (conj lst_cache (MyCacheEx. (.cache ignite "my_dataset_real_table") (MyDatasetTablePK. table_id dataset_id) (MyDatasetRealTable. table_id f_colum_name dataset_id) (SqlType/INSERT)))))
                   (and (false? is_exists) (true? has_table_name)) (throw (Exception. (format "该数据集已经存在数据表：%s 不能在添加了！" f_colum_name)))
                   (and (false? is_exists) (false? has_table_name)) (let [table_id (.incrementAndGet (.atomicSequence ignite "my_dataset_real_table" 0 true))]
                                                                        (recur ignite r_colum_name dataset_id is_exists (conj lst_cache (MyCacheEx. (.cache ignite "my_dataset_real_table") (MyDatasetTablePK. table_id dataset_id) (MyDatasetRealTable. table_id f_colum_name dataset_id) (SqlType/INSERT)))))
                   ))
         lst_cache)))

(defn drop_to_my_dataset_table_real
    ([^Ignite ignite lst_colums ^Long dataset_id ^Boolean is_exists] (drop_to_my_dataset_table_real ignite lst_colums dataset_id is_exists []))
    ([^Ignite ignite [f_colum_name & r_colum_name] ^Long dataset_id ^Boolean is_exists lst_cache]
     (if (some? f_colum_name)
         (if-let [has_table_name (has_table ignite "my_dataset_real_table" dataset_id f_colum_name)]
             (cond (and (true? is_exists) (true? has_table_name)) (recur ignite r_colum_name dataset_id is_exists lst_cache)
                   (and (true? is_exists) (false? has_table_name)) (let [table_id (.incrementAndGet (.atomicSequence ignite "my_dataset_real_table" 0 true))]
                                                                       (recur ignite r_colum_name dataset_id is_exists (conj lst_cache (MyCacheEx. (.cache ignite "my_dataset_real_table") (MyDatasetTablePK. table_id dataset_id) nil (SqlType/DELETE)))))
                   (and (false? is_exists) (true? has_table_name)) (throw (Exception. (format "该数据集已经存在数据表：%s 不能在添加了！" f_colum_name)))
                   (and (false? is_exists) (false? has_table_name)) (let [table_id (.incrementAndGet (.atomicSequence ignite "my_dataset_real_table" 0 true))]
                                                                        (recur ignite r_colum_name dataset_id is_exists (conj lst_cache (MyCacheEx. (.cache ignite "my_dataset_real_table") (MyDatasetTablePK. table_id dataset_id) nil (SqlType/DELETE)))))
                   ))
         lst_cache)))

(defn get_index_line [^Ignite ignite ^Long index_no]
    (loop [[f_item & r_item] (.getAll (.query (.cache ignite "table_index") (.setArgs (SqlFieldsQuery. "SELECT m.index_item, m.sort_order FROM MY_META.table_index_item as m WHERE m.index_no = ?") (to-array [index_no])))) sb (StringBuilder.)]
        (if (some? f_item)
            (if (some? r_item)
                (if-not (Strings/isNullOrEmpty (nth f_item 1))
                    (recur r_item (doto sb (.append (format "%s %s," (nth f_item 0) (nth f_item 1)))))
                    (recur r_item (doto sb (.append (format "%s," (nth f_item 0))))))
                (if-not (Strings/isNullOrEmpty (nth f_item 1))
                    (recur r_item (doto sb (.append (format "%s %s" (nth f_item 0) (nth f_item 1)))))
                    (recur r_item (doto sb (.append (format "%s" (nth f_item 0)))))))
            (.toString sb))))

; 获取非实时的 index 定义
(defn get_indexs_by_table_name [^Ignite ignite ^String table_name ^String data_set_name ^Long data_set_id]
    (loop [[f_index & r_index] (.getAll (.query (.cache ignite "my_meta_tables") (.setArgs (SqlFieldsQuery. "SELECT m.id, m.index_name, m.spatial from table_index as m INNER JOIN my_meta_tables AS t ON t.id = m.table_id where t.table_name = ? AND t.data_set_id = ?") (to-array [table_name data_set_id])))) lst []]
        (if (some? f_index)
            (let [[id index_name spatial] f_index]
                (if (true? spatial)
                    (recur r_index (conj lst {:sql (format "CREATE SPATIAL INDEX %s_%s ON %s_%s (%s)" data_set_name index_name data_set_name table_name (get_index_line ignite id)) :un_sql (format "DROP INDEX %s_%s" data_set_name index_name)}))
                    (recur r_index (conj lst {:sql (format "CREATE INDEX %s_%s ON %s_%s (%s)" data_set_name index_name data_set_name table_name (get_index_line ignite id)) :un_sql (format "DROP INDEX %s_%s" data_set_name index_name)}))
                    )
                )
            lst)
        ))

; 非实时还要创建表
(defn add_to_my_dataset_table
    ([^Ignite ignite lst_colums ^String data_set_name ^Long dataset_id ^Boolean is_exists] (add_to_my_dataset_table ignite lst_colums data_set_name dataset_id is_exists [] []))
    ([^Ignite ignite [f_colum_name & r_colum_name] ^String data_set_name ^Long dataset_id ^Boolean is_exists lst_cache lst_tables]
     (if (some? f_colum_name)
         (if-let [has_table_name (has_table ignite "my_dataset_table" dataset_id f_colum_name)]
             (cond (and (true? is_exists) (true? has_table_name)) (recur ignite r_colum_name data_set_name dataset_id is_exists lst_cache (concat lst_tables (my-create-dataset/get_lst_ddl ignite data_set_name f_colum_name)))
                   (and (true? is_exists) (false? has_table_name)) (let [table_id (.incrementAndGet (.atomicSequence ignite "my_dataset_table" 0 true))]
                                                                       (recur ignite r_colum_name data_set_name dataset_id is_exists (conj lst_cache (MyCacheEx. (.cache ignite "my_dataset_table") (MyDatasetTablePK. table_id dataset_id) (MyDatasetTable. table_id f_colum_name dataset_id false) (SqlType/INSERT))) (concat lst_tables (my-create-dataset/get_lst_ddl ignite data_set_name f_colum_name) (get_indexs_by_table_name ignite f_colum_name data_set_name dataset_id))))
                   (and (false? is_exists) (true? has_table_name)) (throw (Exception. (format "该数据集已经存在数据表：%s 不能在添加了！" f_colum_name)))
                   (and (false? is_exists) (false? has_table_name)) (let [table_id (.incrementAndGet (.atomicSequence ignite "my_dataset_table" 0 true))]
                                                                        (recur ignite r_colum_name data_set_name dataset_id is_exists (conj lst_cache (MyCacheEx. (.cache ignite "my_dataset_table") (MyDatasetTablePK. table_id dataset_id) (MyDatasetTable. table_id f_colum_name dataset_id false) (SqlType/INSERT))) (concat lst_tables (my-create-dataset/get_lst_ddl ignite data_set_name f_colum_name) (get_indexs_by_table_name ignite f_colum_name data_set_name dataset_id))))
                   ))
         {:lst_cache lst_cache :ddl lst_tables})))

; 删除相应的表和索引
(defn drop_table_index [^Ignite ignite ^String data_set_name ^Long data_set_id ^String table_name]
    (loop [[f_index_name & r_index_name] (.getAll (.query (.cache ignite "my_meta_tables") (.setArgs (SqlFieldsQuery. "SELECT DISTINCT m.index_name from table_index as m INNER JOIN my_meta_tables AS t ON t.id = m.table_id where t.table_name = ? AND t.data_set_id = ?") (to-array [table_name data_set_id])))) lst []]
        (if (some? f_index_name)
            (recur r_index_name (conj lst (format "DROP INDEX %s_%s" data_set_name f_index_name)))
            (concat [(format "DROP TABLE %s_%s" data_set_name table_name)] lst))))

(defn drop_to_my_dataset_table
    ([^Ignite ignite lst_colums ^String data_set_name ^Long dataset_id ^Boolean is_exists] (drop_to_my_dataset_table ignite lst_colums data_set_name dataset_id is_exists [] []))
    ([^Ignite ignite [f_colum_name & r_colum_name] ^String data_set_name ^Long dataset_id ^Boolean is_exists lst_cache lst_tables]
     (if (some? f_colum_name)
         (if-let [has_table_name (has_table ignite "my_dataset_table" dataset_id f_colum_name)]
             (cond (and (true? is_exists) (true? has_table_name)) (recur ignite r_colum_name data_set_name dataset_id is_exists lst_cache (concat lst_tables (my-create-dataset/get_lst_ddl ignite data_set_name f_colum_name)))
                   (and (true? is_exists) (false? has_table_name)) (let [table_id (.incrementAndGet (.atomicSequence ignite "my_dataset_table" 0 true))]
                                                                       (recur ignite r_colum_name data_set_name dataset_id is_exists (conj lst_cache (MyCacheEx. (.cache ignite "my_dataset_table") (MyDatasetTablePK. table_id dataset_id) nil (SqlType/DELETE))) (concat lst_tables (drop_table_index ignite data_set_name dataset_id f_colum_name))))
                   (and (false? is_exists) (true? has_table_name)) (throw (Exception. (format "该数据集已经存在数据表：%s 不能在添加了！" f_colum_name)))
                   (and (false? is_exists) (false? has_table_name)) (let [table_id (.incrementAndGet (.atomicSequence ignite "my_dataset_table" 0 true))]
                                                                        (recur ignite r_colum_name data_set_name dataset_id is_exists (conj lst_cache (MyCacheEx. (.cache ignite "my_dataset_table") (MyDatasetTablePK. table_id dataset_id) nil (SqlType/DELETE))) (concat lst_tables (drop_table_index ignite data_set_name dataset_id f_colum_name))))
                   ))
         {:lst_cache lst_cache :ddl lst_tables})))

(defn get_run_dll [^Ignite ignite ^String sql_line]
    (if-let [{data_set_name :data_set_name add_or_drop :add_or_drop colums :colums} (get_data_set_alter_obj sql_line)]
        (if-let [m_obj (get_ds_obj ignite (str/trim data_set_name))]
            (if (true? (-> add_or_drop :is_add))
                (if (true? (nth m_obj 1))
                    ; 把 colums 中的表名添加到对应的 my_dataset_table 表中
                    (let [lst_cache (add_to_my_dataset_table_real ignite colums (nth m_obj 0) (-> add_or_drop :is_exists))]
                        (if (true? (.isDataSetEnabled (.configuration ignite)))
                            (let [ds_id (.incrementAndGet (.atomicSequence ignite "dataset_ddl_log" 0 true))]
                                (MyDdlUtil/runDdl ignite {:lst_cachex (doto (my-lexical/to_arryList lst_cache) (.add (MyCacheEx. (.cache ignite "dataset_ddl_log") ds_id (DataSetDdlLog. ds_id (-> m_obj :data_set_name) "alter" sql_line) (SqlType/INSERT))))}))
                            (MyDdlUtil/runDdl ignite {:lst_cachex (my-lexical/to_arryList lst_cache)}))
                        )
                    (let [{ddl :ddl lst_cache :lst_cache} (add_to_my_dataset_table ignite colums data_set_name (nth m_obj 0) (-> add_or_drop :is_exists))]
                        (let [lst_ddl (my-create-dataset/get_sql_lst ddl)]
                            (if (true? (.isDataSetEnabled (.configuration ignite)))
                                (let [ds_id (.incrementAndGet (.atomicSequence ignite "dataset_ddl_log" 0 true))]
                                    (MyDdlUtil/runDdl ignite (assoc lst_ddl :lst_cachex (doto (my-lexical/to_arryList lst_cache) (.add (MyCacheEx. (.cache ignite "dataset_ddl_log") ds_id (DataSetDdlLog. ds_id (-> m_obj :data_set_name) "alter" sql_line) (SqlType/INSERT)))))))
                                (MyDdlUtil/runDdl ignite (assoc lst_ddl :lst_cachex (my-lexical/to_arryList lst_cache))))
                            )
                        )
                    )
                ; drop table
                (if (true? (nth m_obj 1))
                    ; 把 colums 中的表名添加到对应的 my_dataset_table 表中
                    (let [lst_cache (drop_to_my_dataset_table_real ignite colums (nth m_obj 0) (-> add_or_drop :is_exists))]
                        (if (true? (.isDataSetEnabled (.configuration ignite)))
                            (let [ds_id (.incrementAndGet (.atomicSequence ignite "dataset_ddl_log" 0 true))]
                                (MyDdlUtil/runDdl ignite {:lst_cachex (doto (my-lexical/to_arryList lst_cache) (.add (MyCacheEx. (.cache ignite "dataset_ddl_log") ds_id (DataSetDdlLog. ds_id (-> m_obj :data_set_name) "alter" sql_line) (SqlType/INSERT))))}))
                            (MyDdlUtil/runDdl ignite {:lst_cachex (my-lexical/to_arryList lst_cache)})))
                    (let [{ddl :ddl lst_cache :lst_cache} (drop_to_my_dataset_table ignite colums data_set_name (nth m_obj 0) (-> add_or_drop :is_exists))]
                        (let [lst_ddl (my-create-dataset/get_sql_lst ddl)]
                            (if (true? (.isDataSetEnabled (.configuration ignite)))
                                (let [ds_id (.incrementAndGet (.atomicSequence ignite "dataset_ddl_log" 0 true))]
                                    (MyDdlUtil/runDdl ignite (assoc lst_ddl :lst_cachex (doto (my-lexical/to_arryList lst_cache) (.add (MyCacheEx. (.cache ignite "dataset_ddl_log") ds_id (DataSetDdlLog. ds_id (-> m_obj :data_set_name) "alter" sql_line) (SqlType/INSERT)))))))
                                (MyDdlUtil/runDdl ignite (assoc lst_ddl :lst_cachex (my-lexical/to_arryList lst_cache))))
                            )
                        )
                    )
                )
            (throw (Exception. (format "要修改的数据集：%s 不存在！" data_set_name))))
        (throw (Exception. "修改数据集的语句错误！"))))

; 修改数据集
(defn alter_data_set [^Ignite ignite ^Long group_id ^String sql_line]
    (if (= group_id 0)
        (get_run_dll ignite (str/lower-case sql_line))
        (if-let [my_group (.get (.cache ignite "my_users_group") group_id)]
            (let [data_set_id (.getData_set_id my_group) group_type (.getGroup_type my_group)]
                (if (and (contains? #{"ALL" "DDL"} group_type) (= data_set_id 0))
                    (get_run_dll ignite (str/lower-case sql_line))
                    (throw (Exception. "用户组没有修改数据集的权限，或者用户组不能的数据集不是元数据集！")))))))











































