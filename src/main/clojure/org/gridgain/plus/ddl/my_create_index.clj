(ns org.gridgain.plus.ddl.my-create-index
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
        :name org.gridgain.plus.dml.MyCreateIndex
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

; item obj
(defn get_item_obj [^String item]
    (let [m (str/split item #"(?i)\s+")]
        (cond (= (count m) 1) {:item_name (str/lower-case (nth m 0))}
              (= (count m) 2) {:item_name (str/lower-case (nth m 0)) :asc_desc (nth m 1)}
              :else
              (throw (Exception. (format "创建索引语句错误！位置：%s" item)))
              )))

; index obj
; 输入："country DESC, city"
; 返回结果：[{:item_name "country" :asc_desc "DESC"}, {:item_name "city"}]
(defn get_index_obj [^String index_items]
    (if-let [lst_items (str/split index_items #"(?i)\s*,\s*")]
        (loop [[f & r] lst_items lst []]
            (if (some? f)
                (recur r (conj lst (get_item_obj f)))
                lst))
        (throw (Exception. "创建索引语句错误！"))))

; 是否是 inline_size
(defn get_inline_size [^String inline]
    (if-let [items (str/split inline #"(?i)\s+INLINE_SIZE\s+")]
        (cond (and (= (count items) 1) (re-find #"^(?i)\(\s*\)$" (nth items 0))) ""
              (and (= (count items) 2) (re-find #"^(?i)\(\s*\)$" (nth items 0))) (nth items 1)
              :else
              (throw (Exception. (format "创建索引语句错误！位置：%s" inline)))
              )
        (throw (Exception. (format "创建索引语句错误！位置：%s" inline)))))

(defn index_exists [^String create_index]
    (if-let [ex (re-find #"(?i)\sIF\sNOT\sEXISTS$" create_index)]
        {:create_index_line create_index :exists true}
        {:create_index_line create_index :exists false}))

(defn index_map
    ([^String create_index] (index_map create_index {:create_index_line create_index} true))
    ([^String create_index dic flag]
     (cond (true? flag) (cond (re-find #"(?i)\sIF\sNOT\sEXISTS$" create_index) (recur create_index (assoc dic :exists true) false)
                              (re-find #"(?i)\sINDEX$" create_index) (recur create_index (assoc dic :exists false) false)
                              :else
                              (throw (Exception. "创建索引语句错误！"))
                              )
           (false? flag) (cond (re-find #"^(?i)CREATE\sSPATIAL\sINDEX\s*" create_index) (recur create_index (assoc dic :spatial true) nil)
                               (re-find #"^(?i)CREATE\sINDEX\s*" create_index) (recur create_index (assoc dic :spatial false) nil)
                               :else
                               (throw (Exception. "创建索引语句错误！"))
                               )
           :else
           dic
           )
     ))

(defn get_create_index_obj [^String sql_line]
    (if-let [sql (my-create-table/get_sql sql_line)]
        (let [create_index_line (re-find #"^(?i)CREATE\sINDEX\sIF\sNOT\sEXISTS\s|^(?i)CREATE\sINDEX\s|^(?i)CREATE\sSPATIAL\sINDEX\sIF\sNOT\sEXISTS\s|^(?i)CREATE\sSPATIAL\sINDEX\s" sql) last_line (str/replace sql #"^(?i)CREATE\sINDEX\sIF\sNOT\sEXISTS\s|^(?i)CREATE\sINDEX\s|^(?i)CREATE\sSPATIAL\sINDEX\sIF\sNOT\sEXISTS\s|^(?i)CREATE\sSPATIAL\sINDEX\s" "")]
            (if (some? create_index_line)
                (let [index_name (re-find #"^(?i)\w+\sON\s" last_line) last_line_1 (str/replace last_line #"^(?i)\w+\sON\s" "")]
                    (if (some? index_name)
                        (let [table_name (re-find #"^(?i)\w+\.\w+\s|^(?i)\w+\s" last_line_1) last_line_2 (str/replace last_line_1 #"^(?i)\w+\.\w+\s|^(?i)\w+\s" "")]
                            (if (some? table_name)
                                (let [index_items (re-find #"(?i)(?<=^\()[\s\S]*(?=\))" last_line_2) last_line_3 (str/replace last_line_2 #"(?i)(?<=^\()[\s\S]*(?=\))" "") {schema_name :schema_name table_name :table_name} (my-lexical/get-schema (str/trim (str/lower-case table_name)))]
                                    (if (some? index_name)
                                        {:create_index (index_map (str/trim create_index_line))
                                         :index_name (str/replace index_name #"(?i)\sON\s$" "")
                                         :schema_name schema_name
                                         :table_name table_name :index_items_obj (get_index_obj (str/trim index_items))
                                         :inline_size (get_inline_size last_line_3)}
                                        ))
                                (throw (Exception. "创建索引语句错误！"))
                                ))
                        (throw (Exception. "创建索引语句错误！"))))
                (throw (Exception. "创建索引语句错误！"))))
        (throw (Exception. "创建索引语句错误！"))))

; 获取 sql
; index_obj = (get_create_index_obj "create index ...")
(defn get_sql_line [index_obj]
    (let [sb (StringBuilder.) sb_items (StringBuilder.)]
        (loop [[f & r] (-> index_obj :index_items_obj)]
            (if (some? f)
                (do
                    (if (and (contains? f :asc_desc) (not (Strings/isNullOrEmpty (-> f :asc_desc))))
                        (doto sb_items
                            (.append (-> f :item_name))
                            (.append " ")
                            (.append (-> f :asc_desc))
                            )
                        (doto sb_items
                            (.append (-> f :item_name))
                            ))
                    (if (> (count r) 0)
                        (doto sb_items (.append ",")))
                    (recur r))))
        (.toString (doto sb
                       (.append (-> index_obj :create_index :create_index_line))
                       (.append " ")
                       (.append (-> index_obj :index_name))
                       (.append " ON ")
                       (.append (-> index_obj :table_name))
                       (.append " (")
                       (.append (.toString sb_items))
                       (.append ")")
                       ))))

(defn get_sql_line_ds [^clojure.lang.PersistentArrayMap index_obj ^String data_set_name]
    (let [sb (StringBuilder.) sb_items (StringBuilder.)]
        (loop [[f & r] (-> index_obj :index_items_obj)]
            (if (some? f)
                (do
                    (if (and (contains? f :asc_desc) (not (Strings/isNullOrEmpty (-> f :asc_desc))))
                        (doto sb_items
                            (.append (-> f :item_name))
                            (.append " ")
                            (.append (-> f :asc_desc))
                            )
                        (doto sb_items
                            (.append (-> f :item_name))
                            ))
                    (if (> (count r) 0)
                        (doto sb_items (.append ",")))
                    (recur r))))
        (.toString (doto sb
                       (.append (-> index_obj :create_index :create_index_line))
                       (.append " ")
                       (.append (format "%s_%s" data_set_name (-> index_obj :index_name)))
                       (.append " ON ")
                       (.append (format "%s_%s" data_set_name (-> index_obj :table_name)))
                       (.append " (")
                       (.append (.toString sb_items))
                       (.append ")")
                       ))))

; 获取反的 ddl
(defn get_un_sql_line [index_obj]
    (let [{index_name :index_name} index_obj sb (StringBuilder.)]
        (.toString (doto sb
                       (.append "DROP INDEX IF EXISTS ")
                       (.append index_name)))))

(defn get_un_sql_line_ds [^clojure.lang.PersistentArrayMap index_obj ^String data_set_name]
    (let [{index_name :index_name} index_obj sb (StringBuilder.)]
        (.toString (doto sb
                       (.append "DROP INDEX IF EXISTS ")
                       (.append (format "%s_%s" data_set_name index_name))))))

; 获取 table_index_item 的 mycacheex
(defn getMyTableIndexItemCache
    ([^Ignite ignite ^Long index_no index_items_obj] (getMyTableIndexItemCache ignite index_no index_items_obj []))
    ([^Ignite ignite ^Long index_no [f & r] lst]
     (if (some? f)
         (if-let [id (.incrementAndGet (.atomicSequence ignite "table_index_item" 0 true))]
             (recur ignite index_no r (conj lst (MyCacheEx. (.cache ignite "table_index_item") (MyTableItemPK. id index_no) (MyTableIndexItem. id (-> f :item_name) (-> f :asc_desc) index_no) (SqlType/INSERT))))
             )
         lst)))

; 获取 mycacheex
(defn myIndexToMyCacheEx [^Ignite ignite index_obj ^Long data_set_id]
    (let [id (.incrementAndGet (.atomicSequence ignite "table_index" 0 true)) {table_name :table_name index_name :index_name index_items_obj :index_items_obj} index_obj]
        (if-let [table_ids (first (.getAll (.query (.cache ignite "my_meta_tables") (.setArgs (SqlFieldsQuery. "select m.id from my_meta_tables as m where m.data_set_id = ? and m.table_name = ?") (to-array [data_set_id (str/lower-case table_name)])))))]
            (if (> (count table_ids) 0)
                (if-let [lst_items (getMyTableIndexItemCache ignite id index_items_obj)]
                    (if (> (count lst_items) 0)
                        (cons (MyCacheEx. (.cache ignite "table_index") (MyTableItemPK. id (nth table_ids 0)) (MyTableIndex. id index_name (-> index_items_obj :spatial) (nth table_ids 0)) (SqlType/INSERT)) lst_items)
                        (throw (Exception. "添加索引的列必须存在！"))))
                (throw (Exception. "表不存在，不能添加索引！")))
            (throw (Exception. "表不存在，不能添加索引！"))))
    )

; 整个 ddl 的 obj
;(defn get_ddl_obj [^Ignite ignite ^String sql_line]
;    (if-let [m (get_create_index_obj sql_line)]
;        {:sql (get_sql_line m) :un_sql (get_un_sql_line m) :lst_cachex (my-lexical/to_arryList (myIndexToMyCacheEx ignite m))}
;        (throw (Exception. "修改表语句错误！请仔细检查并参考文档"))))

; run ddl obj
;(defn run_ddl [^Ignite ignite ^String sql_line]
;    (if-let [m (get_ddl_obj ignite sql_line)]
;        (MyDdlUtil/runDdl ignite m)))

;(defn get_sql_line_all
;    ([^Ignite ignite ^clojure.lang.PersistentArrayMap m]
;     (if-let [lst_ds (my-lexical/get_all_ds ignite (-> m :table_name))]
;         (get_sql_line_all lst_ds m [(get_sql_line m)])
;         [(get_sql_line m)]))
;    ([[f & r] m lst]
;     (if (some? f)
;         (recur r m (conj lst (get_sql_line_ds m f)))
;         lst)))
(defn get_sql_line_all [^Ignite ignite ^clojure.lang.PersistentArrayMap m]
    ())

;(defn get_un_sql_line_all
;    ([^Ignite ignite ^clojure.lang.PersistentArrayMap m]
;     (if-let [lst_ds (my-lexical/get_all_ds ignite (-> m :table_name))]
;         (get_un_sql_line_all lst_ds m [(get_un_sql_line m)])))
;    ([[f & r] m lst]
;     (if (some? f)
;         (recur r m (conj lst (get_un_sql_line_ds m f)))
;         lst)))

(defn get_un_sql_line_all [^Ignite ignite ^clojure.lang.PersistentArrayMap m]
    ())

; 实时数据集
(defn run_ddl_real_time [^Ignite ignite ^String sql_line ^Long data_set_id ^Long group_id]
    (if-let [m (get_create_index_obj sql_line)]
        (if-not (my-lexical/is-eq? (-> m :schema_name) "my_meta")
            (if (true? (.isDataSetEnabled (.configuration ignite)))
                (let [ddl_id (.incrementAndGet (.atomicSequence ignite "ddl_log" 0 true))]
                    (MyDdlUtil/runDdl ignite {:sql (my-lexical/to_arryList (get_sql_line_all ignite m)) :un_sql (my-lexical/to_arryList (get_un_sql_line_all ignite m)) :lst_cachex (doto (my-lexical/to_arryList (myIndexToMyCacheEx ignite m data_set_id)) (.add (MyCacheEx. (.cache ignite "ddl_log") ddl_id (DdlLog. ddl_id group_id sql_line data_set_id) (SqlType/INSERT))))})
                    )
                (MyDdlUtil/runDdl ignite {:sql (my-lexical/to_arryList (get_sql_line_all ignite m)) :un_sql (my-lexical/to_arryList (get_un_sql_line_all ignite m)) :lst_cachex (my-lexical/to_arryList (myIndexToMyCacheEx ignite m data_set_id))}))
            (throw (Exception. "没有执行语句的权限！")))
        (throw (Exception. "修改表语句错误！请仔细检查并参考文档"))))

; 批处理数据集
(defn run_ddl [^Ignite ignite ^String sql_line ^Long data_set_id ^Long group_id]
    (if-let [m (get_create_index_obj sql_line)]
        (if-not (my-lexical/is-eq? (-> m :schema_name) "my_meta")
            (if (true? (.isDataSetEnabled (.configuration ignite)))
                (let [ddl_id (.incrementAndGet (.atomicSequence ignite "ddl_log" 0 true))]
                    (MyDdlUtil/runDdl ignite {:sql (my-lexical/to_arryList [(get_sql_line m)]) :un_sql (my-lexical/to_arryList [(get_un_sql_line m)]) :lst_cachex (doto (my-lexical/to_arryList (myIndexToMyCacheEx ignite m data_set_id)) (.add (MyCacheEx. (.cache ignite "ddl_log") ddl_id (DdlLog. ddl_id group_id sql_line data_set_id) (SqlType/INSERT))))})
                    )
                (MyDdlUtil/runDdl ignite {:sql (my-lexical/to_arryList [(get_sql_line m)]) :un_sql (my-lexical/to_arryList [(get_un_sql_line m)]) :lst_cachex (my-lexical/to_arryList (myIndexToMyCacheEx ignite m data_set_id))}))
            (throw (Exception. "没有执行语句的权限！")))
        (throw (Exception. "修改表语句错误！请仔细检查并参考文档"))))

; 新增 index
(defn create_index [^Ignite ignite ^Long group_id ^String dataset_name ^String group_type ^Long dataset_id ^String sql_line]
    (let [sql_code (str/lower-case sql_line)]
        (if (= group_id 0)
            (run_ddl_real_time ignite sql_code 0 group_id)
            (if (contains? #{"ALL" "DDL"} group_type)
                (run_ddl_real_time ignite sql_code dataset_id group_id)
                (throw (Exception. "该用户组没有执行 DDL 语句的权限！")))
            )))


































































