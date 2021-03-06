(ns org.gridgain.plus.ddl.my-create-index
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

(defn create-index-obj [^Ignite ignite ^String data_set_name ^String sql_line]
    (letfn [(get-table-id [^Ignite ignite ^String data_set_name ^String table_name]
                (if (my-lexical/is-eq? "public" data_set_name)
                    (first (first (.getAll (.query (.cache ignite "my_meta_tables") (.setArgs (SqlFieldsQuery. "select m.id from my_meta_tables as m where m.data_set_id = 0 and m.table_name = ?") (to-array [(str/lower-case table_name)]))))))
                    (first (first (.getAll (.query (.cache ignite "my_meta_tables") (.setArgs (SqlFieldsQuery. "select m.id from my_meta_tables as m, my_dataset as d where m.data_set_id = d.id and d.dataset_name = ? and m.table_name = ?") (to-array [(str/lower-case data_set_name) (str/lower-case table_name)])))))))
                )
            (get-cachex [^Ignite ignite ^clojure.lang.PersistentArrayMap m ^ArrayList lst]
                (let [{index_name :index_name schema_name :schema_name table_name :table_name index_items_obj :index_items_obj {spatial :spatial} :create_index} m]
                    (if-let [table_id (get-table-id ignite schema_name table_name)]
                        (let [index_id (.incrementAndGet (.atomicSequence ignite "table_index" 0 true))]
                            (loop [[f & r] index_items_obj]
                                (if (some? f)
                                    (do
                                        (let [index_item_id (.incrementAndGet (.atomicSequence ignite "table_index_item" 0 true)) {index_item :item_name sort_order :asc_desc} f]
                                            (doto lst (.add (MyCacheEx. (.cache ignite "table_index_item") (MyTableItemPK. index_item_id index_id) (MyTableIndexItem. index_item_id index_item sort_order index_id) (SqlType/INSERT)))))
                                        (recur r))))
                            (doto lst (.add (MyCacheEx. (.cache ignite "table_index") (MyTableItemPK. index_id table_id) (MyTableIndex. index_id (format "%s_%s" schema_name index_name) spatial table_id) (SqlType/INSERT))))
                            ))))
            (get-index-obj [^String data_set_name ^String sql_line]
                (let [m (get_create_index_obj sql_line)]
                    (cond (and (= (-> m :schema_name) "") (not (= data_set_name ""))) (assoc m :schema_name data_set_name)
                          (or (and (not (= (-> m :schema_name) "")) (my-lexical/is-eq? data_set_name "MY_META")) (and (not (= (-> m :schema_name) "")) (my-lexical/is-eq? (-> m :schema_name) data_set_name))) m
                          :else
                          (throw (Exception. "没有创建索引的权限！"))
                          )))
            (get-index-items-line
                ([index_items_obj] (get-index-items-line index_items_obj (StringBuilder.)))
                ([[f & r] ^StringBuilder sb]
                 (letfn [(item-line [item]
                             (if (contains? item :asc_desc)
                                 (format "%s %s" (-> item :item_name) (-> item :asc_desc))
                                 (-> item :item_name)))]
                     (if (some? f)
                         (if (nil? r)
                             (recur r (doto sb (.append (item-line f))))
                             (recur r (doto sb (.append (str (item-line f) ","))))
                             )
                         (.toString sb)))))
            ]
        (let [m (get-index-obj data_set_name sql_line)]
            {:sql (format "%s %s_%s ON %s.%s (%s)" (-> m :create_index :create_index_line) (-> m :schema_name) (-> m :index_name) (-> m :schema_name) (-> m :table_name) (get-index-items-line (-> m :index_items_obj)))
             :un_sql (format  "DROP INDEX IF EXISTS %s_%s" (-> m :schema_name) (-> m :index_name))
             :lst_cachex (get-cachex ignite m (ArrayList.))})))

; 实时数据集
;(defn run_ddl_real_time [^Ignite ignite ^String sql_line ^Long data_set_id ^Long group_id]
;    (if-let [m (get_create_index_obj sql_line)]
;        (if-not (my-lexical/is-eq? (-> m :schema_name) "my_meta")
;            (if (true? (.isMyLogEnabled (.configuration ignite)))
;                (let [ddl_id (.incrementAndGet (.atomicSequence ignite "ddl_log" 0 true))]
;                    (MyDdlUtil/runDdl ignite {:sql (my-lexical/to_arryList (get_sql_line_all ignite m)) :un_sql (my-lexical/to_arryList (get_un_sql_line_all ignite m)) :lst_cachex (doto (my-lexical/to_arryList (myIndexToMyCacheEx ignite m data_set_id)) (.add (MyCacheEx. (.cache ignite "ddl_log") ddl_id (DdlLog. ddl_id group_id sql_line data_set_id) (SqlType/INSERT))))})
;                    )
;                (MyDdlUtil/runDdl ignite {:sql (my-lexical/to_arryList (get_sql_line_all ignite m)) :un_sql (my-lexical/to_arryList (get_un_sql_line_all ignite m)) :lst_cachex (my-lexical/to_arryList (myIndexToMyCacheEx ignite m data_set_id))}))
;            (throw (Exception. "没有执行语句的权限！")))
;        (throw (Exception. "修改表语句错误！请仔细检查并参考文档"))))

(defn run_ddl_real_time [^Ignite ignite ^String sql_line ^String dataset_name]
    (let [{sql :sql un_sql :un_sql lst_cachex :lst_cachex} (create-index-obj ignite dataset_name sql_line)]
        (if-not (nil? lst_cachex)
            (MyDdlUtil/runDdl ignite {:sql (doto (ArrayList.) (.add sql)) :un_sql (doto (ArrayList.) (.add un_sql)) :lst_cachex lst_cachex})
            (throw (Exception. "没有执行语句的权限！")))
        ))

; 新增 index
(defn create_index [^Ignite ignite ^Long group_id ^String dataset_name ^String group_type ^Long dataset_id ^String sql_line]
    (let [sql_code (str/lower-case sql_line)]
        (if (= group_id 0)
            (run_ddl_real_time ignite dataset_name sql_code)
            (if (contains? #{"ALL" "DDL"} (str/upper-case group_type))
                (run_ddl_real_time ignite dataset_name sql_code)
                (throw (Exception. "该用户组没有执行 DDL 语句的权限！")))
            )))


































































