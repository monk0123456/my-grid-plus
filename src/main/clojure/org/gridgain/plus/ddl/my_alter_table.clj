(ns org.gridgain.plus.ddl.my-alter-table
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
             (java.math BigDecimal))
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyAlterTable
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

; 获取要添加或删除的 item 定义
(defn get_items_line [items_line]
    (if (and (= (first items_line) \() (= (last items_line) \)))
        (str/trim (subs items_line 1 (- (count items_line) 1)))
        (str/trim items_line)
        ))

(defn get_items_obj [items_line]
    (my-create-table/items_obj (my-create-table/get_items (my-lexical/to-back (get_items_line items_line)))))

(defn get_obj
    ([items] (get_obj items (ArrayList.) (StringBuilder.)))
    ([[f & r] ^ArrayList lst ^StringBuilder sb]
     (if (some? f)
         (when-let [{table_item :table_item code_line :code} (my-create-table/to_item f)]
             (do
                 (.add lst table_item)
                 (.append sb (.concat (.trim (.toString code_line)) ","))
                 (recur r lst sb)
                 ))
         (let [code (str/trim (.toString sb))]
             {:lst_table_item (my-create-table/table_items lst) :code_line (str/trim (subs code 0 (- (count code) 1)))})
         )))

(defn add_or_drop [^String line]
    (cond (not (nil? (re-find #"^(?i)DROP\s+COLUMN\s+IF\s+EXISTS\s*" line))) {:line line :is_drop true :is_add false :is_exists true :is_no_exists false}
          (not (nil? (re-find #"^(?i)ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s*" line))) {:line line :is_drop false :is_add true :is_exists false :is_no_exists true}
          (not (nil? (re-find #"^(?i)DROP\s+COLUMN\s*" line))) {:line line :is_drop true :is_add false :is_exists false :is_no_exists false}
          (not (nil? (re-find #"^(?i)ADD\s+COLUMN\s*" line))) {:line line :is_drop false :is_add true :is_exists false :is_no_exists false}
          :else
          (throw (Exception. (format "修改表的语句错误！位置：%s" line)))
          ))

; 获取 alter obj
(defn get_table_alter_obj [^String sql_line]
    (if-let [sql (my-create-table/get_sql sql_line)]
        (let [alter_table (re-find #"^(?i)ALTER\sTABLE\sIF\sEXISTS\s|^(?i)ALTER\sTABLE\s" sql) last_line (str/replace sql #"^(?i)ALTER\sTABLE\sIF\sEXISTS\s|^(?i)ALTER\sTABLE\s" "")]
            (if (some? alter_table)
                (let [table_name (re-find #"^(?i)\w+\s" last_line) last_line_1 (str/replace last_line #"^(?i)\w+\s" "")]
                    (if (some? table_name)
                        (let [add_or_drop_line (re-find #"^(?i)ADD\sCOLUMN\sIF\sNOT\sEXISTS\s|^(?i)DROP\sCOLUMN\sIF\sNOT\sEXISTS\s|^(?i)ADD\sCOLUMN\sIF\sEXISTS\s|^(?i)DROP\sCOLUMN\sIF\sEXISTS\s|^(?i)ADD\sCOLUMN\s|^(?i)DROP\sCOLUMN\s|^(?i)ADD\s|^(?i)DROP\s" last_line_1) colums_line (str/replace last_line_1 #"^(?i)ADD\sCOLUMN\sIF\sNOT\sEXISTS\s|^(?i)DROP\sCOLUMN\sIF\sNOT\sEXISTS\s|^(?i)ADD\sCOLUMN\sIF\sEXISTS\s|^(?i)DROP\sCOLUMN\sIF\sEXISTS\s|^(?i)ADD\sCOLUMN\s|^(?i)DROP\sCOLUMN\s|^(?i)ADD\s|^(?i)DROP\s" "")]
                            ;(println (get_items_obj colums_line))
                            {:alter_table alter_table :table_name table_name :add_or_drop (add_or_drop add_or_drop_line) :colums (get_obj (get_items_obj colums_line))}
                            )
                        (throw (Exception. (format "修改表的语句错误！位置：%s" table_name)))))
                (throw (Exception. "修改表的语句错误！"))))
        (throw (Exception. "修改表的语句错误！"))))

; 保存到数据库
(defn get_sql_line [table_obj]
    (let [sb (StringBuilder.)]
        (.toString (doto sb
                       (.append (-> table_obj :alter_table))
                       (.append (-> table_obj :table_name))
                       (.append (-> (-> table_obj :add_or_drop) :line))
                       (.append (-> (-> table_obj :colums) :code_line))
                       ))))

(defn get_un_sql_line [table_obj]
    (let [sb (StringBuilder.) add_or_drop_sb (StringBuilder.)]
        (let [{is_drop :is_drop} (-> table_obj :add_or_drop)]
            (if (true? is_drop)
                (.append add_or_drop_sb "ADD COLUMN ")
                (.append add_or_drop_sb "DROP COLUMN "))
            )
        (.toString (doto sb
                       (.append (-> table_obj :alter_table))
                       (.append (-> table_obj :table_name))
                       (.append (.toString add_or_drop_sb))
                       (.append (-> (-> table_obj :colums) :code_line))
                       ))))

; lst 为 obj 的 lst_table_item
(defn myTableItemToMyCacheEx
    ([^Ignite ignite ^Boolean is_add ^String table_name ^Long data_set_id lst]
     (if (true? is_add)
         (if-let [ids (first (.getAll (.query (.cache ignite "my_meta_tables") (.setArgs (SqlFieldsQuery. "select m.id from my_meta_tables as m where m.data_set_id = ? and m.table_name = ?") (to-array [data_set_id table_name])))))]
             (if (and (some? ids) (> (count ids) 0))
                 (myTableItemToMyCacheEx data_set_id ignite is_add (nth ids 0) lst []))
             (throw (Exception. (format "%s: 表不存在！" table_name))))
         (myTableItemToMyCacheEx data_set_id ignite is_add nil lst []))
     )
    ([^Long data_set_id ^Ignite ignite ^Boolean is_add ^Long table_id [f & r] lst]
     (if (some? f)
         (if (true? is_add)
             (recur data_set_id ignite is_add table_id r (conj lst (MyCacheEx. (.cache ignite "table_item") (MyTableItemPK. (.incrementAndGet (.atomicSequence ignite "table_item" 0 true)) table_id) f (SqlType/INSERT))))
             (recur data_set_id ignite is_add table_id r (conj lst (MyCacheEx. (.cache ignite "table_item") (MyTableItemPK. (.getId f) (.getTable_id f)) nil (SqlType/DELETE)))))
         (my-lexical/to_arryList lst))))

; 整个 ddl 的 obj
;(defn get_ddl_obj [^Ignite ignite ^String sql_line]
;    (if-let [m (get_table_alter_obj sql_line)]
;        {:sql (get_sql_line m) :un_sql (get_un_sql_line m) :lst_cachex (myTableItemToMyCacheEx ignite (-> m :add_or_drop :is_add) (str/trim (str/lower-case (-> m :table_name))) (-> (-> m :colums) :lst_table_item))}
;        (throw (Exception. "修改表语句错误！请仔细检查并参考文档"))))

(defn get_ds_obj [^Ignite ignite ^Long data_set_id [f & r] m lst]
    (if (some? f)
        (let [m1 (assoc m :table_name (format "%s_%s" (first f) (-> m :table_name)))]
            (recur ignite data_set_id r m (concat lst [{:sql (get_sql_line m1) :un_sql (get_un_sql_line m1)}])))
        (concat lst [{:sql (get_sql_line m) :un_sql (get_un_sql_line m) :lst_cachex (myTableItemToMyCacheEx ignite (-> m :add_or_drop :is_add) (str/trim (-> m :table_name)) data_set_id (-> (-> m :colums) :lst_table_item))}])))

(defn get_lst_obj
    ([^Ignite ignite ^String sql_line ^Long data_set_id] (get_lst_obj ignite sql_line data_set_id []))
    ([^Ignite ignite ^String sql_line ^Long data_set_id ^clojure.lang.PersistentVector lst]
     (if-let [m (get_table_alter_obj sql_line)]
         (get_ds_obj ignite data_set_id (my-lexical/get_all_ds ignite (str/trim (-> m :table_name))) m lst))))

(defn get_ddl_objs
    ([lst ^Long group_id ^String sql_line ^Ignite ignite data_set_id] (get_ddl_objs lst [] [] [] group_id sql_line ignite data_set_id))
    ([[f & r] lst_sql lst_un_sql lst_cachex ^Long group_id ^String sql_line ^Ignite ignite data_set_id]
     (if (some? f)
         (let [{sql :sql un_sql :un_sql cachex :lst_cachex} f]
             (if (nil? cachex)
                 (recur r (concat lst_sql [sql]) (concat lst_un_sql [un_sql]) lst_cachex group_id sql_line ignite data_set_id)
                 (recur r (concat lst_sql [sql]) (concat lst_un_sql [un_sql]) (concat lst_cachex cachex) group_id sql_line ignite data_set_id))
             )
         (if (true? (.isDataSetEnabled (.configuration ignite)))
             (let [ddl_id (.incrementAndGet (.atomicSequence ignite "ddl_log" 0 true))]
                 {:sql (my-lexical/to_arryList lst_sql) :un_sql (my-lexical/to_arryList lst_un_sql) :lst_cachex (doto (my-lexical/to_arryList lst_cachex) (.add (MyCacheEx. (.cache ignite "ddl_log") ddl_id (DdlLog. ddl_id group_id sql_line data_set_id) (SqlType/INSERT))))})
             {:sql (my-lexical/to_arryList lst_sql) :un_sql (my-lexical/to_arryList lst_un_sql) :lst_cachex (my-lexical/to_arryList lst_cachex)})
         )))

; run ddl obj
;(defn run_ddl [^Ignite ignite ^String sql_line]
;    (if-let [m (get_ddl_obj ignite sql_line)]
;        (MyDdlUtil/runDdl ignite m)))

; 执行实时数据集中的 ddl
(defn run_ddl_real_time [^Ignite ignite ^String sql_line ^Long data_set_id ^Long group_id]
    (MyDdlUtil/runDdl ignite (get_ddl_objs (get_lst_obj ignite sql_line data_set_id) group_id sql_line ignite data_set_id)))

; 1、如果要修改的是实时数据集，则修改实时数据集的时候要同步修改在其它数据集中的表
; 2、判断要修改的表是否是实时数据集映射到，批处理数据集中的，如果是就不能修改，如果不是就可以修改
; 执行 alter table
(defn my_alter_table [^Ignite ignite ^Long group_id ^String sql_line]
    (let [sql_code (str/lower-case sql_line)]
        (if (= group_id 0)
            (run_ddl_real_time ignite sql_code 0 group_id)
            (if-let [my_group (.get (.cache ignite "my_users_group") group_id)]
                (let [group_type (.getGroup_type my_group) dataset (.get (.cache ignite "my_dataset") (.getData_set_id my_group))]
                    (if (contains? #{"ALL" "DDL"} group_type)
                        (if (true? (.getIs_real dataset))
                            (run_ddl_real_time ignite sql_code (.getId dataset) group_id)
                            (if-let [m (get_table_alter_obj sql_code)]
                                (if-not (my-lexical/is-eq? (-> (my-lexical/get-schema (-> m :table_name)) :schema_name) "my_meta")
                                    (if-let [tables (first (.getAll (.query (.cache ignite "my_dataset_table") (.setArgs (SqlFieldsQuery. "select COUNT(t.id) from my_dataset_table as t WHERE t.dataset_id = ? and t.table_name = ?") (to-array [(.getData_set_id my_group) (str/trim (-> m :table_name))])))))]
                                        (if (> (first tables) 0)
                                            (throw (Exception. (format "该用户组不能修改实时数据集对应到该数据集中的表：%s！" (str/trim (-> m :table_name)))))
                                            (let [ds_m (assoc m :table_name (format "%s_%s" (.getDataset_name dataset) (-> m :table_name)))]
                                                (if (true? (.isDataSetEnabled (.configuration ignite)))
                                                    (let [ddl_id (.incrementAndGet (.atomicSequence ignite "ddl_log" 0 true))]
                                                        (MyDdlUtil/runDdl ignite {:sql (doto (ArrayList.) (.append (get_sql_line ds_m))) :un_sql (doto (ArrayList.) (.append (get_un_sql_line ds_m))) :lst_cachex (doto (my-lexical/to_arryList (myTableItemToMyCacheEx ignite (-> ds_m :add_or_drop :is_add) (str/trim (-> ds_m :table_name)) (.getId dataset) (-> (-> ds_m :colums) :lst_table_item))) (.add (MyCacheEx. (.cache ignite "ddl_log") ddl_id (DdlLog. ddl_id group_id sql_line (.getData_set_id my_group)) (SqlType/INSERT))))}))
                                                    (MyDdlUtil/runDdl ignite {:sql (doto (ArrayList.) (.append (get_sql_line ds_m))) :un_sql (doto (ArrayList.) (.append (get_un_sql_line ds_m))) :lst_cachex (my-lexical/to_arryList (myTableItemToMyCacheEx ignite (-> ds_m :add_or_drop :is_add) (str/trim (-> ds_m :table_name)) (.getId dataset) (-> (-> ds_m :colums) :lst_table_item)))}))
                                                )
                                            ))
                                    (throw (Exception. "没有执行语句的权限！")))
                                (throw (Exception. "修改表语句错误！请仔细检查并参考文档"))))
                        (throw (Exception. "该用户组没有执行 DDL 语句的权限！"))))
                (throw (Exception. "不存在该用户组！"))
                ))))












































