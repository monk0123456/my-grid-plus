(ns org.gridgain.plus.dml.my-update
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-expression :as my-expression]
        [org.gridgain.plus.tools.my-util :as my-util]
        [org.gridgain.plus.init.plus-init-sql :as plus-init-sql]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.tools MyConvertUtil KvSql MyDbUtil)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache SqlType MyLog)
             (org.gridgain.dml.util MyCacheExUtil)
             (cn.plus.model.db MyScenesCache ScenesType MyScenesParams MyScenesParamsPk)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (org.apache.ignite.binary BinaryObjectBuilder BinaryObject)
             (java.util ArrayList List Date Iterator)
             (java.sql Timestamp)
             (java.math BigDecimal)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyUpdate
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [my_call_scenes [org.apache.ignite.Ignite Long clojure.lang.PersistentArrayMap java.util.ArrayList] Object]]
        ;:methods [^:static [my_update_run_log [org.apache.ignite.Ignite Long String] java.util.ArrayList]]
        ))

; 获取名字
(defn get_table_name [[f & r]]
    (if (and (some? f) (my-lexical/is-eq? f "update") (my-lexical/is-eq? (second r) "set"))
        {:table_name (first r) :rs_lst (rest (rest r))}))

(defn get_items
    ([rs_lst] (get_items rs_lst []))
    ([[f & r] lst]
     (if (some? f)
         (if (my-lexical/is-eq? f "where")
             {:items_line lst :where_line r}
             (recur r (conj lst f)))
         {:items_line lst :where_line r})))

(defn get_item_lst
    ([lst_tokens] (get_item_lst lst_tokens [] [] []))
    ([[f & r] stack lst lst_result]
     (if (some? f)
         (cond (and (= f ",") (= (count stack) 0) (> (count lst) 0)) (recur r [] [] (conj lst_result lst))
               (= f "(") (recur r (conj stack f) (conj lst f) lst_result)
               (= f ")") (recur r (pop stack) (conj lst f) lst_result)
               :else
               (recur r stack (conj lst f) lst_result))
         (if (> (count lst) 0) (conj lst_result lst) lst_result))))

(defn to_item_obj [lst]
    (if (= (second lst) "=")
        {:item_name (first lst) :item_obj (my-select/sql-to-ast (rest (rest lst)))}))

(defn item_jsons
    ([lst] (item_jsons lst []))
    ([[f & r] lst]
     (if (some? f)
         (recur r (conj lst (to_item_obj f)))
         lst)))

(defn get_json [^String sql]
    (if-let [lst (my-lexical/to-back sql)]
        (if-let [{table_name :table_name rs_lst :rs_lst} (get_table_name lst)]
            (if-let [{items_line :items_line where_line :where_line} (get_items rs_lst)]
                (if-let [items (get_item_lst items_line)]
                    {:table_name table_name :items (item_jsons items) :where_line where_line})
                (throw (Exception. "更新数据的语句错误！")))
            (throw (Exception. "更新数据的语句错误！")))
        (throw (Exception. "更新数据的语句错误！"))))

(defn get_json_lst [^clojure.lang.PersistentVector lst]
    (if-not (empty? lst)
        (if-let [{table_name :table_name rs_lst :rs_lst} (get_table_name lst)]
            (if-let [{items_line :items_line where_line :where_line} (get_items rs_lst)]
                (if-let [items (get_item_lst items_line)]
                    {:table_name table_name :items (item_jsons items) :where_line where_line})
                (throw (Exception. "更新数据的语句错误！")))
            (throw (Exception. "更新数据的语句错误！")))
        (throw (Exception. "更新数据的语句不能为空！"))))

; 获取 update view obj
(defn get_view_obj [lst]
    (when-let [{table_name :table_name rs_lst :rs_lst} (get_table_name lst)]
        (when-let [{items_line :items_line where_line :where_line} (get_items rs_lst)]
            {:table_name table_name :items (filter #(not= % ",") (map #(str/lower-case %) items_line)) :where_line (my-lexical/double-to-signal where_line)})))

; UPDATE categories set categoryname, description where categoryname = '白酒'
(defn get_view_db [^Ignite ignite ^Long group_id ^String table_name]
    (when-let [lst_rs (first (.getAll (.query (.cache ignite "my_update_views") (.setArgs (SqlFieldsQuery. "select m.code from my_update_views as m join my_group_view as v on m.id = v.view_id where m.table_name = ? and v.my_group_id = ? and v.view_type = ?") (to-array [table_name group_id "改"])))))]
        (if (> (count lst_rs) 0) (get_view_obj (my-lexical/to-back (nth lst_rs 0))))))

(defn my-contains [[f & r] item_name]
    (if (my-lexical/is-eq? f item_name)
        true
        (recur r item_name)))

(defn has_authority_item [[f & r] v_items]
    (if (some? f)
        (if (my-contains v_items (str/lower-case (-> f :item_name)))
            (recur r v_items)
            (throw (Exception. (format "%s列没有修改的权限！" (-> f :item_name)))))))

; 合并 where
(defn merge_where [where_line v_where_line]
    (cond (and (some? where_line) (not (some? v_where_line))) where_line
          (and (some? v_where_line) (not (some? where_line))) v_where_line
          (and (some? where_line) (some? v_where_line)) (concat ["("] where_line [") and ("] v_where_line [")"])
          ))

; 判断权限
(defn get-authority [^Ignite ignite ^Long group_id ^String sql]
    (when-let [{table_name :table_name items :items where_line :where_line} (get_json sql)]
        (if-let [{v_items :items v_where_line :where_line} (get_view_db ignite group_id table_name)]
            (if (nil? (has_authority_item items v_items))
                {:table_name table_name :items items :where_line (merge_where where_line v_where_line)}
                {:table_name table_name :items items :where_line where_line})
            {:table_name table_name :items items :where_line where_line}
            )))

(defn get-authority-lst [^Ignite ignite ^Long group_id ^clojure.lang.PersistentVector sql_lst]
    (when-let [{table_name :table_name items :items where_line :where_line} (get_json_lst sql_lst)]
        (if-let [{v_items :items v_where_line :where_line} (get_view_db ignite group_id table_name)]
            (if (nil? (has_authority_item items v_items))
                {:table_name table_name :items items :where_line (merge_where where_line v_where_line)}
                {:table_name table_name :items items :where_line where_line})
            {:table_name table_name :items items :where_line where_line}
            )))

; 直接在实时中执行
; 过程如下：
; 1、获取表的 PK 定义
; 2、生成 select sql 通过 PK 查询 cache
; 3，修改 cache 的值并生成 MyLogCache
; 4、对 cache 和 MyLogCache 执行事务

; 1、获取表的 PK 定义
(defn get_pk_def [^Ignite ignite ^String table_name]
    (when-let [rows (.getAll (.query (.cache ignite "my_meta_tables") (doto (SqlFieldsQuery. "SELECT m.column_name, m.pkid, m.column_type FROM table_item AS m INNER JOIN my_meta_tables AS o ON m.table_id = o.id WHERE o.table_name = ?")
                                                                          (.setArgs (to-array [table_name])))))]
        (loop [[f & r] rows lst [] lst_pk [] dic {}]
            (if (some? f)
                (if (= (.get f 1) true) (recur r lst (conj lst_pk (.get f 0)) (assoc dic (.get f 0) (.get f 2)))
                                        (recur r (conj lst (.get f 0)) lst_pk (assoc dic (.get f 0) (.get f 2))))
                {:lst lst :lst_pk lst_pk :dic dic}))))

(defn get_pk_def_map [^Ignite ignite ^String table_name]
    (when-let [{lst :lst lst_pk :lst_pk dic :dic} (get_pk_def ignite table_name)]
        (if (> (count lst_pk) 1)
            (loop [[f & r] lst_pk sb (StringBuilder.)]
                (if (some? f)
                    (if (some? r)
                        (recur r (doto sb (.append (format "%s_pk" f)) (.append ",")))
                        (recur r (doto sb (.append (format "%s_pk" f)))))
                    {:line (.toString sb) :lst lst :lst_pk lst_pk :dic dic}))
            {:line (first lst_pk) :lst lst :lst_pk lst_pk :dic dic}
            )
        ))

; 2、生成 select sql 通过 PK 查询 cache
(defn get_update_query_sql [^Ignite ignite obj]
    (when-let [{pk_line :line lst :lst lst_pk :lst_pk dic :dic} (get_pk_def_map ignite (-> obj :table_name))]
        (letfn [(get_items_type [[f & r] dic lst]
                    (if (some? f)
                        (if (contains? dic (-> f :item_name))
                            (recur r dic (conj lst (assoc f :type (get dic (-> f :item_name)))))
                            (recur r dic lst))
                        lst))
                (get_pk_lst [[f & r] dic lst]
                    (if (some? f)
                        (if (contains? dic f)
                            (recur r dic (conj lst {:item_name f :item_type (get dic f)}))
                            (recur r dic lst))
                        lst))]
            {:table_name (-> obj :table_name) :sql (format "select %s from %s where %s" pk_line (-> obj :table_name) (my-select/my-array-to-sql (-> obj :where_line))) :items (get_items_type (-> obj :items) dic []) :pk_lst (get_pk_lst lst_pk dic []) :lst lst :dic dic})
        ))

(defn get_update_query_sql_fun [^Ignite ignite group_id obj ^clojure.lang.PersistentArrayMap dic_paras]
    (when-let [{pk_line :line lst :lst lst_pk :lst_pk dic :dic} (get_pk_def_map ignite (-> obj :table_name))]
        (letfn [(get_items_type [[f & r] dic lst]
                    (if (some? f)
                        (if (contains? dic (-> f :item_name))
                            (recur r dic (conj lst (assoc f :type (get dic (-> f :item_name)))))
                            (recur r dic lst))
                        lst))
                (get_pk_lst [[f & r] dic lst]
                    (if (some? f)
                        (if (contains? dic f)
                            (recur r dic (conj lst {:item_name f :item_type (get dic f)}))
                            (recur r dic lst))
                        lst))]
            (if-let [m (my-select/my_update_delete ignite group_id (format "select %s from %s where %s" pk_line (-> obj :table_name) (my-select/my-array-to-sql (-> obj :where_line))) dic_paras)]
                (assoc m :table_name (-> obj :table_name) :items (get_items_type (-> obj :items) dic []) :pk_lst (get_pk_lst lst_pk dic []) :lst lst :dic dic))
            )
        ))

; update 转换为对象
(defn get_update_obj [^Ignite ignite ^Long group_id ^String sql]
    (if-let [m (get-authority ignite group_id sql)]
        (if-let [us (get_update_query_sql ignite m)]
            us
            (throw (Exception. "更新语句字符串错误！")))
        (throw (Exception. "更新语句字符串错误！"))))

(defn get_update_obj_fun [^Ignite ignite ^Long group_id ^String sql ^clojure.lang.PersistentArrayMap dic_paras]
    (if-let [m (get-authority ignite group_id sql)]
        (if-let [us (get_update_query_sql_fun ignite group_id m dic_paras)]
            us
            (throw (Exception. "更新语句字符串错误！")))
        (throw (Exception. "更新语句字符串错误！"))))

(defn update_run_super_admin [^Ignite ignite ^String sql]
    (if-let [{table_name :table_name} (get_table_name (my-lexical/to-back sql))]
        (if (contains? plus-init-sql/my-grid-tables-set (str/lower-case table_name))
            (.getAll (.query (.cache ignite (str/lower-case table_name)) (SqlFieldsQuery. sql)))
            (throw (Exception. "超级管理员不能修改具体的业务数据！")))
        (throw (Exception. "更新语句字符串错误！"))))

(defn update_run_log [^Ignite ignite ^Long group_id ^String sql]
    (if-let [{table_name :table_name sql :sql items :items pk_lst :pk_lst dic :dic} (get_update_obj ignite group_id sql)]
        (if-let [it (.iterator (.query (.cache ignite (format "f_%s" table_name)) (doto (SqlFieldsQuery. sql)
                                                                         (.setLazy true))))]
            (letfn [(item_value_tokens [^Ignite ignite lst_tokens ^BinaryObject binaryObject dic]
                        (cond (contains? lst_tokens :item_name) (my-expression/item_type_binaryObj (-> lst_tokens :java_item_type) (-> lst_tokens :item_name) binaryObject dic)
                              (contains? lst_tokens :parenthesis) (my-expression/mid_to_forwrod_binaryObject ignite group_id binaryObject dic (reverse (-> lst_tokens :parenthesis)))
                              (contains? lst_tokens :operation) (my-expression/mid_to_forwrod_binaryObject ignite group_id binaryObject dic (reverse (-> lst_tokens :operation)))
                              ))
                    (get_key_obj [^Ignite ignite ^String table_name row pk_lst]
                        (if-let [keyBuilder (.builder (.binary ignite) (KvSql/getKeyType ignite (format "f_%s" table_name)))]
                            (loop [[f & r] row [f_pk & r_pk] pk_lst kp keyBuilder lst_kv (ArrayList.)]
                                (if (and (some? f) (some? f_pk))
                                    (let [key (format "%s_pk" (-> f_pk :item_name)) value (my-lexical/get_jave_vs (-> f_pk :item_type) f)]
                                        (recur r r_pk (doto kp (.setField key value)) (doto lst_kv (.add (MyKeyValue. key value))))
                                        )
                                    [(.build kp) lst_kv]))))
                    (get_value_obj [^Ignite ignite ^String table_name pk items dic]
                        (if-let [valueBinaryObject (.toBuilder (.get (.withKeepBinary (.cache ignite (format "f_%s" table_name))) pk))]
                            (loop [[f & r] items vp valueBinaryObject lst_kv (ArrayList.)]
                                (if (some? f)
                                    (let [my_vs (item_value_tokens ignite (-> f :item_obj) (.build vp) dic)]
                                        (if (and (vector? my_vs) (map? (first my_vs)))
                                            (let [key (-> f :item_name) value (my-lexical/get_jave_vs (-> f :type) (-> (first my_vs) :express))]
                                                (recur r (doto vp (.setField key value)) (doto lst_kv (.add (MyKeyValue. key value)))))
                                            (let [key (-> f :item_name) value (my-lexical/get_jave_vs (-> f :type) my_vs)]
                                                (recur r (doto vp (.setField key value)) (doto lst_kv (.add (MyKeyValue. key value)))))))
                                    [(.build vp) lst_kv]))))
                    (get_cache_pk [^Ignite ignite ^String table_name it pk_lst]
                        (loop [itr it lst []]
                            (if (.hasNext itr)
                                (if-let [row (.next itr)]
                                    (cond (= (count pk_lst) 1) (recur itr (conj lst (my-lexical/get_jave_vs (-> (first pk_lst) :item_type) (.get row 0))))
                                          (> (count pk_lst) 1) (recur itr (conj lst (get_key_obj ignite table_name row pk_lst)))
                                          :else
                                          (throw (Exception. "表没有主键！"))
                                          ))
                                lst)))
                    (get_cache_data [^Ignite ignite ^String table_name it pk_lst items dic]
                        (if-let [lst_pk (get_cache_pk ignite table_name it pk_lst)]
                            (loop [[f_pk & r_pk] lst_pk ms items lst_rs []]
                                (if (some? f_pk)
                                    (if (vector? f_pk)
                                        (let [[pk kv_pk] f_pk log_id (.incrementAndGet (.atomicSequence ignite "my_log" 0 true))]
                                            (let [[vs kv_vs] (get_value_obj ignite table_name pk ms dic)]
                                                (recur r_pk ms (concat lst_rs [(MyCacheEx. (.cache ignite (format "f_%s" table_name)) pk vs (SqlType/UPDATE))
                                                                               (MyCacheEx. (.cache ignite "my_log") log_id (MyLog. log_id table_name (MyCacheExUtil/objToBytes (MyLogCache. (format "f_%s" table_name) kv_pk kv_vs (SqlType/UPDATE)))) (SqlType/INSERT))])))
                                            )
                                        (let [[vs kv_vs] (get_value_obj ignite table_name f_pk ms dic) log_id (.incrementAndGet (.atomicSequence ignite "my_log" 0 true))]
                                            (recur r_pk ms (concat lst_rs [(MyCacheEx. (.cache ignite (format "f_%s" table_name)) f_pk vs (SqlType/UPDATE))
                                                                           (MyCacheEx. (.cache ignite "my_log") log_id (MyLog. log_id table_name (MyCacheExUtil/objToBytes (MyLogCache. (format "f_%s" table_name) f_pk kv_vs (SqlType/UPDATE)))) (SqlType/INSERT))]))))
                                    lst_rs))))]
                (get_cache_data ignite table_name it pk_lst items dic)
                      ))))

(defn run_log_fun [ignite group_id table_name sql args items pk_lst dic dic_paras]
    (if-let [it (.iterator (.query (.cache ignite (format "f_%s" table_name)) (doto (SqlFieldsQuery. sql)
                                                                                  (.setArgs args)
                                                                                  (.setLazy true))))]
        (letfn [(item_value_tokens [^Ignite ignite lst_tokens ^BinaryObject binaryObject dic dic_paras]
                    (cond (contains? lst_tokens :item_name) (my-expression/item_type_binaryObj_fun (-> lst_tokens :java_item_type) (-> lst_tokens :item_name) binaryObject dic dic_paras)
                          (contains? lst_tokens :parenthesis) (my-expression/mid_to_forwrod_binaryObject_fun ignite group_id binaryObject dic (reverse (-> lst_tokens :parenthesis)) dic_paras)
                          (contains? lst_tokens :operation) (my-expression/mid_to_forwrod_binaryObject_fun ignite group_id binaryObject dic (reverse (-> lst_tokens :operation)) dic_paras)
                          ))
                (get_key_obj [^Ignite ignite ^String table_name row pk_lst]
                    (if-let [keyBuilder (.builder (.binary ignite) (KvSql/getKeyType ignite (format "f_%s" table_name)))]
                        (loop [[f & r] row [f_pk & r_pk] pk_lst kp keyBuilder lst_kv (ArrayList.)]
                            (if (and (some? f) (some? f_pk))
                                (let [key (format "%s_pk" (-> f_pk :item_name)) value (my-lexical/get_jave_vs (-> f_pk :item_type) f)]
                                    (recur r r_pk (doto kp (.setField key value)) (doto lst_kv (.add (MyKeyValue. key value))))
                                    )
                                [(.build kp) lst_kv]))))
                (get_value_obj [^Ignite ignite ^String table_name pk items dic dic_paras]
                    (if-let [valueBinaryObject (.toBuilder (.get (.withKeepBinary (.cache ignite (format "f_%s" table_name))) pk))]
                        (loop [[f & r] items vp valueBinaryObject lst_kv (ArrayList.)]
                            (if (some? f)
                                (let [my_vs (item_value_tokens ignite (-> f :item_obj) (.build vp) dic dic_paras)]
                                    (if (and (vector? my_vs) (map? (first my_vs)))
                                        (let [key (-> f :item_name) value (my-lexical/get_jave_vs (-> f :type) (-> (first my_vs) :express))]
                                            (recur r (doto vp (.setField key value)) (doto lst_kv (.add (MyKeyValue. key value)))))
                                        (let [key (-> f :item_name) value (my-lexical/get_jave_vs (-> f :type) my_vs)]
                                            (recur r (doto vp (.setField key value)) (doto lst_kv (.add (MyKeyValue. key value)))))))
                                [(.build vp) lst_kv]))))
                (get_cache_pk [^Ignite ignite ^String table_name it pk_lst]
                    (loop [itr it lst []]
                        (if (.hasNext itr)
                            (if-let [row (.next itr)]
                                (cond (= (count pk_lst) 1) (recur itr (conj lst (my-lexical/get_jave_vs (-> (first pk_lst) :item_type) (.get row 0))))
                                      (> (count pk_lst) 1) (recur itr (conj lst (get_key_obj ignite table_name row pk_lst)))
                                      :else
                                      (throw (Exception. "表没有主键！"))
                                      ))
                            lst)))
                (get_cache_data [^Ignite ignite ^String table_name it pk_lst items dic dic_paras]
                    (if-let [lst_pk (get_cache_pk ignite table_name it pk_lst)]
                        (loop [[f_pk & r_pk] lst_pk ms items lst_rs []]
                            (if (some? f_pk)
                                (if (vector? f_pk)
                                    (let [[pk kv_pk] f_pk log_id (.incrementAndGet (.atomicSequence ignite "my_log" 0 true))]
                                        (let [[vs kv_vs] (get_value_obj ignite table_name pk ms dic dic_paras)]
                                            (recur r_pk ms (concat lst_rs [(MyCacheEx. (.cache ignite (format "f_%s" table_name)) pk vs (SqlType/UPDATE))
                                                                           (MyCacheEx. (.cache ignite "my_log") log_id (MyLog. log_id table_name (MyCacheExUtil/objToBytes (MyLogCache. (format "f_%s" table_name) kv_pk kv_vs (SqlType/UPDATE)))) (SqlType/INSERT))])))
                                        )
                                    (let [[vs kv_vs] (get_value_obj ignite table_name f_pk ms dic dic_paras) log_id (.incrementAndGet (.atomicSequence ignite "my_log" 0 true))]
                                        (recur r_pk ms (concat lst_rs [(MyCacheEx. (.cache ignite (format "f_%s" table_name)) f_pk vs (SqlType/UPDATE))
                                                                       (MyCacheEx. (.cache ignite "my_log") log_id (MyLog. log_id table_name (MyCacheExUtil/objToBytes (MyLogCache. (format "f_%s" table_name) f_pk kv_vs (SqlType/UPDATE)))) (SqlType/INSERT))]))))
                                lst_rs))))]
            (get_cache_data ignite table_name it pk_lst items dic dic_paras)
            )))

(defn update_run_log_fun [^Ignite ignite ^Long group_id ^clojure.lang.PersistentArrayMap ast ^clojure.lang.PersistentArrayMap dic_paras]
    (if-let [{table_name :table_name sql :sql args :args items :items pk_lst :pk_lst dic :dic} ast]
        (run_log_fun ignite group_id table_name sql args items pk_lst dic dic_paras)))

(defn update_run_log_fun_tran [^Ignite ignite ^Long group_id ^String sql ^clojure.lang.PersistentArrayMap dic_paras]
    (if-let [{table_name :table_name sql :sql args :args items :items pk_lst :pk_lst dic :dic} (get_update_obj ignite group_id sql)]
        (run_log_fun ignite group_id table_name sql args items pk_lst dic dic_paras)))
;
;(defn -my_update_run_log [^Ignite ignite ^Long group_id ^String sql]
;    (my-lexical/to_arryList (update_run_log ignite group_id sql)))

(defn run_no_log_fun [ignite group_id table_name sql args items pk_lst dic dic_paras]
    (if-let [it (.iterator (.query (.cache ignite (format "f_%s" table_name)) (doto (SqlFieldsQuery. sql)
                                                                                  (.setArgs args)
                                                                                  (.setLazy true))))]
        (letfn [(item_value_tokens [^Ignite ignite lst_tokens ^BinaryObject binaryObject dic dic_paras]
                    (cond (contains? lst_tokens :item_name) (my-expression/item_type_binaryObj_fun (-> lst_tokens :java_item_type) (-> lst_tokens :item_name) binaryObject dic dic_paras)
                          (contains? lst_tokens :parenthesis) (my-expression/mid_to_forwrod_binaryObject_fun ignite group_id binaryObject dic (reverse (-> lst_tokens :parenthesis)) dic_paras)
                          (contains? lst_tokens :operation) (my-expression/mid_to_forwrod_binaryObject_fun ignite group_id binaryObject dic (reverse (-> lst_tokens :operation)) dic_paras)
                          ))
                (get_key_obj [^Ignite ignite ^String table_name row pk_lst]
                    (if-let [keyBuilder (.builder (.binary ignite) (KvSql/getKeyType ignite (format "f_%s" table_name)))]
                        (loop [[f & r] row [f_pk & r_pk] pk_lst kp keyBuilder]
                            (if (and (some? f) (some? f_pk))
                                (recur r r_pk (doto kp (.setField (format "%s_pk" (-> f_pk :item_name)) (my-lexical/get_jave_vs (-> f_pk :item_type) f))))
                                (.build kp)))))
                (get_value_obj [^Ignite ignite ^String table_name pk items dic dic_paras]
                    (if-let [valueBinaryObject (.toBuilder (.get (.withKeepBinary (.cache ignite (format "f_%s" table_name))) pk))]
                        (loop [[f & r] items vp valueBinaryObject]
                            (if (some? f)
                                (let [my_vs (item_value_tokens ignite (-> f :item_obj) (.build vp) dic dic_paras)]
                                    (if (and (vector? my_vs) (map? (first my_vs)))
                                        (recur r (doto vp (.setField (-> f :item_name) (my-lexical/get_jave_vs (-> f :type) (-> (first my_vs) :express)))))
                                        (recur r (doto vp (.setField (-> f :item_name) (my-lexical/get_jave_vs (-> f :type) (my-lexical/get_jave_vs (-> f :type) my_vs)))))))
                                (.build vp)))))
                (get_cache_pk [^Ignite ignite ^String table_name it pk_lst]
                    (loop [itr it lst []]
                        (if (.hasNext itr)
                            (if-let [row (.next itr)]
                                (cond (= (count pk_lst) 1) (recur itr (conj lst (my-lexical/get_jave_vs (-> (first pk_lst) :item_type) (.get row 0))))
                                      (> (count pk_lst) 1) (recur itr (conj lst (get_key_obj ignite table_name row pk_lst)))
                                      :else
                                      (throw (Exception. "表没有主键！"))
                                      ))
                            lst)))
                (get_cache_data [^Ignite ignite ^String table_name it pk_lst items dic dic_paras]
                    (if-let [lst_pk (get_cache_pk ignite table_name it pk_lst)]
                        (loop [[f_pk & r_pk] lst_pk ms items lst_rs []]
                            (if (some? f_pk)
                                (recur r_pk ms (conj lst_rs (MyCacheEx. (.cache ignite (format "f_%s" table_name)) f_pk (get_value_obj ignite table_name f_pk ms dic dic_paras) (SqlType/UPDATE))))
                                lst_rs))))]
            (get_cache_data ignite table_name it pk_lst items dic dic_paras)
            )))

(defn update_run_no_log [^Ignite ignite ^Long group_id ^String sql]
    (if-let [{table_name :table_name sql :sql items :items pk_lst :pk_lst dic :dic} (get_update_obj ignite group_id sql)]
        (if-let [it (.iterator (.query (.cache ignite (format "f_%s" table_name)) (doto (SqlFieldsQuery. sql)
                                                                                      (.setLazy true))))]
            (letfn [(item_value_tokens [^Ignite ignite lst_tokens ^BinaryObject binaryObject dic]
                        (cond (contains? lst_tokens :item_name) (my-expression/item_type_binaryObj (-> lst_tokens :java_item_type) (-> lst_tokens :item_name) binaryObject dic)
                              (contains? lst_tokens :parenthesis) (my-expression/mid_to_forwrod_binaryObject ignite group_id binaryObject dic (reverse (-> lst_tokens :parenthesis)))
                              (contains? lst_tokens :operation) (my-expression/mid_to_forwrod_binaryObject ignite group_id binaryObject dic (reverse (-> lst_tokens :operation)))
                              ))
                    (get_key_obj [^Ignite ignite ^String table_name row pk_lst]
                        (if-let [keyBuilder (.builder (.binary ignite) (KvSql/getKeyType ignite (format "f_%s" table_name)))]
                            (loop [[f & r] row [f_pk & r_pk] pk_lst kp keyBuilder]
                                (if (and (some? f) (some? f_pk))
                                    (recur r r_pk (doto kp (.setField (format "%s_pk" (-> f_pk :item_name)) (my-lexical/get_jave_vs (-> f_pk :item_type) f))))
                                    (.build kp)))))
                    (get_value_obj [^Ignite ignite ^String table_name pk items dic]
                        (if-let [valueBinaryObject (.toBuilder (.get (.withKeepBinary (.cache ignite (format "f_%s" table_name))) pk))]
                            (loop [[f & r] items vp valueBinaryObject]
                                (if (some? f)
                                    (let [my_vs (item_value_tokens ignite (-> f :item_obj) (.build vp) dic)]
                                        (if (and (vector? my_vs) (map? (first my_vs)))
                                            (recur r (doto vp (.setField (-> f :item_name) (my-lexical/get_jave_vs (-> f :type) (-> (first my_vs) :express)))))
                                            (recur r (doto vp (.setField (-> f :item_name) (my-lexical/get_jave_vs (-> f :type) (my-lexical/get_jave_vs (-> f :type) my_vs)))))))
                                    (.build vp)))))
                    (get_cache_pk [^Ignite ignite ^String table_name it pk_lst]
                        (loop [itr it lst []]
                            (if (.hasNext itr)
                                (if-let [row (.next itr)]
                                    (cond (= (count pk_lst) 1) (recur itr (conj lst (my-lexical/get_jave_vs (-> (first pk_lst) :item_type) (.get row 0))))
                                          (> (count pk_lst) 1) (recur itr (conj lst (get_key_obj ignite table_name row pk_lst)))
                                          :else
                                          (throw (Exception. "表没有主键！"))
                                          ))
                                lst)))
                    (get_cache_data [^Ignite ignite ^String table_name it pk_lst items dic]
                        (if-let [lst_pk (get_cache_pk ignite table_name it pk_lst)]
                            (loop [[f_pk & r_pk] lst_pk ms items lst_rs []]
                                (if (some? f_pk)
                                    (recur r_pk ms (conj lst_rs (MyCacheEx. (.cache ignite (format "f_%s" table_name)) f_pk (get_value_obj ignite table_name f_pk ms dic) (SqlType/UPDATE))))
                                    lst_rs))))]
                (get_cache_data ignite table_name it pk_lst items dic)
                ))))

(defn update_run_no_log_fun [^Ignite ignite ^Long group_id ^clojure.lang.PersistentArrayMap my_ast ^clojure.lang.PersistentArrayMap dic_paras]
    (if-let [{table_name :table_name sql :sql args :args items :items pk_lst :pk_lst dic :dic} my_ast]
        (run_no_log_fun ignite group_id table_name sql args items pk_lst dic dic_paras)))

(defn update_run_no_log_fun_tran [^Ignite ignite ^Long group_id ^String sql ^clojure.lang.PersistentArrayMap dic_paras]
    (if-let [{table_name :table_name sql :sql args :args items :items pk_lst :pk_lst dic :dic} (get_update_obj ignite group_id sql)]
        (run_no_log_fun ignite group_id table_name sql args items pk_lst dic dic_paras)))

(defn get_update_cache_tran [^Ignite ignite ^Long group_id ^String sql ^clojure.lang.PersistentArrayMap dic_paras]
    (if (> group_id 0)
        (if-let [ds_obj (my-util/get_ds_by_group_id ignite group_id)]
            (if-not (empty? ds_obj)
                ; 如果是实时数据集
                (if (true? (nth (first ds_obj) 1))
                    ; 在实时数据集
                    (if (true? (.isDataSetEnabled (.configuration ignite)))
                        (update_run_log_fun_tran ignite group_id sql dic_paras)
                        (update_run_no_log_fun_tran ignite group_id sql dic_paras))
                    ; 在非实时树集
                    (let [{table_name :table_name} (get_table_name (my-lexical/to-back sql))]
                        (if (true? (my-util/is_in_ds ignite (nth ds_obj 0) table_name))
                            (throw (Exception. "表来至实时数据集不能在该表上执行更新操作！"))
                            (update_run_no_log_fun_tran ignite group_id sql dic_paras)))
                    )
                (throw (Exception. "用户不存在或者没有权限！")))
            )))

(defn get_update_cache [^Ignite ignite ^Long group_id ^clojure.lang.PersistentArrayMap ast ^clojure.lang.PersistentArrayMap dic_paras]
    (if (> group_id 0)
        (if-let [ds_obj (my-util/get_ds_by_group_id ignite group_id)]
            (if-not (empty? ds_obj)
                ; 如果是实时数据集
                (if (true? (nth (first ds_obj) 1))
                    ; 在实时数据集
                    (if-let [my_ast (get_update_query_sql_fun ignite group_id ast dic_paras)]
                        (if (true? (.isDataSetEnabled (.configuration ignite)))
                            (update_run_log_fun ignite group_id my_ast dic_paras)
                            (update_run_no_log_fun ignite group_id my_ast dic_paras)))
                    ; 在非实时树集
                    (if-let [my_ast (get_update_query_sql_fun ignite group_id ast dic_paras)]
                        (if (true? (my-util/is_in_ds ignite (nth ds_obj 0) (-> ast :table_name)))
                            (throw (Exception. "表来至实时数据集不能在该表上执行更新操作！"))
                            (update_run_no_log_fun ignite group_id my_ast dic_paras)))
                    )
                (throw (Exception. "用户不存在或者没有权限！")))
            )))

; 1、判断用户组在实时数据集，还是非实时数据
; 如果是非实时数据集,
; 获取表名后，查一下，表名是否在 对应的 my_dataset_table 中，如果在就不能添加，否则直接执行 insert sql
; 2、如果是在实时数据集是否需要 log
(defn update_run [^Ignite ignite ^Long group_id ^String sql]
    (let [sql (str/lower-case sql)]
        (if (= group_id 0)
            ; 超级用户
            (update_run_super_admin ignite sql)
            (let [{my_table_name :table_name} (get_table_name (my-lexical/to-back sql))]
                (let [{schema_name :schema_name table_name :table_name} (my-lexical/get-schema my_table_name)]
                    (if-not (= schema_name "my_meta")
                        (if-let [ds_obj (my-util/get_ds_by_group_id ignite group_id)]
                            (if-not (empty? ds_obj)
                                ; 如果是实时数据集
                                (if (true? (nth (first ds_obj) 1))
                                    ; 在实时数据集
                                    (if (true? (.isDataSetEnabled (.configuration ignite)))
                                        (my-lexical/trans ignite (update_run_log ignite group_id sql))
                                        (my-lexical/trans ignite (update_run_no_log ignite group_id sql)))
                                    ; 在非实时树集
                                    (if (true? (my-util/is_in_ds ignite (nth ds_obj 0) table_name))
                                        (throw (Exception. "表来至实时数据集不能在该表上执行更新操作！"))
                                        (my-lexical/trans ignite (update_run_no_log ignite group_id sql)))
                                    )
                                (throw (Exception. "用户不存在或者没有权限！")))
                            )
                        (throw (Exception. "用户不存在或者没有权限！")))))
            ))
    )

; 以下是保存到 cache 中的 scenes_name, ast, 参数列表
(defn save_scenes [^Ignite ignite ^Long group_id ^String scenes_name ^String sql_code ^clojure.lang.PersistentVector sql_lst ^String descrip ^List params ^Boolean is_batch]
    (if-let [ast (get-authority-lst ignite group_id sql_lst)]
        (let [m (MyScenesCache. group_id scenes_name sql_code descrip is_batch params ast (ScenesType/UPDATE))]
            (.put (.cache ignite "my_scenes") (str/lower-case scenes_name) m))
        (throw (Exception. "更新语句错误！"))))

; 调用
(defn call_scenes [^Ignite ignite ^Long group_id ^String scenes_name ^clojure.lang.PersistentArrayMap dic_paras]
    (if-let [vs (.get (.cache ignite "my_scenes") (str/lower-case scenes_name))]
        (if (= (.getGroup_id vs) group_id)
            (get_update_cache ignite group_id (.getAst vs) dic_paras)
            (if-let [m_group_id (MyDbUtil/getGroupIdByCall ignite group_id scenes_name)]
                (get_update_cache ignite m_group_id (.getAst vs) dic_paras)
                (throw (Exception. (format "用户组 %s 没有执行权限！" group_id)))))
        (throw (Exception. (format "场景名称 %s 不存在！" scenes_name)))))

(defn my_call_scenes [^Ignite ignite ^Long group_id ^clojure.lang.PersistentArrayMap vs ^java.util.ArrayList lst_paras]
    (let [dic_paras (my-lexical/get_scenes_dic vs lst_paras)]
        (if (= (.getGroup_id vs) group_id)
            (get_update_cache ignite group_id (.getAst vs) dic_paras)
            (if-let [m_group_id (MyDbUtil/getGroupIdByCall ignite group_id (.getScenes_name vs))]
                (get_update_cache ignite m_group_id (.getAst vs) dic_paras)
                (throw (Exception. (format "用户组 %s 没有执行权限！" group_id))))))
    )

; 调用
(defn -my_call_scenes [^Ignite ignite ^Long group_id ^clojure.lang.PersistentArrayMap vs ^java.util.ArrayList lst_paras]
    (my_call_scenes ignite group_id vs lst_paras)
    )























































