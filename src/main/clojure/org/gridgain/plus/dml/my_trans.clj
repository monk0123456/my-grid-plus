(ns org.gridgain.plus.dml.my-trans
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.dml.my-expression :as my-expression]
        [org.gridgain.plus.dml.my-scenes-util :as my-scenes-util]
        [org.gridgain.plus.context.my-context :as my-context]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil KvSql MyDbUtil)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache SqlType)
             (org.gridgain.dml.util MyCacheExUtil)
             (cn.plus.model.db MyScenesCache ScenesType MyScenesParams MyScenesParamsPk)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache CacheMode CacheAtomicityMode)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (org.apache.ignite.binary BinaryObjectBuilder BinaryObject)
             (java.util ArrayList List Date Iterator)
             (java.sql Timestamp)
             (java.math BigDecimal)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyTrans
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [my_call_scenes [org.apache.ignite.Ignite Long clojure.lang.PersistentArrayMap java.util.ArrayList] Object]]
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

; 输入 lst token
(defn get_segment
    ([lst_trans] (get_segment lst_trans [] [] [] []))
    ([[f & r] lst_init_stack lst_init lst_body_stack lst_body]
     (if (some? f)
         (cond (and (my-lexical/is-eq? f "begin:") (= (count lst_init_stack) 0)) (recur r (conj lst_init_stack f) lst_init lst_body_stack lst_body)
               (and (not (my-lexical/is-eq? f "execute:")) (> (count lst_init_stack) 0)) (recur r (conj lst_init_stack f) lst_init lst_body_stack lst_body)
               (and (my-lexical/is-eq? f "execute:") (> (count lst_init_stack) 0) (= (count lst_body_stack) 0)) (recur r [] lst_init_stack (conj lst_body_stack f) lst_body)
               (and (not (my-lexical/is-eq? f "end;")) (> (count lst_body_stack) 0)) (recur r [] lst_init (conj lst_body_stack f) lst_body)
               (and (my-lexical/is-eq? f "end;") (> (count lst_body_stack) 0)) (recur r [] lst_init [] lst_body_stack)
               :else
               (throw (Exception. "事务语言错误！"))
               )
         {:init (rest lst_init) :body (rest lst_body)})))

(defn get_segment_lst
    ([lst] (get_segment_lst lst [] [] [] [] []))
    ([[f & r] stack_str stack_str_b stack_b lst_stack lst]
     (if (some? f)
         (cond (and (= f ";") (= (count stack_str) 0) (= (count stack_str_b) 0) (= (count stack_b) 0)) (if (> (count lst_stack) 0) (recur r [] [] [] [] (conj lst lst_stack))
                                                                                                                                   (recur r [] [] [] [] lst))
               (= f "(") (recur r stack_str stack_str_b (conj stack_b f) (conj lst_stack f) lst)
               (= f ")") (recur r stack_str stack_str_b (pop stack_b) (conj lst_stack f) lst)
               (= f "'") (if (nil? (peek stack_str)) (recur r (conj stack_str f) stack_str_b stack_b (conj lst_stack f) lst)
                                                     (recur r (pop stack_str) stack_str_b stack_b (conj lst_stack f) lst))
               (= f "\"") (if (nil? (peek stack_str)) (recur r stack_str (conj stack_str_b f) stack_b (conj lst_stack f) lst)
                                                      (recur r stack_str (pop stack_str_b) stack_b (conj lst_stack f) lst))
               :else
               (recur r stack_str stack_str_b stack_b (conj lst_stack f) lst)
               )
         (if (> (count lst_stack) 0)
             (conj lst lst_stack)
             lst))))

(defn get_var [[f & r]]
    (if (and (my-lexical/is-eq? f "val") (= (second r) "="))
        {:name (first r) :value (my-select/sql-to-ast (rest (rest r)))}
        (throw (Exception. (format "初始化出错：%s" (str/join " " (cons f r)))))))

(defn get_vars
    ([lst_tokens] (get_vars lst_tokens []))
    ([[f & r] lst]
     (if (some? f)
         (when-let [m (get_var f)]
             (recur r (conj lst m)))
         lst)))

; trans to json
(defn get_trans_to_json [lst-sql]
    (if-let [{init :init body :body} (get_segment lst-sql)]
        {:vars (get_vars (get_segment_lst init)) :body (get_segment_lst body)}
        (throw (Exception. "事务语言错误！"))))

(defn get_trans_to_json_lst [lst]
    (if-let [{init :init body :body} (get_segment lst)]
        {:vars (get_vars (get_segment_lst init)) :body (get_segment_lst body)}
        (throw (Exception. "事务语言错误！"))))

; 执行
(defn tran_run [^Ignite ignite ^Long group_id lst-sql]
    (let [{vars :vars body :body} (get_trans_to_json lst-sql)]
        (letfn [(get-var
                    ([^Ignite ignite ^Long group_id ^clojure.lang.PersistentVector vars] (get-var ignite group_id vars {}))
                    ([^Ignite ignite ^Long group_id [f & r] dic_paras]
                     (if (some? f)
                         (if-let [value (my-expression/get_value_tokens ignite group_id (-> f :value))]
                             (recur ignite group_id r (assoc dic_paras (-> f :name) {:value value :type Object}))
                             (throw (Exception. (format "定义的变量出错！%s" (-> f :name)))))
                         dic_paras)))
                (get-body
                    ([^Ignite ignite ^Long group_id vars body] (get-body ignite group_id body [] (get-var ignite group_id vars)))
                    ([^Ignite ignite ^Long group_id [f & r] lst_cache dic_paras]
                     (if (some? f)
                         (cond (my-lexical/is-eq? (first f) "insert") (recur ignite group_id r (concat lst_cache (my-insert/get_insert_cache ignite group_id dic_paras (str/join " " f))) dic_paras)
                               (my-lexical/is-eq? (first f) "update") (recur ignite group_id r (concat lst_cache (my-update/get_update_cache ignite group_id dic_paras (str/join " " f))) dic_paras)
                               (my-lexical/is-eq? (first f) "delete") (recur ignite group_id r (concat lst_cache (my-delete/get_delete_cache ignite group_id dic_paras (str/join " " f))) dic_paras)
                               )
                         lst_cache)))
                ]
            (if-let [ls_cache (get-body ignite group_id vars body)]
                (my-lexical/trans ignite ls_cache)
                (throw (Exception. "该事务没有改变任何值，请确认写法正确！"))))))

; 执行
(defn tran_run_fun [^Ignite ignite ^Long group_id lst-sql ^clojure.lang.PersistentArrayMap input_dic_paras]
    (let [{vars :vars body :body} (get_trans_to_json lst-sql)]
        (letfn [(get-var
                    ([^Ignite ignite ^Long group_id ^clojure.lang.PersistentVector vars input_dic_paras] (get-var ignite group_id vars {} input_dic_paras))
                    ([^Ignite ignite ^Long group_id [f & r] dic_paras input_dic_paras]
                     (if (some? f)
                         (if-not (contains? input_dic_paras (-> f :name))
                             (if-let [value (my-expression/get_value_tokens_fun ignite group_id (-> f :value) input_dic_paras)]
                                 (recur ignite group_id r (assoc dic_paras (-> f :name) {:value value :type Object}) input_dic_paras)
                                 (throw (Exception. (format "定义的变量出错！%s" (-> f :name)))))
                             (throw (Exception. (format "%s 已经在输入参数中被定义，请换一个名字！" (-> f :name)))))
                         (concat dic_paras input_dic_paras))))
                (get-body
                    ([^Ignite ignite ^Long group_id vars body input_dic_paras] (get-body ignite group_id body [] (get-var ignite group_id vars input_dic_paras) input_dic_paras))
                    ([^Ignite ignite ^Long group_id [f & r] lst_cache dic_paras input_dic_paras]
                     (if (some? f)
                         (cond (my-lexical/is-eq? (first f) "insert") (recur ignite group_id r (concat lst_cache (my-insert/get_insert_cache ignite group_id (str/join " " f) dic_paras)) dic_paras input_dic_paras)
                               (my-lexical/is-eq? (first f) "update") (recur ignite group_id r (concat lst_cache (my-update/get_update_cache ignite group_id (str/join " " f) dic_paras)) dic_paras input_dic_paras)
                               (my-lexical/is-eq? (first f) "delete") (recur ignite group_id r (concat lst_cache (my-delete/get_delete_cache ignite group_id (str/join " " f) dic_paras)) dic_paras input_dic_paras)
                               )
                         lst_cache)))
                ]
            (if-let [ls_cache (get-body ignite group_id vars body input_dic_paras)]
                (my-lexical/trans ignite ls_cache)
                (throw (Exception. "该事务没有改变任何值，请确认写法正确！"))))))

; 执行
(defn tran_run_fun_ast [^Ignite ignite ^Long group_id ^clojure.lang.PersistentArrayMap ast ^clojure.lang.PersistentArrayMap input_dic_paras]
    (let [{vars :vars body :body} ast]
        (letfn [(get-var
                    ([^Ignite ignite ^Long group_id ^clojure.lang.PersistentVector vars input_dic_paras] (get-var ignite group_id vars {} input_dic_paras))
                    ([^Ignite ignite ^Long group_id [f & r] dic_paras input_dic_paras]
                     (if (some? f)
                         (if-not (contains? input_dic_paras (-> f :name))
                             (if-let [value (my-expression/get_value_tokens_fun ignite group_id (-> f :value) input_dic_paras)]
                                 (recur ignite group_id r (assoc dic_paras (-> f :name) {:value value :type Object}) input_dic_paras)
                                 (throw (Exception. (format "定义的变量出错！%s" (-> f :name)))))
                             (throw (Exception. (format "%s 已经在输入参数中被定义，请换一个名字！" (-> f :name)))))
                         (concat dic_paras input_dic_paras))))
                (get-body
                    ([^Ignite ignite ^Long group_id vars body input_dic_paras] (get-body ignite group_id body [] (get-var ignite group_id vars input_dic_paras) input_dic_paras))
                    ([^Ignite ignite ^Long group_id [f & r] lst_cache dic_paras input_dic_paras]
                     (if (some? f)
                         (cond (my-lexical/is-eq? (first f) "insert") (recur ignite group_id r (concat lst_cache (my-insert/get_insert_cache_tran ignite group_id (str/join " " f) dic_paras)) dic_paras input_dic_paras)
                               (my-lexical/is-eq? (first f) "update") (recur ignite group_id r (concat lst_cache (my-update/get_update_cache_tran ignite group_id (str/join " " f) dic_paras)) dic_paras input_dic_paras)
                               (my-lexical/is-eq? (first f) "delete") (recur ignite group_id r (concat lst_cache (my-delete/get_delete_cache_tran ignite group_id (str/join " " f) dic_paras)) dic_paras input_dic_paras)
                               )
                         lst_cache)))
                ]
            (if-let [ls_cache (get-body ignite group_id vars body input_dic_paras)]
                (my-lexical/trans ignite ls_cache)
                (throw (Exception. "该事务没有改变任何值，请确认写法正确！"))))))

; 以下是保存到 cache 中的 scenes_name, ast, 参数列表
;(defn save_scenes [^Ignite ignite ^Long group_id ^String scenes_name ^String sql_code ^String descrip ^List params]
;    (let [m (MyScenesCache. group_id scenes_name sql_code descrip false params (get_trans_to_json (str/lower-case sql_code)))]
;        (.put (.cache ignite "my_scenes") (str/lower-case scenes_name) m)))

;(defn save_scenes [^Ignite ignite ^Long group_id ^String scenes_code]
;    (let [{scenes_type :scenes_type scenes_obj :obj} (my-scenes-util/my_scenes_obj scenes_code)]
;        (cond (= scenes_type "scenes") (let [{scenes_name :name params :params sql :sql descrip :descrip is_batch :is_batch} scenes_obj]
;                                           (let [m (MyScenesCache. group_id scenes_name scenes_code descrip is_batch params (get_trans_to_json_lst sql))]
;                                               (.put (.cache ignite "my_scenes") (str/lower-case scenes_name) m)))
;              (= scenes_type "tran") (let [{scenes_name :name params :params trans :trans descrip :descrip is_batch :is_batch} scenes_obj]
;                                           (let [m (MyScenesCache. group_id scenes_name scenes_code descrip is_batch params (get_trans_to_json_lst trans))]
;                                               (.put (.cache ignite "my_scenes") (str/lower-case scenes_name) m)))
;              (= scenes_type "cron") (let [{scenes_name :name params :params batch :batch descrip :descrip is_batch :is_batch} scenes_obj]
;                                           (let [m (MyScenesCache. group_id scenes_name scenes_code descrip is_batch params (get_trans_to_json_lst sql))]
;                                               (.put (.cache ignite "my_scenes") (str/lower-case scenes_name) m)))
;              )))

(defn save_scenes [^Ignite ignite ^Long group_id ^String scenes_code ^String descrip]
    (let [{scenes_name :name params :params trans :trans} (my-scenes-util/tran_obj scenes_code)]
        (let [m (MyScenesCache. group_id scenes_name scenes_code descrip false params (get_trans_to_json_lst trans) (ScenesType/TRAN))]
            (.put (.cache ignite "my_scenes") (str/lower-case scenes_name) m)))
    )

; 调用
(defn call_scenes [^Ignite ignite ^Long group_id ^String scenes_name ^clojure.lang.PersistentArrayMap dic_paras]
    (if-let [vs (.get (.cache ignite "my_scenes") (str/lower-case scenes_name))]
        (if (= (.getGroup_id vs) group_id)
            (tran_run_fun_ast ignite group_id (.getAst vs) dic_paras)
            (if-let [m_group_id (MyDbUtil/getGroupIdByCall ignite group_id scenes_name)]
                (tran_run_fun_ast ignite m_group_id (.getAst vs) dic_paras)
                (throw (Exception. (format "用户组 %s 没有执行权限！" group_id)))))
        (throw (Exception. (format "场景名称 %s 不存在！" scenes_name)))))

(defn my_call_scenes [^Ignite ignite ^Long group_id ^clojure.lang.PersistentArrayMap vs ^java.util.ArrayList lst_paras]
    (let [dic_paras (my-lexical/get_scenes_dic vs lst_paras)]
        (if (= (.getGroup_id vs) group_id)
            (tran_run_fun_ast ignite group_id (.getAst vs) dic_paras)
            (if-let [m_group_id (MyDbUtil/getGroupIdByCall ignite group_id (.getScenes_name vs))]
                (tran_run_fun_ast ignite m_group_id (.getAst vs) dic_paras)
                (throw (Exception. (format "用户组 %s 没有执行权限！" group_id))))))
    )

(defn -my_call_scenes [^Ignite ignite ^Long group_id ^clojure.lang.PersistentArrayMap vs ^java.util.ArrayList lst_paras]
    (my_call_scenes ignite group_id vs lst_paras)
    )
















































