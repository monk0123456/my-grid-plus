(ns org.gridgain.plus.dml.my-scenes
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.dml.my-scenes-util :as my-scenes-util]
        [org.gridgain.plus.dml.my-trans :as my-trans]
        [org.gridgain.plus.context.my-context :as my-context]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.gridgain.myservice MyCronService)
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
        :implements [org.gridgain.superservice.IMyScenes]
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyScenes
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [superInvoke [org.apache.ignite.Ignite String java.util.ArrayList] Object]]
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

; 以下是保存到 cache 中的 scenes_name, ast, 参数列表
(defn save_scenes [^Ignite ignite ^Long group_id lst_scenes_code]
    (let [{scenes_type :scenes_type scenes_obj :obj} (my-scenes-util/my_scenes_obj lst_scenes_code)]
        (cond (= scenes_type "scenes") (let [{scenes_name :name params :params sql :sql descrip :descrip is_batch :is_batch} scenes_obj]
                                           (cond (my-lexical/is-eq? (first sql) "select") (my-select/save_scenes ignite group_id scenes_name lst_scenes_code sql descrip params is_batch)
                                                 (my-lexical/is-eq? (first sql) "insert") (my-insert/save_scenes ignite group_id scenes_name lst_scenes_code sql descrip params is_batch)
                                                 (my-lexical/is-eq? (first sql) "update") (my-update/save_scenes ignite group_id scenes_name lst_scenes_code sql descrip params is_batch)
                                                 (my-lexical/is-eq? (first sql) "delete") (my-delete/save_scenes ignite group_id scenes_name lst_scenes_code sql descrip params is_batch)
                                                 )
                                           )
              (= scenes_type "tran") (let [{scenes_name :name params :params trans :trans descrip :descrip is_batch :is_batch} scenes_obj]
                                           (let [m (MyScenesCache. group_id scenes_name lst_scenes_code descrip is_batch params (my-trans/get_trans_to_json_lst trans) (ScenesType/TRAN))]
                                               (.put (.cache ignite "my_scenes") (str/lower-case scenes_name) m)))
              (= scenes_type "cron") (let [{scenes_name :name params :params descrip :descrip is_batch :is_batch} scenes_obj]
                                           (let [m (MyScenesCache. group_id scenes_name lst_scenes_code descrip is_batch params (.addJob (.getMyCron (MyCronService/getInstance)) ignite group_id scenes_obj) (ScenesType/CRON))]
                                               (.put (.cache ignite "my_scenes") (str/lower-case scenes_name) m)))
              )))

; myInvoke sql 的方法
(defn my-invoke [^Ignite ignite ^String methodName ^ArrayList lst]
    (if-let [vs (.get (.cache ignite "my_scenes") (str/lower-case methodName))]
        (cond (= (.getScenesType vs) (ScenesType/INSERT)) (my-insert/my_call_scenes ignite methodName vs lst)
              (= (.getScenesType vs) (ScenesType/UPDATE)) (my-update/my_call_scenes ignite methodName vs lst)
              (= (.getScenesType vs) (ScenesType/DELETE)) (my-delete/my_call_scenes ignite methodName vs lst)
              (= (.getScenesType vs) (ScenesType/SELECT)) (my-select/my_call_scenes ignite methodName vs lst)
              (= (.getScenesType vs) (ScenesType/TRAN)) (my-trans/my_call_scenes ignite methodName vs lst)
              :else
              (throw (Exception. (format "%s 场景错误！" methodName)))
              )
        (throw (Exception. (format "%s 场景不存在！" methodName)))))

; myInvoke sql 的方法
(defn -myInvoke [this ^Ignite ignite ^String methodName ^ArrayList lst]
    (my-invoke ignite methodName lst))

; myInvoke sql 的方法
(defn -superInvoke [^Ignite ignite ^String methodName ^ArrayList lst]
    (my-invoke ignite methodName lst))















































































