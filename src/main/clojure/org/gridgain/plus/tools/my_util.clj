(ns org.gridgain.plus.tools.my-util
    (:require
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache CacheMode)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             com.google.gson.GsonBuilder)
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.tools.MyUtil
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [gson [] com.google.gson.Gson]]
        ))

; 获取 gson 与 :methods 中定义的 gson 对应
;(defn -gson []
;    (.create (.setDateFormat (.enableComplexMapKeySerialization (GsonBuilder.)) "yyyy-MM-dd HH:mm:ss")))

;; 执行 sql
;(defn run-sql [sql args cache]
;    (.getAll (.query cache (.setArgs (SqlFieldsQuery. sql) args))))

; 调用方法
;(defn my_invoke [^Ignite ignite ^String scenes_name]
;    ())

; 通过 group_id 获取 data_set_id 和 is_real
(defn get_ds_by_group_id [^Ignite ignite ^Long group_id]
    (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. "SELECT m.data_set_id, ds.is_real from my_users_group m join my_dataset ds on m.data_set_id = ds.id where m.id = ? and m.group_type in ('ALL', 'DML')") (to-array [group_id])))))

; 通过 data_set_id 和 table_name 判断是否是非实时数据集中实时的表
(defn is_in_ds [^Ignite ignite ^Long data_set_id ^String table_name]
    (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. "SELECT m.id FROM my_dataset_table m WHERE m.table_name = ? AND m.dataset_id = ? and m.to_real = ?") (to-array [(str/lower-case table_name) data_set_id true])))))




















































