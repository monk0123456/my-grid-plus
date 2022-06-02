(ns org.gridgain.plus.smart-func
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.dml.my-select-plus-args :as my-select-plus-args]
        [clojure.core.reducers :as r]
        [clojure.string :as str]
        [clojure.walk :as w])
    (:import (org.apache.ignite Ignite)
             (org.gridgain.smart MyVar MyLetLayer)
             (com.google.common.base Strings)
             (cn.plus.model MyKeyValue MyLogCache SqlType)
             (org.gridgain.dml.util MyCacheExUtil)
             (cn.plus.model.db MyScenesCache ScenesType MyScenesParams MyScenesParamsPk MyScenesCachePk)
             (org.apache.ignite.cache.query SqlFieldsQuery)
             (java.math BigDecimal)
             (java.util List ArrayList Hashtable Date Iterator)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.SmartFunc
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [get_plus_sql [org.apache.ignite.Ignite Long String] String]
        ;          ^:static [getSqlToAst [org.apache.ignite.Ignite String String] clojure.lang.LazySeq]
        ;          ^:static [putAstCache [org.apache.ignite.Ignite String String String] void]]
        ))

(defn get-data-set-id-by-group-id [^Ignite ignite ^Long group_id]
    (let [rs (first (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. "select m.id, m.dataset_name from my_users_group as g JOIN my_dataset as m ON m.id = g.data_set_id where g.id = ?") (to-array [group_id])))))]
        (first rs)))

(defn get-data-set-id-by-ds-name [^Ignite ignite ^String schema_name]
    (let [rs (first (.getAll (.query (.cache ignite "my_dataset") (.setArgs (SqlFieldsQuery. "select m.id from my_dataset as m where m.dataset_name = ?") (to-array [schema_name])))))]
        (first rs)))

(defn smart-view-select [^Ignite ignite ^Long group_id lst code]
    (let [ast (my-select-plus/sql-to-ast lst)]
        (if (= (count ast) 1)
            (let [{table-items :table-items} (-> (first ast) :table-items)]
                (if (= (count table-items) 1)
                    (if (Strings/isNullOrEmpty (-> (first table-items) :schema_name))
                        (.getAll (.query (.cache ignite "my_select_views") (.setArgs (SqlFieldsQuery. "insert into my_select_views (id, table_name, data_set_id, code) values (?, ?, ?, ?)") (to-array [(.incrementAndGet (.atomicSequence ignite "my_select_views" 0 true)) (-> (first table-items) :table_name) (get-data-set-id-by-group-id ignite group_id) code]))))
                        (.getAll (.query (.cache ignite "my_select_views") (.setArgs (SqlFieldsQuery. "insert into my_select_views (id, table_name, data_set_id, code) values (?, ?, ?, ?)") (to-array [(.incrementAndGet (.atomicSequence ignite "my_select_views" 0 true)) (-> (first table-items) :table_name) (get-data-set-id-by-ds-name ignite (-> (first table-items) :schema_name)) code])))))
                    )))))

(defn smart-view-insert [^Ignite ignite ^Long group_id lst code]
    (let [{schema_name :schema_name table_name :table_name vs-line :vs-line} (my-insert/insert-body (rest (rest lst)))]
        (if (Strings/isNullOrEmpty schema_name)
            (.getAll (.query (.cache ignite "my_insert_views") (.setArgs (SqlFieldsQuery. "insert into my_insert_views (id, table_name, data_set_id, code) values (?, ?, ?, ?)") (to-array [(.incrementAndGet (.atomicSequence ignite "my_insert_views" 0 true)) table_name (get-data-set-id-by-group-id ignite group_id) code]))))
            (.getAll (.query (.cache ignite "my_insert_views") (.setArgs (SqlFieldsQuery. "insert into my_insert_views (id, table_name, data_set_id, code) values (?, ?, ?, ?)") (to-array [(.incrementAndGet (.atomicSequence ignite "my_insert_views" 0 true)) table_name (get-data-set-id-by-ds-name ignite schema_name) code])))))))

(defn smart-view-update [^Ignite ignite ^Long group_id lst code]
    (let [{schema_name :schema_name table_name :table_name vs-line :vs-line} (my-update/get_table_name lst)]
        (if (Strings/isNullOrEmpty schema_name)
            (.getAll (.query (.cache ignite "my_update_views") (.setArgs (SqlFieldsQuery. "insert into my_update_views (id, table_name, data_set_id, code) values (?, ?, ?, ?)") (to-array [(.incrementAndGet (.atomicSequence ignite "my_update_views" 0 true)) table_name (get-data-set-id-by-group-id ignite group_id) code]))))
            (.getAll (.query (.cache ignite "my_update_views") (.setArgs (SqlFieldsQuery. "insert into my_update_views (id, table_name, data_set_id, code) values (?, ?, ?, ?)") (to-array [(.incrementAndGet (.atomicSequence ignite "my_update_views" 0 true)) table_name (get-data-set-id-by-ds-name ignite schema_name) code])))))))

(defn smart-view-delete [^Ignite ignite ^Long group_id lst code]
    (let [{schema_name :schema_name table_name :table_name where_lst :where_lst} (my-delete/get_table_name lst)]
        (if (Strings/isNullOrEmpty schema_name)
            (.getAll (.query (.cache ignite "my_delete_views") (.setArgs (SqlFieldsQuery. "insert into my_delete_views (id, table_name, data_set_id, code) values (?, ?, ?, ?)") (to-array [(.incrementAndGet (.atomicSequence ignite "my_delete_views" 0 true)) table_name (get-data-set-id-by-group-id ignite group_id) code]))))
            (.getAll (.query (.cache ignite "my_delete_views") (.setArgs (SqlFieldsQuery. "insert into my_delete_views (id, table_name, data_set_id, code) values (?, ?, ?, ?)") (to-array [(.incrementAndGet (.atomicSequence ignite "my_delete_views" 0 true)) table_name (get-data-set-id-by-ds-name ignite schema_name) code])))))))

; 输入用户组和 code 名称添加 权限视图
(defn smart-view [^Ignite ignite ^Long group_id ^String code]
    (let [lst (my-lexical/to-back code)]
        (cond (my-lexical/is-eq? (first lst) "insert") (smart-view-insert ignite group_id lst code)
              (my-lexical/is-eq? (first lst) "update") (smart-view-update ignite group_id lst code)
              (my-lexical/is-eq? (first lst) "delete") (smart-view-delete ignite group_id lst code)
              (my-lexical/is-eq? (first lst) "select") (smart-view-select ignite group_id lst code)
              )))

; smart view 方法
(defn smart-func [^Ignite ignite ^Long group_id lst]
    ())
































































