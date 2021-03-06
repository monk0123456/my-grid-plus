(ns org.gridgain.plus.dml.my-smart-db-line
    (:require
        [org.gridgain.plus.ddl.my-create-table :as my-create-table]
        [org.gridgain.plus.ddl.my-alter-table :as my-alter-table]
        [org.gridgain.plus.ddl.my-create-index :as my-create-index]
        [org.gridgain.plus.ddl.my-drop-index :as my-drop-index]
        [org.gridgain.plus.ddl.my-drop-table :as my-drop-table]
        [org.gridgain.plus.ddl.my-create-dataset :as my-create-dataset]
        [org.gridgain.plus.ddl.my-alter-dataset :as my-alter-dataset]
        [org.gridgain.plus.ddl.my-drop-dataset :as my-drop-dataset]
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.dml.my-smart-func-args-token-clj :as my-smart-func-args-token-clj]
        [org.gridgain.plus.dml.my-select-plus-args :as my-select-plus-args]
        [org.gridgain.plus.dml.my-smart-db :as my-smart-db]
        [org.gridgain.plus.nosql.my-super-cache :as my-super-cache]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (com.google.common.base Strings)
             (org.gridgain.smart MyVar MyLetLayer)
             (org.tools MyConvertUtil KvSql MyDbUtil MyLineToBinary)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache MyNoSqlCache SqlType)
             (org.gridgain.dml.util MyCacheExUtil)
             (cn.plus.model.db MyScenesCache ScenesType MyScenesParams MyScenesParamsPk)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache CacheMode CacheAtomicityMode)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (org.apache.ignite.binary BinaryObjectBuilder BinaryObject)
             (java.util ArrayList List Date Iterator Hashtable)
             (java.sql Timestamp)
             (java.math BigDecimal)
             (cn.log MyLogger)
             )
    (:gen-class
        ; ?????? class ?????????
        :name org.gridgain.plus.dml.MySmartDbLine
        ; ???????????? class ??? main ??????
        :main false
        ; ?????? java ???????????????
        ;:methods [^:static [superSql [org.apache.ignite.Ignite Object Object] String]
        ;          ^:static [getGroupId [org.apache.ignite.Ignite String] Boolean]]
        ))

(defn insert-to-cache [ignite group_id lst]
    (let [insert_obj (my-insert/my_insert_obj ignite group_id lst)]
        (let [{pk_rs :pk_rs data_rs :data_rs} (my-insert/get_pk_data_with_data (my-insert/get_pk_data ignite (-> insert_obj :schema_name) (-> insert_obj :table_name)) insert_obj)]
            (MyLogCache. (format "f_%s_%s" (str/lower-case (-> insert_obj :schema_name)) (str/lower-case (-> insert_obj :table_name))) (-> insert_obj :schema_name) (-> insert_obj :table_name) (my-smart-db/get-insert-pk ignite group_id pk_rs {:dic {}, :keys []}) (my-smart-db/get-insert-data ignite group_id data_rs {:dic {}, :keys []}) (SqlType/INSERT))))
    )

(defn insert-to-cache-no-authority [ignite group_id lst]
    (let [insert_obj (my-insert/my_insert_obj-no-authority ignite group_id lst)]
        (let [{pk_rs :pk_rs data_rs :data_rs} (my-insert/get_pk_data_with_data (my-insert/get_pk_data ignite (-> insert_obj :schema_name) (-> insert_obj :table_name)) insert_obj)]
            (MyLogCache. (format "f_%s_%s" (str/lower-case (-> insert_obj :schema_name)) (str/lower-case (-> insert_obj :table_name))) (-> insert_obj :schema_name) (-> insert_obj :table_name) (my-smart-db/get-insert-pk ignite group_id pk_rs {:dic {}, :keys []}) (my-smart-db/get-insert-data ignite group_id data_rs {:dic {}, :keys []}) (SqlType/INSERT))))
    )

(defn update-to-cache [ignite group_id lst]
    (let [{schema_name :schema_name table_name :table_name query-lst :query-lst sql :sql items :items select-args :args} (my-update/my_update_obj ignite group_id lst {})]
        (loop [it (.iterator (.query (.getOrCreateCache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                                              (.setArgs (to-array select-args))
                                                                                                              (.setLazy true)))) lst-rs []]
            (if (.hasNext it)
                (if-let [row (.next it)]
                    (recur it (conj lst-rs (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name (my-smart-db/get-update-key row (filter #(-> % :is-pk) query-lst)) (my-smart-db/get-update-value ignite group_id row (filter #(false? (-> % :is-pk)) query-lst) {:dic {}, :keys []} items) (SqlType/UPDATE))))
                    )
                lst-rs))))

(defn update-to-cache-no-authority [ignite group_id lst]
    (let [{schema_name :schema_name table_name :table_name query-lst :query-lst sql :sql items :items select-args :args} (my-update/my_update_obj-authority ignite group_id lst {})]
        (loop [it (.iterator (.query (.getOrCreateCache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                                              (.setArgs (to-array select-args))
                                                                                                              (.setLazy true)))) lst-rs []]
            (if (.hasNext it)
                (if-let [row (.next it)]
                    (recur it (conj lst-rs (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name (my-smart-db/get-update-key row (filter #(-> % :is-pk) query-lst)) (my-smart-db/get-update-value ignite group_id row (filter #(false? (-> % :is-pk)) query-lst) {:dic {}, :keys []} items) (SqlType/UPDATE))))
                    )
                lst-rs))))

(defn delete-to-cache [ignite group_id lst]
    (let [{schema_name :schema_name table_name :table_name sql :sql select-args :args pk_lst :pk_lst} (my-delete/my_delete_obj ignite group_id lst {})]
        (loop [it (.iterator (.query (.getOrCreateCache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                                              (.setArgs (to-array select-args))
                                                                                                              (.setLazy true)))) lst-rs []]
            (if (.hasNext it)
                (if-let [row (.next it)]
                    (recur it (conj lst-rs (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name (my-smart-db/get-delete-key row pk_lst) nil (SqlType/DELETE))))
                    )
                lst-rs))))

(defn delete-to-cache-no-authority [ignite group_id lst]
    (let [{schema_name :schema_name table_name :table_name sql :sql select-args :args pk_lst :pk_lst} (my-delete/my_delete_obj-no-authority ignite group_id lst {})]
        (loop [it (.iterator (.query (.getOrCreateCache ignite (format "f_%s_%s" schema_name table_name)) (doto (SqlFieldsQuery. sql)
                                                                                                              (.setArgs (to-array select-args))
                                                                                                              (.setLazy true)))) lst-rs []]
            (if (.hasNext it)
                (if-let [row (.next it)]
                    (recur it (conj lst-rs (MyLogCache. (format "f_%s_%s" schema_name table_name) schema_name table_name (my-smart-db/get-delete-key row pk_lst) nil (SqlType/DELETE))))
                    )
                lst-rs))))

(defn query-sql-no-args [ignite group_id lst]
    (cond (my-lexical/is-eq? "select" (first lst)) (if-let [ast (my-select-plus/sql-to-ast lst)]
                                                       (-> (my-select-plus-args/my-ast-to-sql ignite group_id nil ast) :sql))
          (my-lexical/is-eq? "insert" (first lst)) (let [logCache (insert-to-cache ignite group_id lst)]
                                                       (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList [logCache])))
                                                           "select show_msg('true') as tip;"
                                                           "select show_msg('false') as tip;"))
          (my-lexical/is-eq? "update" (first lst)) (let [logCache (update-to-cache ignite group_id lst)]
                                                       (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
                                                           "select show_msg('true') as tip;"
                                                           "select show_msg('false') as tip;"))
          (my-lexical/is-eq? "delete" (first lst)) (let [logCache (delete-to-cache ignite group_id lst)]
                                                       (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
                                                           "select show_msg('true') as tip;"
                                                           "select show_msg('false') as tip;"))
          :else
          (throw (Exception. "query_sql ???????????? DML ?????????"))
          ))

(defn query-sql-no-args-log-no-authority [ignite group_id lst]
    (cond (my-lexical/is-eq? "select" (first lst)) (if-let [ast (my-select-plus/sql-to-ast lst)]
                                                       (-> (my-select-plus-args/my-ast-to-sql-no-authority ignite group_id nil ast) :sql))
          (my-lexical/is-eq? "insert" (first lst)) (let [logCache (insert-to-cache-no-authority ignite group_id lst)]
                                                       (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList [logCache])))
                                                           "select show_msg('true') as tip;"
                                                           "select show_msg('false') as tip;"))
          (my-lexical/is-eq? "update" (first lst)) (let [logCache (update-to-cache-no-authority ignite group_id lst)]
                                                       (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
                                                           "select show_msg('true') as tip;"
                                                           "select show_msg('false') as tip;"))
          (my-lexical/is-eq? "delete" (first lst)) (let [logCache (delete-to-cache-no-authority ignite group_id lst)]
                                                       (if (nil? (MyCacheExUtil/transLogCache ignite (my-lexical/to_arryList logCache)))
                                                           "select show_msg('true') as tip;"
                                                           "select show_msg('false') as tip;"))
          :else
          (throw (Exception. "query_sql ???????????? DML ?????????"))
          ))

; 1??????  my log???????????????
; 2?????? my log, ????????????

; 1??????  my log???????????????
(defn my-log-authority [ignite group_id lst]
    (query-sql-no-args ignite group_id lst))

; 2?????? my log, ????????????
(defn my-log-no-authority [ignite group_id lst]
    (query-sql-no-args-log-no-authority ignite group_id lst))

(defn query_sql [ignite group_id lst]
    (if (.isMultiUserGroup (.configuration ignite))
        (my-log-authority ignite group_id lst)
        (my-log-no-authority ignite group_id lst)
        ))












































