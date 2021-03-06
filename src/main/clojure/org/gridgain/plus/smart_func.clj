(ns org.gridgain.plus.smart-func
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.dml.my-select-plus-args :as my-select-plus-args]
        [org.gridgain.plus.sql.my-smart-scenes :as my-smart-scenes]
        [clojure.core.reducers :as r]
        [clojure.string :as str]
        [clojure.walk :as w])
    (:import (org.apache.ignite Ignite)
             (org.tools MyConvertUtil MyPlusUtil KvSql MyDbUtil)
             (com.google.common.base Strings)
             (org.gridgain.dml.util MyCacheExUtil)
             (cn.plus.model.db MyScenesCache ScenesType MyScenesParams MyScenesParamsPk MyScenesCachePk)
             (cn.plus.model MyCacheEx MyKeyValue MyLogCache MCron SqlType)
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
        :methods [^:static [smart_view [org.apache.ignite.Ignite Long String] String]]
        ))

(defn cron-to-str
    ([lst] (cron-to-str lst []))
    ([[f & r] lst]
     (if (some? f)
         (if (= "*" (first r))
             (recur r (concat lst [f " "]))
             (recur r (concat lst [f])))
         (str/join lst))))

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

; 添加 job
(defn add-job [^Ignite ignite ^Long group_id ^String job-name ^Object ps ^String cron]
    (if-let [scheduleProcessor (MyPlusUtil/getIgniteScheduleProcessor ignite)]
        (if-let [scheduledFutures (.getScheduledFutures scheduleProcessor)]
            (if (.containsKey scheduledFutures job-name)
                (throw (Exception. (format "已存在任务 %s 不能添加相同的任务名！" job-name)))
                (try
                    (let [cron-line (cron-to-str (my-lexical/to-back cron)) my-cron-cache (.cache ignite "my_cron")]
                        (if-not (nil? (.scheduleLocal (.scheduler ignite) job-name (proxy [Object Runnable] []
                                                                                       (run []
                                                                                           (my-smart-scenes/my-invoke-scenes ignite group_id job-name ps)))
                                                      cron-line))
                            (.put my-cron-cache job-name (MCron. job-name cron-line (MyCacheExUtil/objToBytes ps)))))
                    (catch Exception ex
                        (.remove scheduledFutures job-name))
                    )
                ))))

; 删除 job
(defn remove-job [^Ignite ignite ^Long group_id ^String job-name]
    (if-let [scheduleProcessor (MyPlusUtil/getIgniteScheduleProcessor ignite)]
        (if-let [scheduledFutures (.getScheduledFutures scheduleProcessor)]
            (let [job-cache (.cache ignite "my_cron")]
                (if-let [job-obj (.get job-cache job-name)]
                    (if (.containsKey scheduledFutures job-name)
                        (try
                            (.remove scheduledFutures job-name)
                            (.remove job-cache job-name)
                            (catch Exception ex
                                (add-job ignite group_id job-name (MyCacheExUtil/restore (.getPs job-obj)) (.getCron job-obj)))
                            )
                        (throw (Exception. (format "任务 %s 不存在！" job-name)))))
                )
            )))

(defn _smart_view [^Ignite ignite ^Long group_id ^String code]
    (smart-view ignite group_id code))































































