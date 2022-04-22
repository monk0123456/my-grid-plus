(ns org.gridgain.plus.dml.my-select-plus-args
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.context.my-context :as my-context]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [clojure.core.reducers :as r]
        [clojure.string :as str]
        [clojure.walk :as w])
    (:import (org.apache.ignite Ignite)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil MyDbUtil KvSql)
             (cn.plus.model.db MyScenesCache ScenesType MyScenesParams MyScenesParamsPk)
             (org.apache.ignite.cache.query SqlFieldsQuery)
             (java.math BigDecimal)
             (java.util List ArrayList Date Iterator)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MySelectPlusArgs
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [get_plus_sql [org.apache.ignite.Ignite Long String] String]
        ;          ^:static [getSqlToAst [org.apache.ignite.Ignite String String] clojure.lang.LazySeq]
        ;          ^:static [putAstCache [org.apache.ignite.Ignite String String String] void]]
        ))

; ast to sql
(defn ast_to_sql [ignite group_id dic-args ast]
    (letfn [(select_to_sql_single [ignite group_id dic-args ast]
                (if (and (some? ast) (map? ast))
                    (when-let [{query-items :query-items table-items :table-items where-items :where-items group-by :group-by having :having order-by :order-by limit :limit} ast]
                        (cond (and (some? query-items) (some? table-items) (empty? where-items) (empty? group-by) (empty? having) (empty? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items)]
                                                                                                                                                                        {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args)))})
                              (and (some? query-items) (some? table-items) (some? where-items) (empty? group-by) (empty? having) (empty? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items)]
                                                                                                                                                                       {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args)))})
                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (empty? having) (empty? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-3 (lst-token-to-line ignite group_id dic-args group-by)]
                                                                                                                                                                      {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "group by" (-> tk-3 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args)))})
                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (some? having) (empty? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-3 (lst-token-to-line ignite group_id dic-args group-by) tk-4 (lst-token-to-line ignite group_id dic-args having)]
                                                                                                                                                                     {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "group by" (-> tk-3 :sql) "having" (-> tk-4 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args) (-> tk-4 :args)))})
                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (some? having) (some? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-3 (lst-token-to-line ignite group_id dic-args group-by) tk-4 (lst-token-to-line ignite group_id dic-args having) tk-5 (lst-token-to-line ignite group_id dic-args order-by)]
                                                                                                                                                                    {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "group by" (-> tk-3 :sql) "having" (-> tk-4 :sql) "order by" (-> tk-5 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args) (-> tk-4 :args) (-> tk-5 :args)))})

                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (some? having) (some? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-3 (lst-token-to-line ignite group_id dic-args group-by) tk-4 (lst-token-to-line ignite group_id dic-args having) tk-5 (lst-token-to-line ignite group_id dic-args order-by) tk-6 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                   {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "group by" (-> tk-3 :sql) "having" (-> tk-4 :sql) "order by" (-> tk-5 :sql) "limit" (-> tk-6 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args) (-> tk-4 :args) (-> tk-5 :args) (-> tk-6 :args)))})

                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (some? having) (empty? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-3 (lst-token-to-line ignite group_id dic-args group-by) tk-4 (lst-token-to-line ignite group_id dic-args having) tk-5 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                    {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "group by" (-> tk-3 :sql) "having" (-> tk-4 :sql) "limit" (-> tk-5 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args) (-> tk-4 :args) (-> tk-5 :args)))})

                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (empty? having) (some? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-3 (lst-token-to-line ignite group_id dic-args group-by) tk-4 (lst-token-to-line ignite group_id dic-args order-by)]
                                                                                                                                                                     {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "group by" (-> tk-3 :sql) "order by" (-> tk-4 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args) (-> tk-4 :args)))})

                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (empty? having) (empty? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-3 (lst-token-to-line ignite group_id dic-args group-by) tk-4 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                     {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "group by" (-> tk-3 :sql) "limit" (-> tk-4 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args) (-> tk-4 :args)))})
                              (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (empty? having) (some? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-3 (lst-token-to-line ignite group_id dic-args group-by) tk-5 (lst-token-to-line ignite group_id dic-args order-by) tk-6 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                    {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "group by" (-> tk-3 :sql) "order by" (-> tk-5 :sql) "limit" (-> tk-6 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args) (-> tk-5 :args) (-> tk-6 :args)))})

                              (and (some? query-items) (some? table-items) (some? where-items) (empty? group-by) (empty? having) (some? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-5 (lst-token-to-line ignite group_id dic-args order-by)]
                                                                                                                                                                      {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "order by" (-> tk-5 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-5 :args)))})
                              (and (some? query-items) (some? table-items) (some? where-items) (empty? group-by) (empty? having) (empty? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-5 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                      {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "limit" (-> tk-5 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-5 :args)))})
                              (and (some? query-items) (some? table-items) (some? where-items) (empty? group-by) (empty? having) (some? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args where-items) tk-5 (lst-token-to-line ignite group_id dic-args order-by) tk-6 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                     {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "where" (-> tk-2 :sql) "order by" (-> tk-5 :sql) "limit" (-> tk-6 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-5 :args) (-> tk-6 :args)))})

                              (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (empty having) (empty? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args group-by)]
                                                                                                                                                                      {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "group by" (-> tk-2 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args)))})
                              (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (some? having) (empty? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args group-by) tk-3 (lst-token-to-line ignite group_id dic-args having)]
                                                                                                                                                                      {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "group by" (-> tk-2 :sql) "having" (-> tk-3 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args)))})
                              (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (some? having) (some? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args group-by) tk-3 (lst-token-to-line ignite group_id dic-args having) tk-4 (lst-token-to-line ignite group_id dic-args order-by)]
                                                                                                                                                                     {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "group by" (-> tk-2 :sql) "having" (-> tk-3 :sql) "order by" (-> tk-4 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args) (-> tk-4 :args)))})
                              (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (some? having) (some? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args group-by) tk-3 (lst-token-to-line ignite group_id dic-args having) tk-4 (lst-token-to-line ignite group_id dic-args order-by) tk-5 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                    {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "group by" (-> tk-2 :sql) "having" (-> tk-3 :sql) "order by" (-> tk-4 :sql) "limit" (-> tk-5 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args) (-> tk-4 :args) (-> tk-5 :args)))})

                              (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (empty having) (empty? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args group-by) tk-5 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                     {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "group by" (-> tk-2 :sql) "limit" (-> tk-5 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-5 :args)))})
                              (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (some? having) (empty? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args group-by) tk-3 (lst-token-to-line ignite group_id dic-args having) tk-5 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                     {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "group by" (-> tk-2 :sql) "having" (-> tk-3 :sql) "limit" (-> tk-5 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args) (-> tk-5 :args)))})

                              (and (some? query-items) (some? table-items) (empty? where-items) (empty? group-by) (empty? having) (some? order-by) (empty? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args order-by)]
                                                                                                                                                                       {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "order by" (-> tk-2 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args)))})
                              (and (some? query-items) (some? table-items) (empty? where-items) (empty? group-by) (empty? having) (some? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-2 (lst-token-to-line ignite group_id dic-args order-by) tk-3 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                      {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "order by" (-> tk-2 :sql) "limit" (-> tk-3 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-2 :args) (-> tk-3 :args)))})
                              (and (some? query-items) (some? table-items) (empty? where-items) (empty? group-by) (empty? having) (empty? order-by) (some? limit)) (let [tk (lst-token-to-line ignite group_id dic-args query-items) tk-1 (lst-token-to-line ignite group_id dic-args table-items) tk-3 (lst-token-to-line ignite group_id dic-args limit)]
                                                                                                                                                                       {:sql (.toString (ignite group_id my-select-plus/ar-to-sql ["select" (-> tk :sql) "from" (-> tk-1 :sql) "limit" (-> tk-3 :sql)] (StringBuilder.))) :args (filter #(not (nil? %)) (concat (-> tk :args) (-> tk-1 :args) (-> tk-3 :args)))})

                              ))))
            (get-map-token-to-sql [m]
                (loop [[f & r] m lst-sql [] lst-args []]
                    (if (some? f)
                        (let [{sql :sql args :args} f]
                            (recur r (concat lst-sql [sql]) (concat lst-args args))
                            )
                        {:sql lst-sql :args lst-args})))
            (lst-token-to-line
                ([ignite group_id dic-args lst_token] (cond (string? lst_token) lst_token
                                                   (map? lst_token) (let [{sql :sql args :args} (token-to-sql ignite group_id dic-args lst_token)]
                                                                        {:sql (my-select-plus/my-array-to-sql sql) :args args})
                                                   :else
                                                            (let [{sql :sql args :args} (lst-token-to-line ignite group_id dic-args lst_token [] [])]
                                                                {:sql (my-select-plus/my-array-to-sql sql) :args args})
                                                   ))
                ([ignite group_id dic-args [f & rs] lst lst-args]
                 (if (some? f)
                     (let [{sql :sql args :args} (token-to-sql ignite group_id dic-args f)]
                         (recur ignite group_id dic-args rs (conj lst (my-select-plus/my-array-to-sql sql)) (concat lst-args args)))
                     {:sql lst :args lst-args})))
            (token-to-sql [ignite group_id dic-args m]
                (if (some? m)
                    (cond (my-lexical/is-seq? m) (get-map-token-to-sql (map (partial token-to-sql ignite group_id dic-args) m))
                          (map? m) (map-token-to-sql ignite group_id dic-args m))))
            (map-token-to-sql
                [ignite group_id dic-args m]
                (if (some? m)
                    (cond
                        (contains? m :sql_obj) (select-to-sql ignite group_id dic-args m)
                        (and (contains? m :func-name) (contains? m :lst_ps)) (func-to-line ignite group_id dic-args m)
                        (contains? m :and_or_symbol) {:sql (get m :and_or_symbol) :args nil} ;(get m :and_or_symbol)
                        (contains? m :keyword) {:sql (get m :keyword) :args nil} ;(get m :keyword)
                        (contains? m :operation) (get-map-token-to-sql (map (partial token-to-sql ignite group_id dic-args) (get m :operation)))
                        (contains? m :comparison_symbol) {:sql (get m :comparison_symbol) :args nil} ; (get m :comparison_symbol)
                        (contains? m :in_symbol) {:sql (get m :in_symbol) :args nil} ; (get m :in_symbol)
                        (contains? m :operation_symbol) {:sql (get m :operation_symbol) :args nil} ; (get m :operation_symbol)
                        (contains? m :join) {:sql (get m :join) :args nil} ;(get m :join)
                        (contains? m :on) (on-to-line ignite group_id dic-args m)
                        (contains? m :comma_symbol) {:sql (get m :comma_symbol) :args nil} ;(get m :comma_symbol)
                        (contains? m :order-item) (let [{sql :sql args :args} (token-to-sql ignite group_id dic-args (-> m :order-item))]
                                                      {:sql (concat sql [(-> m :order)]) :args args})
                        (contains? m :item_name) (item-to-line dic-args m)
                        (contains? m :table_name) (table-to-line ignite group_id dic-args m)
                        (contains? m :exists) (let [{sql :sql args :args} (token-to-sql ignite group_id dic-args (get (get m :select_sql) :parenthesis))]
                                                  {:sql (concat [(get m :exists) "("] sql [")"]) :args args})
                        (contains? m :parenthesis) (let [{sql :sql args :args} (token-to-sql ignite group_id dic-args (get m :parenthesis))]
                                                       {:sql (concat ["("] sql [")"]) :args args})
                        :else
                        (throw (Exception. "select 语句错误！请仔细检查！"))
                        )))
            (on-to-line [ignite group_id dic-args m]
                (if (some? m)
                    (let [{sql :sql args :args} (token-to-sql ignite group_id dic-args (get m :on))]
                        {:sql (str/join ["on" sql]) :args args})
                    ))
            (func-to-line [ignite group_id dic-args m]
                (if (and (contains? m :alias) (not (nil? (-> m :alias))))
                    (let [{sql :sql args :args} (get-map-token-to-sql (map (partial token-to-sql ignite group_id dic-args) (-> m :lst_ps)))]
                        {:sql (concat [(-> m :func-name) "("] sql [")" " as"] [(-> m :alias)]) :args args})
                    (let [{sql :sql args :args} (get-map-token-to-sql (map (partial token-to-sql ignite group_id dic-args) (-> m :lst_ps)))]
                        {:sql (concat [(-> m :func-name) "("] sql [")"]) :args args})
                    ))
            (item-to-line [dic-args m]
                (let [{table_alias :table_alias item_name :item_name alias :alias} m]
                    (cond
                        (and (not (Strings/isNullOrEmpty table_alias)) (not (nil? alias)) (not (Strings/isNullOrEmpty alias))) {:sql (str/join [table_alias "." item_name " as " alias]) :args nil}
                        (and (not (Strings/isNullOrEmpty table_alias)) (Strings/isNullOrEmpty alias)) {:sql (str/join [table_alias "." item_name]) :args nil}
                        (and (Strings/isNullOrEmpty table_alias) (Strings/isNullOrEmpty alias)) (if (contains? dic-args item_name)
                                                                                                    {:sql item_name :args [(get dic-args item_name)]}
                                                                                                    {:sql item_name :args nil})
                        )))
            (table-to-line [ignite group_id dic-args m]
                (if (some? m)
                    (if-let [{table_name :table_name table_alias :table_alias} m]
                        (if (Strings/isNullOrEmpty table_alias)
                            (let [data_set_name (get_data_set_name ignite group_id dic-args)]
                                (if (Strings/isNullOrEmpty data_set_name)
                                    {:sql table_name :args nil}
                                    {:sql (str/join [data_set_name "." table_name]) :args nil}))
                            (let [data_set_name (get_data_set_name ignite group_id dic-args)]
                                (if (Strings/isNullOrEmpty data_set_name)
                                    {:sql (str/join [table_name " " table_alias]) :args nil}
                                    {:sql (str/join [(str/join [data_set_name "." table_name]) " " table_alias]) :args nil}
                                    ))
                            ))))
            ; 获取 data_set 的名字和对应的表
            (get_data_set_name [^Ignite ignite ^Long group_id]
                (when-let [m (first (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. "select m.dataset_name from my_users_group as g JOIN my_dataset as m ON m.id = g.data_set_id where g.id = ?") (to-array [group_id])))))]
                    (first m)))
            (select-to-sql
                ([ignite group_id dic-args ast]
                 (cond (and (some? ast) (instance? clojure.lang.LazySeq ast)) (let [{sql :sql args :args} (select-to-sql ignite group_id dic-args ast [] [])]
                                                                                  {:sql (.toString (ignite group_id my-select-plus/ar-to-sql sql (StringBuilder.))) :args args})
                       (contains? ast :sql_obj) (select_to_sql_single ignite group_id dic-args (get ast :sql_obj))
                       :else
                       (throw (Exception. "select 语句错误！"))))
                ([ignite group_id dic-args [f & rs] lst_rs lst-args]
                 (if (some? f)
                     (if (map? f)
                         (cond (contains? f :sql_obj) (let [{sql :sql args :args} (select_to_sql_single ignite group_id dic-args (get f :sql_obj))]
                                                          (recur ignite group_id dic-args rs (conj lst_rs sql) (concat lst-args args)))
                               (contains? f :keyword) (recur ignite group_id dic-args rs (conj lst_rs (get f :keyword)) lst-args)
                               :else
                               (throw (Exception. "select 语句错误！"))) (throw (Exception. "select 语句错误！"))) lst_rs)))]
        (select-to-sql ignite group_id dic-args ast)))