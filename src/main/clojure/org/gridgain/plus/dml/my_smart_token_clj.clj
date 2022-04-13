(ns org.gridgain.plus.dml.my-smart-token-clj
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.sql.my-smart-scenes :as my-smart-scenes]
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
             (java.util List ArrayList Date Iterator)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MySmartTokenClj
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [get_plus_sql [org.apache.ignite.Ignite Long String] String]
        ;          ^:static [getSqlToAst [org.apache.ignite.Ignite String String] clojure.lang.LazySeq]
        ;          ^:static [putAstCache [org.apache.ignite.Ignite String String String] void]]
        ))

(declare is-symbol-priority run-express calculate is-func? is-scenes? func-to-clj item-to-clj token-lst-to-clj token-lst-clj
         token-to-clj map-token-to-clj)

; 判断符号优先级
; f symbol 的优先级大于等于 s 返回 true 否则返回 false
(defn is-symbol-priority [f s]
    (cond (or (= (-> f :operation_symbol) "*") (= (-> f :operation_symbol) "/")) true
          (and (or (= (-> f :operation_symbol) "+") (= (-> f :operation_symbol) "-")) (or (= (-> s :operation_symbol) "+") (= (-> s :operation_symbol) "-"))) true
          :else
          false))

(defn run-express [stack_number stack_symbo my-context]
    (if (some? (peek stack_symbo))
        (let [first_item (peek stack_number) second_item (peek (pop stack_number)) top_symbol (peek stack_symbo)]
            (cond (and (contains? (-> my-context :let-params) (-> first_item :item_name)) (contains? (-> my-context :let-params) (-> second_item :item_name)))
                  (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (.getVar %s) (.getVar %s))" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) my-context)
                  (contains? (-> my-context :let-params) (-> first_item :item_name))
                  (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (.getVar %s) %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) my-context)
                  (contains? (-> my-context :let-params) (-> second_item :item_name))
                  (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s (.getVar %s))" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) my-context)
                  :else
                  (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo) my-context)
                  ))
        (-> (first stack_number) :item_name)
        ;(first stack_number)
        ))

(defn calculate
    ([^Ignite ignite ^Long group_id lst my-context] (calculate ignite group_id lst [] [] my-context))
    ([^Ignite ignite ^Long group_id [f & r] stack_number stack_symbol my-context]
     (if (some? f)
         (cond (contains? f :operation_symbol) (cond
                                                   ; 若符号栈为空，则符号直接压入符号栈
                                                   (= (count stack_symbol) 0) (recur ignite group_id r stack_number (conj stack_symbol f) my-context)
                                                   ; f 符号的优先级高于或等于符号栈栈顶的优先级，则直接入栈
                                                   (is-symbol-priority f (peek stack_symbol)) (recur ignite group_id r stack_number (conj stack_symbol f) my-context)
                                                   ; f 符号的优先级低于栈顶的优先级，则将符号栈顶，弹出参与计算后，在压入，数据栈
                                                   :else
                                                   (let [first_item (peek stack_number) second_item (peek (pop stack_number)) top_symbol (peek stack_symbol)]
                                                       ;(recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) my-context)
                                                       (cond (and (contains? (-> my-context :let-params) (-> first_item :item_name)) (contains? (-> my-context :let-params) (-> second_item :item_name)))
                                                             (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (.getVar %s) (.getVar %s))" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) my-context)
                                                             (contains? (-> my-context :let-params) (-> first_item :item_name))
                                                             (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (.getVar %s) %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) my-context)
                                                             (contains? (-> my-context :let-params) (-> second_item :item_name))
                                                             (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s (.getVar %s))" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) my-context)
                                                             :else
                                                             (recur ignite group_id r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) my-context)
                                                             ))
                                                   )
               (contains? f :parenthesis) (let [m (calculate ignite group_id (reverse (-> f :parenthesis)) my-context)]
                                              (recur ignite group_id r (conj stack_number {:table_alias "", :item_name m, :item_type "", :java_item_type java.lang.Object, :const false}) stack_symbol my-context))
               (contains? f :item_name) (recur ignite group_id r (conj stack_number f) stack_symbol my-context)
               (contains? f :func-name) (recur ignite group_id r (conj stack_number (func-to-clj ignite group_id f false my-context)) stack_symbol my-context)
               :else
               (recur ignite group_id r (conj stack_number f) stack_symbol my-context)
               )
         (run-express stack_number stack_symbol my-context))))

; 判断 func
(defn is-func? [^Ignite ignite ^String func-name]
    (.containsKey (.cache ignite "my_func") func-name))

; 判断 scenes
(defn is-scenes? [^Ignite ignite ^Long group_id ^String scenes-name]
    (.containsKey (.cache ignite "my_scenes") (MyScenesCachePk. group_id scenes-name)))

; 调用方法这个至关重要
(defn func-to-clj [^Ignite ignite group_id m my-context]
    (let [{func-name :func-name lst_ps :lst_ps} m]
        (cond (my-lexical/is-eq? func-name "trans") (let [t_f (gensym "t_f") t_r (gensym "t_r") lst-rs (gensym "lst-rs-")]
                                                        (format "(loop [[f_%s & r_%s] %s lst-rs-%s []]
                                                                   (if (some? f_%s)
                                                                         (let [[sql args] f_%s]
                                                                                 (let [sql-lst (my-lexical/to-back (apply format sql (my-args args)))]
                                                                                     (cond (my-lexical/is-eq? (first sql-lst) \"insert\") (recur r_%s (conj lst-rs-%s (insert-to-cache ignite group_id %s sql-lst)))
                                                                                           (my-lexical/is-eq? (first sql-lst) \"update\") ()
                                                                                           (my-lexical/is-eq? (first sql-lst) \"delete\") ()
                                                                                     )))
                                                                         (if-not (empty? lst-rs-%s)
                                                                             (MyCacheExUtil/transCache ignite lst-rs-%s))
                                                                   ))" t_f t_r (-> (first lst_ps) :item_name) lst-rs
                                                                t_f
                                                                t_f
                                                                t_r lst-rs (str my-context)
                                                                lst-rs
                                                                lst-rs
                                                                ))
              (is-func? ignite func-name) (format "(my-smart-scenes/my-invoke-func ignite group_id %s %s)" func-name (token-to-clj ignite group_id lst_ps my-context))
              (is-scenes? ignite group_id func-name) (format "(my-smart-scenes/my-invoke-scenes ignite group_id %s %s)" func-name (token-to-clj ignite group_id lst_ps my-context))
              (my-lexical/is-eq? "log" func-name) (format "(log %s)" (token-to-clj ignite group_id lst_ps my-context))
              (my-lexical/is-eq? "println" func-name) (format "(println %s)" (token-to-clj ignite group_id lst_ps my-context))
              :else
              (println "Inner func")
              )))

(defn item-to-clj [m my-context]
    (if (contains? (-> my-context :let-params) (-> m :item_name))
        (format "(.getVar %s)" (-> m :item_name))
        (-> m :item_name)))

(defn token-lst-to-clj [ignite group_id m my-context]
    (if (and (>= (count m) 3) (contains? (nth m 1) :comparison_symbol) (= (-> (nth m 1) :comparison_symbol) "=")
             (contains? (nth m 0) :item_name))
        (format "(.setVar %s %s)" (-> (nth m 0) :item_name) (token-to-clj ignite group_id m my-context))
        (token-lst-clj ignite group_id m my-context)))

(defn token-lst-clj [ignite group_id m my-context]
    (loop [index 0 sb (StringBuilder.)]
        (if (< index (count m))
            (recur (+ index 1) (doto sb (.append (token-to-clj ignite group_id (nth m index) my-context))))
            (.toString sb))))

(defn token-to-clj [ignite group_id m my-context]
    (if (some? m)
        (cond (or (vector? m) (seq? m) (list? m)) (token-lst-to-clj ignite group_id m my-context)
              (map? m) (map-token-to-clj ignite group_id m my-context))))

(defn map-token-to-clj [ignite group_id m my-context]
    (if (some? m)
        (cond (and (contains? m :func-name) (contains? m :lst_ps)) (func-to-clj ignite group_id m my-context)
              (contains? m :and_or_symbol) (get m :and_or_symbol)
              (contains? m :operation) (token-lst-to-clj ignite group_id (get m :operation) my-context)
              (contains? m :comparison_symbol) (get m :comparison_symbol)
              (contains? m :operation_symbol) (get m :operation_symbol)
              (contains? m :comma_symbol) (get m :comma_symbol)
              (contains? m :item_name) (item-to-clj m my-context)
              (contains? m :parenthesis) (token-to-clj ignite group_id (get m :parenthesis) my-context)
              )))
