(ns org.gridgain.plus.dml.my-smart-sql
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [clojure.core.reducers :as r]
        [clojure.string :as str]
        [clojure.walk :as w])
    (:import (org.apache.ignite Ignite)
             (org.gridgain.smart MyVar)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil MyDbUtil KvSql)
             (cn.plus.model.db MyScenesCache ScenesType MyScenesParams MyScenesParamsPk)
             (org.apache.ignite.cache.query SqlFieldsQuery)
             (java.math BigDecimal)
             (java.util List ArrayList Date Iterator)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MySmartSql
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [get_plus_sql [org.apache.ignite.Ignite Long String] String]
        ;          ^:static [getSqlToAst [org.apache.ignite.Ignite String String] clojure.lang.LazySeq]
        ;          ^:static [putAstCache [org.apache.ignite.Ignite String String String] void]]
        ))

(defn query_sql [ignite sql & args]
    (if (nil? args)
        (.iterator (.query (.cache ignite "public_meta") (doto (SqlFieldsQuery. sql) (.setLazy true))))
        (.iterator (.query (.cache ignite "public_meta") (doto (SqlFieldsQuery. sql) (.setLazy true) (.setArgs (to-array args)))))))

; iterator 转 loop
(defn my-iterator [it]
    (if (.hasNext it)
        [(.next it) it]))

(defn split-pair-item
    ([lst] (split-pair-item lst [] [] []))
    ([[f & r] stack stack-lst lst]
     (if (some? f)
         (cond (= f "(") (recur r (conj stack f) (conj stack-lst f) lst)
               (= f ")") (recur r (pop stack) (conj stack-lst f) lst)
               (and (= f ":") (empty? stack) (not (empty? stack-lst))) (recur r [] [] (conj lst stack-lst))
               :else
               (recur r stack (conj stack-lst f) lst)
               )
         (if-not (empty? stack-lst)
             (conj lst stack-lst)
             lst))))

; 获取成对小括号
(defn get-small [lst]
    (if (= (first lst) "(")
        (letfn [(get-small-lst
                    ([lst] (get-small-lst lst [] []))
                    ([[f & r] stack stack-lst]
                     (if (some? f)
                         (cond (and (= f "(") (empty? stack)) (recur r (conj stack f) stack-lst)
                               (and (= f "(") (not (empty? stack))) (recur r (conj stack f) (conj stack-lst f))
                               (and (= f ")") (> (count stack) 1)) (recur r (pop stack) (conj stack-lst f))
                               (and (= f ")") (= (count stack) 1)) {:args-lst stack-lst :body-lst r}
                               :else
                               (recur r stack (conj stack-lst f))
                               )
                         (throw (Exception. "Smart 脚本错误！请仔细检查！"))))
                    )]
            (get-small-lst lst))))

; 获取成对大括号
(defn get-big [lst]
    (if (= (first lst) "{")
        (letfn [(get-big-lst
                    ([lst] (get-big-lst lst [] []))
                    ([[f & r] stack stack-lst]
                     (if (some? f)
                         (cond (and (= f "{") (empty? stack)) (recur r (conj stack f) stack-lst)
                               (and (= f "{") (not (empty? stack))) (recur r (conj stack f) (conj stack-lst f))
                               (and (= f "}") (> (count stack) 1)) (recur r (pop stack) (conj stack-lst f))
                               (and (= f "}") (= (count stack) 1)) {:big-lst stack-lst :rest-lst r}
                               :else
                               (recur r stack (conj stack-lst f))
                               )
                         (throw (Exception. "Smart 脚本错误！请仔细检查！"))))
                    )]
            (get-big-lst lst))))

; 获取 func 的名字 和 参数
(defn get-func-name [[f & r]]
    (if (and (my-lexical/is-eq? f "function") (= (second r) "("))
        (let [{args-lst :args-lst body-lst :body-lst} (get-small (rest r))]
            {:func-name (first r) :args-lst (filter #(not (= % ",")) args-lst) :body-lst body-lst})
        ))

(defn get-for-in-args [lst]
    (if (my-lexical/is-eq? (second lst) "in")
        {:tmp_val (my-select-plus/sql-to-ast [(first lst)]) :seq (my-select-plus/sql-to-ast (rest (rest lst)))}))

(defn lst-to-token [lst]
    (cond (and (my-lexical/is-eq? (first lst) "let") (= (second (rest lst)) "=")) (let [my-let-vs (my-select-plus/sql-to-ast (rest (rest (rest lst))))]
                                                                                      {:let-name (second lst) :let-vs my-let-vs})
          (my-lexical/is-eq? (first lst) "else") {:else-vs (my-select-plus/sql-to-ast (rest lst))}
          (my-lexical/is-eq? (first lst) "break") {:break-vs true}
          :else
          (let [pair-item (split-pair-item lst)]
              (cond (= (count pair-item) 2) {:pair (my-select-plus/sql-to-ast (first pair-item)) :pair-vs (my-select-plus/sql-to-ast (second pair-item))}
                    (= (count pair-item) 1) {:express (my-select-plus/sql-to-ast (first pair-item))}
                    :else
                    (throw (Exception. "switch 中的判断要成对出现！"))
                    ))
          ))

(defn body-segment
    ([lst] (body-segment lst [] []))
    ([[f & r] stack-lst lst]
     (if (some? f)
         (cond (and (empty? stack-lst) (my-lexical/is-eq? f "for") (= (first r) "(")) (let [{args-lst :args-lst body-lst :body-lst} (get-small r)]
                                                                                          (if-not (empty? body-lst)
                                                                                              (let [{big-lst :big-lst rest-lst :rest-lst} (get-big body-lst)]
                                                                                                  (recur rest-lst [] (conj lst {:expression "for" :args (get-for-in-args args-lst) :body (body-segment big-lst)})))))
               (and (empty? stack-lst) (my-lexical/is-eq? f "switch") (= (first r) "{")) (let [{big-lst :big-lst rest-lst :rest-lst} (get-big r)]
                                                                                             (recur rest-lst [] (conj lst {:expression "switch" :pair (body-segment big-lst)})))
               (= f ";") (recur r [] (conj lst (lst-to-token stack-lst)))
               :else
               (recur r (conj stack-lst f) lst)
               )
         lst)))

(defn get-ast [^String sql]
    (if-let [lst (my-lexical/to-back sql)]
        (let [m (get-func-name lst)]
            (assoc m :body-lst (body-segment (my-lexical/get-contain-lst (-> m :body-lst)))))))

(defn contains-context? [my-context token-name]
    (cond (contains? (-> my-context :input-params) token-name) true
          (contains? (-> my-context :let-params) token-name) true
          :else
          false
          ))

(defn get-my-it [my-context]
    (if-let [m (format "M-F-%s-I-%s-c-Y" (gensym "v") (gensym "Q"))]
        (if (contains-context? my-context m)
            (get-my-it my-context)
            m)))

(defn get-my-let [my-context]
    (if-let [m (format "c-F-%s-w-%s-c-Y" (gensym "W") (gensym "G"))]
        (if (contains-context? my-context m)
            (get-my-let my-context)
            m)))

(declare token-to-clj for-seq body-to-clj for-seq-func)


(defn token-to-clj [token my-context]
    ())

(defn for-seq [f r my-context]
    (let [tmp-val-name (-> f :args :tmp_val :item_name) seq-name (-> f :args :seq :item_name) my-it (get-my-it my-context)]
        (let [for-inner-clj (body-to-clj r (conj (-> my-context :let-params) tmp-val-name my-it))]
            (format "(cond (instance? Iterator %s) (loop [%s %s]\n
                                                       (if (.hasNext %s)\n
                                                           (let [%s (.next %s)]\n
                                                               %s\n
                                                               (recur %s)\n
                                                               )))\n
                        (vector? %s) (loop [[f & r] %s]\n
                                         (if (some? f)\n
                                             (do\n
                                                  %s\n
                                             (recur r))))\n
                        :else\n
                        (throw (Exception. \"for 循环只能处理列表或者是执行数据库的结果\"))\n
                        )"
                    seq-name my-it seq-name my-it tmp-val-name my-it for-inner-clj my-it seq-name seq-name for-inner-clj)
            )
        ))

(defn for-seq-func [f r my-context]
    (let [tmp-val-name (-> f :args :tmp_val :item_name) seq-name (get-my-let my-context) my-it (get-my-it my-context) func-clj (token-to-clj (-> f :args :seq) my-context)]
        (let [for-inner-clj (body-to-clj r (conj (-> my-context :let-params) tmp-val-name my-it))]
            (format "(let [%s %s]\n
                          (cond (instance? Iterator %s) (loop [%s %s]\n
                                                              (if (.hasNext %s)\n
                                                                  (let [%s (.next %s)]\n
                                                                       %s\n
                                                                       (recur %s)\n
                                                                       )))\n
                                (vector? %s) (loop [[f & r] %s]\n
                                                  (if (some? f)\n
                                                      (do\n
                                                           %s\n
                                                      (recur r))))\n
                                :else\n
                                (throw (Exception. \"for 循环只能处理列表或者是执行数据库的结果\"))\n
                                ))"
                    seq-name func-clj seq-name my-it seq-name my-it tmp-val-name my-it for-inner-clj my-it seq-name seq-name for-inner-clj))
        ))

; my-context 初始化的时候，记录了输入参数 和 定义的变量
; my-context: {:input-params #{} :let-params #{}}
(defn body-to-clj [[f & r] my-context]
    (if (some? f)
        (cond (contains? f :let-name) (format "(let [%s (MyVar. %s)]\n    (%s))" (-> f :let-name) (token-to-clj (-> f :let-vs) my-context) (body-to-clj r (conj my-context (-> f :let-name))))
              (and (contains? f :expression) (= (-> f :expression) "for")) (cond (and (contains? (-> f :args :tmp_val) :item_name) (contains? (-> f :args :seq) :item_name)) (for-seq f r my-context)
                                                                                 (and (contains? (-> f :args :tmp_val) :item_name) (contains? (-> f :args :seq) :func-name)) (for-seq-func f r my-context)
                                                                                 :else
                                                                                 (throw (Exception. "for 语句只能处理数据库结果或者是列表"))
                                                                                 )

              )))

(defn ast-to-clj [ast]
    (let [{func-name :func-name args-lst :args-lst body-lst :body-lst} ast]
        (format "(defn %s [%s]\n    (%s))" func-name (str/join " " args-lst))))









































