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

(defn my-re-match
    ([lst] (my-re-match lst []))
    ([[f & r] lst]
     (if (some? f)
         (cond (contains? f :pair) (recur r (conj lst f))
               (contains? f :else-vs) (recur r (conj lst f))
               :else (let [my-last-item (peek lst)]
                         (if (nil? my-last-item)
                             (throw (Exception. "match 语句块语法错误！"))
                             (cond (contains? my-last-item :pair) (cond (map? (-> my-last-item :pair-vs)) (let [new-peek (assoc my-last-item :pair-vs [(-> my-last-item :pair-vs) f])]
                                                                                                              (recur r (conj (pop lst) new-peek)))
                                                                        (or (vector? (-> my-last-item :pair-vs)) (seq? (-> my-last-item :pair-vs))) (let [new-peek (assoc my-last-item :pair-vs (conj (-> my-last-item :pair-vs) f))]
                                                                                                                                                        (recur r (conj (pop lst) new-peek)))
                                                                        :else
                                                                        (throw (Exception. "match 语句块语法错误！"))
                                                                        )
                                   (contains? my-last-item :else-vs) (cond (map? (-> my-last-item :else-vs)) (let [new-peek (assoc my-last-item :else-vs [(-> my-last-item :else-vs) f])]
                                                                                                                 (recur r (conj (pop lst) new-peek)))
                                                                           (or (vector? (-> my-last-item :else-vs)) (seq? (-> my-last-item :else-vs))) (let [new-peek (assoc my-last-item :else-vs (conj (-> my-last-item :else-vs) f))]
                                                                                                                                                           (recur r (conj (pop lst) new-peek)))
                                                                           :else
                                                                           (throw (Exception. "match 语句块语法错误！"))
                                                                           )
                                   :else
                                   (throw (Exception. "match 语句块语法错误！"))
                                   )))
               )
         lst)))

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
                    (throw (Exception. "match 中的判断要成对出现！"))
                    ))
          ))

(declare body-segment get-ast-lst get-ast)

(defn body-segment
    ([lst] (body-segment lst [] []))
    ([[f & r] stack-lst lst]
     (if (some? f)
         (cond (and (empty? stack-lst) (my-lexical/is-eq? f "for") (= (first r) "(")) (let [{args-lst :args-lst body-lst :body-lst} (get-small r)]
                                                                                          (if-not (empty? body-lst)
                                                                                              (let [{big-lst :big-lst rest-lst :rest-lst} (get-big body-lst)]
                                                                                                  (recur rest-lst [] (conj lst {:expression "for" :args (get-for-in-args args-lst) :body (body-segment big-lst)})))))
               (and (empty? stack-lst) (my-lexical/is-eq? f "match") (= (first r) "{")) (let [{big-lst :big-lst rest-lst :rest-lst} (get-big r)]
                                                                                             (recur rest-lst [] (conj lst {:expression "match" :pairs (my-re-match (body-segment big-lst))})))
               (and (empty? stack-lst) (my-lexical/is-eq? f "innerFunction") (= (first r) "{")) (let [{big-lst :big-lst rest-lst :rest-lst} (get-big r)]
                                                                                                    (recur rest-lst [] (conj lst {:functions (get-ast-lst big-lst)})))
               (= f ";") (recur r [] (conj lst (lst-to-token stack-lst)))
               :else
               (recur r (conj stack-lst f) lst)
               )
         lst)))

(defn get-ast-lst [lst]
    (let [{func-name :func-name  args-lst :args-lst body-lst :body-lst} (get-func-name lst)]
        (let [{big-lst :big-lst rest-lst :rest-lst} (get-big body-lst)]
            (if-not (nil? rest-lst)
                (concat [{:func-name func-name :args-lst args-lst :body-lst (body-segment big-lst)}] (get-ast-lst rest-lst))
                (if (nil? func-name)
                    (throw (Exception. "smart sql 程序有误！"))
                    [{:func-name func-name :args-lst args-lst :body-lst (body-segment big-lst)}])
                ))))

(defn get-ast [^String sql]
    (if-let [lst (my-lexical/to-back sql)]
        (get-ast-lst lst)
        ))

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

; 判断符号优先级
; f symbol 的优先级大于等于 s 返回 true 否则返回 false
(defn is-symbol-priority [f s]
    (cond (or (= (-> f :operation_symbol) "*") (= (-> f :operation_symbol) "/")) true
          (and (or (= (-> f :operation_symbol) "+") (= (-> f :operation_symbol) "-")) (or (= (-> s :operation_symbol) "+") (= (-> s :operation_symbol) "-"))) true
          :else
          false))

(defn run-express [stack_number stack_symbo]
    (if (some? (peek stack_symbo))
        (let [first_item (peek stack_number) second_item (peek (pop stack_number)) top_symbol (peek stack_symbo)]
            (recur (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (pop stack_symbo)))
        (-> (first stack_number) :item_name)))

(defn calculate
    ([lst] (calculate lst [] []))
    ([[f & r] stack_number stack_symbol]
     (if (some? f)
         (cond (contains? f :operation_symbol) (cond
                                                   ; 若符号栈为空，则符号直接压入符号栈
                                                   (= (count stack_symbol) 0) (recur r stack_number (conj stack_symbol f))
                                                   ; f 符号的优先级高于或等于符号栈栈顶的优先级，则直接入栈
                                                   (is-symbol-priority f (peek stack_symbol)) (recur r stack_number (conj stack_symbol f))
                                                   ; f 符号的优先级低于栈顶的优先级，则将符号栈顶，弹出参与计算后，在压入，数据栈
                                                   :else
                                                   (let [first_item (peek stack_number) second_item (peek (pop stack_number)) top_symbol (peek stack_symbol)]
                                                       (recur r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f)))
                                                   )
               (contains? f :parenthesis) (let [m (calculate (reverse (-> f :parenthesis)))]
                                              (recur r (conj stack_number {:table_alias "", :item_name m, :item_type "", :java_item_type java.lang.Object, :const false}) stack_symbol))
               (contains? f :item_name) (recur r (conj stack_number f) stack_symbol)
               ;(contains? f :func-name)
               :else
               (recur r (conj stack_number f) stack_symbol)
               )
         (run-express stack_number stack_symbol))))

(declare token-to-clj for-seq body-to-clj for-seq-func pair-to-clj pair-lst-to-clj match-to-clj)


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
                        (vector? %s) (loop [[%s & r] %s]\n
                                         (if (some? %s)\n
                                             (do\n
                                                  %s\n
                                             (recur r))))\n
                        :else\n
                        (throw (Exception. \"for 循环只能处理列表或者是执行数据库的结果\"))\n
                        )"
                    seq-name my-it seq-name
                    my-it
                    tmp-val-name my-it
                    for-inner-clj
                    my-it
                    seq-name tmp-val-name seq-name
                    tmp-val-name
                    for-inner-clj)
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
                                (vector? %s) (loop [[%s & r] %s]\n
                                                  (if (some? %s)\n
                                                      (do\n
                                                           %s\n
                                                      (recur r))))\n
                                :else\n
                                (throw (Exception. \"for 循环只能处理列表或者是执行数据库的结果\"))\n
                                ))"
                    seq-name func-clj
                    seq-name my-it seq-name
                    my-it
                    tmp-val-name my-it
                    for-inner-clj
                    my-it
                    seq-name tmp-val-name seq-name
                    tmp-val-name
                    for-inner-clj))
        ))

; 处理 match
; 第一个参数：(-> f :pairs)
(defn pair-to-clj [pair my-context]
    (cond (contains? pair :pair) (if (= (count (-> pair :pair-vs)) 1)
                                     (format "(%s) (%s)" (body-to-clj [(-> pair :pair)] my-context) (body-to-clj (-> pair :pair-vs) my-context))
                                     (format "(%s) (do %s)" (body-to-clj [(-> pair :pair)] my-context) (body-to-clj (-> pair :pair-vs) my-context)))
          (contains? pair :else-vs) (if (= (count (-> pair :else-vs)) 1)
                                        (format ":else (%s)" (body-to-clj (-> pair :else-vs) my-context))
                                        (format ":else (do %s)" (body-to-clj (-> pair :else-vs) my-context)))
          )
    )

(defn pair-lst-to-clj [[f & r] lst my-context]
    (if (some? f)
        (recur r (conj lst (pair-to-clj f my-context)) my-context)
        (str/join " " lst)))

(defn match-to-clj [lst-pairs r my-context]
    (if-not (empty? lst-pairs)
        (let [last-line (body-to-clj r my-context)]
            (if-not (Strings/isNullOrEmpty last-line)
                (format "(cond %s) %s" (pair-lst-to-clj lst-pairs [] my-context) last-line)
                (format "(cond %s)" (pair-lst-to-clj lst-pairs [] my-context))))
        ))

; my-context 初始化的时候，记录了输入参数 和 定义的变量
; my-context: {:input-params #{} :let-params #{}}
(defn body-to-clj
    [[f & r] my-context]
    (if (some? f)
        (cond (contains? f :let-name) (format "(let [%s (MyVar. %s)]\n    (%s))" (-> f :let-name) (token-to-clj (-> f :let-vs) my-context) (body-to-clj r (conj my-context (-> f :let-name))))
              (and (contains? f :expression) (= (-> f :expression) "for")) (cond (and (contains? (-> f :args :tmp_val) :item_name) (contains? (-> f :args :seq) :item_name)) (for-seq f r my-context)
                                                                                 (and (contains? (-> f :args :tmp_val) :item_name) (contains? (-> f :args :seq) :func-name)) (for-seq-func f r my-context)
                                                                                 :else
                                                                                 (throw (Exception. "for 语句只能处理数据库结果或者是列表"))
                                                                                 )
              (and (contains? f :expression) (= (-> f :expression) "match")) (match-to-clj (-> f :pairs) r my-context)
              (contains? f :express) (cond (contains? (-> f :express) :item_name) (-> f :express :item_name)
                                           (contains? (-> f :express) :operation) (calculate (reverse (-> f :operation)))
                                           (contains? (-> f :express) :parenthesis) (calculate (reverse (-> f :parenthesis)))
                                           (contains? (-> f :express) :func-name) (calculate (reverse (-> f :operation)))
                                           )

              )))

;(defn body-to-clj [[f & r] my-context]
;    (if (some? f)
;        (cond (contains? f :let-name) (format "(let [%s (MyVar. %s)]\n    (%s))" (-> f :let-name) (token-to-clj (-> f :let-vs) my-context) (body-to-clj r (conj my-context (-> f :let-name))))
;              (and (contains? f :expression) (= (-> f :expression) "for")) (cond (and (contains? (-> f :args :tmp_val) :item_name) (contains? (-> f :args :seq) :item_name)) (for-seq f r my-context)
;                                                                                 (and (contains? (-> f :args :tmp_val) :item_name) (contains? (-> f :args :seq) :func-name)) (for-seq-func f r my-context)
;                                                                                 :else
;                                                                                 (throw (Exception. "for 语句只能处理数据库结果或者是列表"))
;                                                                                 )
;              (and (contains? f :expression) (= (-> f :expression) "match")) (match-to-clj (-> f :pairs) my-context)
;              (contains? f :express) (cond (contains? (-> f :express) :item_name) (-> f :express :item_name)
;                                           (contains? (-> f :express) :operation)
;                                           )
;
;              )))

(defn ast-to-clj [ast]
    (let [{func-name :func-name args-lst :args-lst body-lst :body-lst} ast]
        (format "(defn %s [%s]\n    (%s))" func-name (str/join " " args-lst))))









































