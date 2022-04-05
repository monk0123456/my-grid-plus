(ns org.gridgain.plus.dml.my-smart-sql
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [clojure.core.reducers :as r]
        [clojure.string :as str]
        [clojure.walk :as w])
    (:import (org.apache.ignite Ignite)
             (org.gridgain.smart MyVar)
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

(defn add-let-name [my-context let-name]
    (assoc my-context :let-params (conj (-> my-context :let-params) let-name)))

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
    ([lst my-context] (calculate lst [] [] my-context))
    ([[f & r] stack_number stack_symbol my-context]
     (if (some? f)
         (cond (contains? f :operation_symbol) (cond
                                                   ; 若符号栈为空，则符号直接压入符号栈
                                                   (= (count stack_symbol) 0) (recur r stack_number (conj stack_symbol f) my-context)
                                                   ; f 符号的优先级高于或等于符号栈栈顶的优先级，则直接入栈
                                                   (is-symbol-priority f (peek stack_symbol)) (recur r stack_number (conj stack_symbol f) my-context)
                                                   ; f 符号的优先级低于栈顶的优先级，则将符号栈顶，弹出参与计算后，在压入，数据栈
                                                   :else
                                                   (let [first_item (peek stack_number) second_item (peek (pop stack_number)) top_symbol (peek stack_symbol)]
                                                       ;(recur r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) my-context)
                                                       (cond (and (contains? (-> my-context :let-params) (-> first_item :item_name)) (contains? (-> my-context :let-params) (-> second_item :item_name)))
                                                             (recur r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (.getVar %s) (.getVar %s))" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) my-context)
                                                             (contains? (-> my-context :let-params) (-> first_item :item_name))
                                                             (recur r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s (.getVar %s) %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) my-context)
                                                             (contains? (-> my-context :let-params) (-> second_item :item_name))
                                                             (recur r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s (.getVar %s))" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) my-context)
                                                             :else
                                                             (recur r (conj (pop (pop stack_number)) {:table_alias "", :item_name (format "(%s %s %s)" (-> top_symbol :operation_symbol) (-> first_item :item_name) (-> second_item :item_name)), :item_type "", :java_item_type java.lang.Object, :const false}) (conj (pop stack_symbol) f) my-context)
                                                             ))
                                                   )
               (contains? f :parenthesis) (let [m (calculate (reverse (-> f :parenthesis)) my-context)]
                                              (recur r (conj stack_number {:table_alias "", :item_name m, :item_type "", :java_item_type java.lang.Object, :const false}) stack_symbol my-context))
               (contains? f :item_name) (recur r (conj stack_number f) stack_symbol my-context)
               ;(contains? f :func-name)
               :else
               (recur r (conj stack_number f) stack_symbol my-context)
               )
         (run-express stack_number stack_symbol my-context))))

(declare token-lst-to-clj token-lst-clj token-to-clj map-token-to-clj func-to-clj item-to-clj for-seq body-to-clj for-seq-func pair-to-clj pair-lst-to-clj match-to-clj
         my-args pk-rs-clj data-rs-clj insert-to-cache lst-to-cache)

; 判断 func
(defn is-func? [^Ignite ignite ^String func-name]
    (.containsKey (.cache ignite "my_func") func-name))

; 判断 scenes
(defn is-scenes? [^Ignite ignite ^Long group_id ^String scenes-name]
    (.containsKey (.cache ignite "my_scenes") (MyScenesCachePk. group_id scenes-name)))

; 调用方法这个至关重要
(defn func-to-clj [^Ignite ignite group_id m is-set my-context]
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
              (is-func? ignite func-name) (format "(my_func %s %s)" func-name (token-to-clj ignite group_id lst_ps is-set my-context))
              (is-scenes? ignite group_id func-name) (format "(my_scenes %s %s)" func-name (token-to-clj ignite group_id lst_ps is-set my-context))
              )))

(defn item-to-clj [m my-context]
    (if (contains? (-> my-context :let-params) (-> m :item_name))
        (format "(.getVar %s)" (-> m :item_name))))

(defn token-lst-to-clj [ignite group_id m is-set my-context]
    (if (and (>= (count m) 3) (contains? (nth m 1) :comparison_symbol) (= (-> (nth m 1) :comparison_symbol) "=")
             (contains? (nth m 0) :item_name))
        (format "(.setVar %s %s)" (-> (nth m 0) :item_name) (token-to-clj ignite group_id m false my-context))
        (token-lst-clj ignite group_id m is-set my-context)))

(defn token-lst-clj [ignite group_id m is-set my-context]
    (loop [index 0 sb (StringBuilder.)]
        (if (< index (count m))
            (recur (+ index 1) (doto sb (.append (token-to-clj ignite group_id (nth m index) is-set my-context))))
            (.toString sb))))

(defn token-to-clj [ignite group_id m is-set my-context]
    (if (some? m)
        (cond (instance? clojure.lang.LazySeq m) (token-lst-to-clj ignite group_id m is-set my-context)
              (map? m) (map-token-to-clj ignite group_id m is-set my-context))))

(defn map-token-to-clj [ignite group_id m is-set my-context]
    (if (some? m)
        (cond (and (contains? m :func-name) (contains? m :lst_ps)) (func-to-clj ignite group_id m is-set my-context)
              (contains? m :and_or_symbol) (get m :and_or_symbol)
              (contains? m :operation) (token-lst-to-clj ignite group_id (get m :operation) is-set my-context)
              (contains? m :comparison_symbol) (get m :comparison_symbol)
              (contains? m :operation_symbol) (get m :operation_symbol)
              (contains? m :comma_symbol) (get m :comma_symbol)
              (contains? m :item_name) (item-to-clj m my-context)
              (contains? m :parenthesis) (token-to-clj ignite group_id (get m :parenthesis) is-set my-context)
              )))

(defn for-seq [ignite group_id f r my-context]
    (let [tmp-val-name (-> f :args :tmp_val :item_name) seq-name (-> f :args :seq :item_name) my-it (get-my-it my-context)]
        (let [for-inner-clj (body-to-clj ignite group_id r (conj (-> my-context :let-params) tmp-val-name my-it))]
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

(defn for-seq-func [ignite group_id f r my-context]
    (let [tmp-val-name (-> f :args :tmp_val :item_name) seq-name (get-my-let my-context) my-it (get-my-it my-context) func-clj (token-to-clj ignite group_id (-> f :args :seq) false my-context)]
        (let [for-inner-clj (body-to-clj ignite group_id r (conj (-> my-context :let-params) tmp-val-name my-it))]
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
(defn pair-to-clj [ignite group_id pair my-context]
    (cond (contains? pair :pair) (if (= (count (-> pair :pair-vs)) 1)
                                     (format "(%s) (%s)" (body-to-clj ignite group_id [(-> pair :pair)] my-context) (body-to-clj ignite group_id (-> pair :pair-vs) my-context))
                                     (format "(%s) (do %s)" (body-to-clj ignite group_id [(-> pair :pair)] my-context) (body-to-clj ignite group_id (-> pair :pair-vs) my-context)))
          (contains? pair :else-vs) (if (= (count (-> pair :else-vs)) 1)
                                        (format ":else (%s)" (body-to-clj ignite group_id (-> pair :else-vs) my-context))
                                        (format ":else (do %s)" (body-to-clj ignite group_id (-> pair :else-vs) my-context)))
          )
    )

(defn pair-lst-to-clj [ignite group_id [f & r] lst my-context]
    (if (some? f)
        (recur ignite group_id r (conj lst (pair-to-clj ignite group_id f my-context)) my-context)
        (str/join " " lst)))

(defn match-to-clj [ignite group_id lst-pairs r my-context]
    (if-not (empty? lst-pairs)
        (let [last-line (body-to-clj ignite group_id r my-context)]
            (if-not (Strings/isNullOrEmpty last-line)
                (format "(cond %s) %s" (pair-lst-to-clj ignite group_id lst-pairs [] my-context) last-line)
                (format "(cond %s)" (pair-lst-to-clj ignite group_id lst-pairs [] my-context))))
        ))

; trans
(defn my-args
    ([agrs] (my-args agrs []))
    ([[f & r] lst]
     (if (some? f)
         (cond (string? f) (recur r (conj lst (format "'%s'" f)))
               :else
               (recur r (conj lst f))
               )
         lst)))

(defn pk-rs-clj [ignite group_id my-context pk-rs]
    (if (= (count pk-rs) 1)
        (token-to-clj ignite group_id (my-select-plus/sql-to-ast (-> (first pk-rs) :item_value)) false my-context)
        (loop [[f & r] pk-rs lst-line []]
            (if (some? f)
                (recur r (conj lst-line (format "(MyKeyValue. %s %s)" (format "%s_pk" (-> f :column_name)) (token-to-clj ignite group_id (my-select-plus/sql-to-ast (-> f :item_value)) false my-context))))
                (format "[%s]" (str/join " " lst-line))))))

(defn data-rs-clj [ignite group_id my-context data-rs]
    (loop [[f & r] data-rs lst-line []]
        (if (some? f)
            (recur r (conj lst-line (format "(MyKeyValue. %s %s)" (-> f :column_name) (token-to-clj ignite group_id (my-select-plus/sql-to-ast (-> f :item_value)) false my-context))))
            (format "[%s]" (str/join " " lst-line)))))

(defn insert-to-cache [ignite group_id my-context-line sql-lst]
    (let [insert_obj (my-insert/get_insert_obj sql-lst) my-context (eval (read-string my-context-line))]
        (let [pk_with_data (my-insert/get_pk_data_with_data (my-insert/get_pk_data ignite (-> insert_obj :table_name)) insert_obj)]
            (let [{pk-rs :pk_rs data-rs :data_rs} (my-insert/insert_obj_to_db ignite 0 (-> insert_obj :table_name) pk_with_data)]
                (let [line (format "(MyLogCache. %s %s %s (SqlType/INSERT))" (-> insert_obj :table_name) (pk-rs-clj ignite group_id my-context pk-rs) (data-rs-clj ignite group_id my-context data-rs))]
                    (eval (read-string line)))))))

(defn lst-to-cache [ignite group_id my-context lst]
    (loop [[f & r] lst lst-rs []]
        (if (some? f)
            (let [[sql args] f]
                (let [sql-lst (my-lexical/to-back (apply format sql (my-args args)))]
                    (cond (my-lexical/is-eq? (first sql-lst) "insert") (recur r (conj lst-rs (insert-to-cache ignite group_id my-context sql-lst)))
                          (my-lexical/is-eq? (first sql-lst) "update") ()
                          (my-lexical/is-eq? (first sql-lst) "delete") ()
                          )))
            lst-rs)))

; my-context 初始化的时候，记录了输入参数 和 定义的变量
; my-context: {:input-params #{} :let-params #{}}
(defn body-to-clj
    [ignite group_id [f & r] my-context]
    (if (some? f)
        (cond (contains? f :let-name) (format "(let [%s (MyVar. %s)]\n    %s)" (-> f :let-name) (token-to-clj ignite group_id (-> f :let-vs) false my-context) (body-to-clj ignite group_id r (add-let-name my-context (-> f :let-name))))
              (and (contains? f :expression) (= (-> f :expression) "for")) (cond (and (contains? (-> f :args :tmp_val) :item_name) (contains? (-> f :args :seq) :item_name)) (for-seq ignite group_id f r my-context)
                                                                                 (and (contains? (-> f :args :tmp_val) :item_name) (contains? (-> f :args :seq) :func-name)) (for-seq-func ignite group_id f r my-context)
                                                                                 :else
                                                                                 (throw (Exception. "for 语句只能处理数据库结果或者是列表"))
                                                                                 )
              (and (contains? f :expression) (= (-> f :expression) "match")) (match-to-clj ignite group_id (-> f :pairs) r my-context)
              (contains? f :express) (cond (contains? (-> f :express) :item_name) (-> f :express :item_name)
                                           (contains? (-> f :express) :operation) (calculate (reverse (-> f :express :operation)) my-context)
                                           (contains? (-> f :express) :parenthesis) (calculate (reverse (-> f :express :parenthesis)) my-context)
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

(defn ast-to-clj [ignite group_id ast]
    (let [{func-name :func-name args-lst :args-lst body-lst :body-lst} ast my-context {:input-params #{} :let-params #{}}]
        (let [func-context (assoc my-context :input-params (conj (-> my-context :input-params) args-lst))]
            (format "(defn %s [%s]\n    %s)" func-name (str/join " " args-lst) (body-to-clj ignite group_id body-lst func-context)))
        ))

(defn smart-to-clj [^Ignite ignite ^Long group_id ^String smart-sql]
    (ast-to-clj ignite group_id (first (get-ast smart-sql))))









































