(ns org.gridgain.plus.dml.my-select
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.context.my-context :as my-context]
        [clojure.core.reducers :as r]
        [clojure.string :as str]
        [clojure.walk :as w])
    (:import (org.apache.ignite Ignite IgniteCache)
             (com.google.common.base Strings)
             (org.tools MyConvertUtil MyDbUtil KvSql)
             (cn.plus.model.db MyScenesCache MyScenesParams)
             (org.apache.ignite.configuration CacheConfiguration)
             (org.apache.ignite.cache CacheMode CacheAtomicityMode)
             (org.apache.ignite.cache.query FieldsQueryCursor SqlFieldsQuery)
             (java.util ArrayList Date Iterator)
             (java.sql Timestamp)
             (java.math BigDecimal)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MySelect
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        :methods [^:static [get_plus_sql [org.apache.ignite.Ignite Long String] String]
                  ^:static [getSqlToAst [org.apache.ignite.Ignite String String] clojure.lang.LazySeq]
                  ^:static [putAstCache [org.apache.ignite.Ignite String String String] void]]
        ))

; 函数的参数
(defrecord funcPs [index ps_name])

; funcObj 存到 my_meta_cache_all_func 中的对象
; 函数参数， 函数 ast
(defrecord funcObj [lst_func_ps ast])

; 函数的参数
(defrecord scenesPs [index ps_name])

; scenesObj 存到 my_meta_cache_all_scenes 中的对象
; 函数参数， 函数 ast
(defrecord scenesObj [lst_func_ps ast])

; 输入 ignite 和 group-id
(declare element-item get-item-alias get-token-line get-where-line get-where-item-line arithmetic-fn my-comma-fn
         func-lst-ps func-fn operation is-true? is-sql-obj? parenthesis get-token get-item-rv get-item-lst pre-query-lst get-query-items get-table table-comma table-join get-table-items
         get-order-by get-limit sql-to-ast-single to-ast sql-to-ast
         find-table-func map-replace re-func plus-func func_scenes_invoke
         get-func get-scenes re-func-ast get-sql-ast
         re-func-obj re-func-obj-map re-func-obj-map-sub
         query_authority query_map get_select_view get_map_table_items
         replace_alias map_replace_alias get_where get_query_table get_query_table_map
         func-to-line table-to-line select-to-sql token-to-sql map-token-to-sql
         get_my_ast
         param_lst param_lst_index get_sql_args get_sql_args_index get_plus_sql_func)

; 生成或获取 cache
;(defn get-cache [ignite cacheName]
;    (let [cfg (CacheConfiguration. cacheName)]
;        (.setCacheMode cfg (CacheMode/REPLICATED))
;        (.setAtomicityMode cfg (CacheAtomicityMode/TRANSACTIONAL))
;        (.setReadFromBackup cfg true)
;        (.getOrCreateCache ignite cfg)
;        ))

(defn get-cache [^Ignite ignite ^String cacheName]
    (doto ignite (.getOrCreateCache (doto (CacheConfiguration. cacheName)
                                        (.setCacheMode (CacheMode/REPLICATED))
                                        (.setAtomicityMode (CacheAtomicityMode/TRANSACTIONAL))
                                        (.setReadFromBackup true)
                                        ))))

; 保存到 cache
(defn -putAstCache [^Ignite ignite ^String cacheName ^String key ^String line]
    (when-let [cache (get-cache ignite cacheName)]
        (.put cache key (sql-to-ast (my-lexical/to-back line)))))

; 获取保存到 cache 中的 ast
; 输入 sql 获取 ast
(defn -getSqlToAst [^Ignite ignite ^String cacheName ^String key]
    (when-let [cache (.cache ignite cacheName)]
        (.get cache key)))

; 获取 my_meta_cache_all_func 的 funcObj
; key 是 func name
; value 是 funcObj
(defn get-func [^Ignite ignite ^String key]
    (-getSqlToAst ignite "my_meta_cache_all_func" key))

; 获取 my_meta_cache_all_scenes 的 scenesObj
(defn get-scenes [^Ignite ignite ^String key]
    (-getSqlToAst ignite "my_meta_cache_all_scenes" key))

; 获取 data_set 的名字和对应的表
(defn get_data_set_name [^Ignite ignite ^Long group_id]
    (when-let [m (first (.getAll (.query (.cache ignite "my_users_group") (.setArgs (SqlFieldsQuery. "select m.dataset_name from my_users_group as g JOIN my_dataset as m ON m.id = g.data_set_id where g.id = ?") (to-array [group_id])))))]
        (first m)))

; 判断 table 是否在数据集中
(defn get_data_set_tables [ignite dataset_name]
    (.getAll (.query (.cache ignite "my_dataset_table") (.setArgs (SqlFieldsQuery. "select distinct m.table_name from my_dataset_table as m JOIN my_dataset AS d ON m.dataset_id = d.id where d.dataset_name = ?") (to-array [dataset_name])))))

(defn is-select?
    ([lst] (if (some? lst)
               (if (and (= (first lst) "(") (= (last lst) ")"))
                   (let [m (is-select? (rest lst) [])]
                       (if (and (some? m) (> (count m) 0))
                           (when-let [sql_objs (sql-to-ast m)]
                               (if (> (count sql_objs) 0) true)))))))
    ([[f & r] my-lst]
     (if (empty? r) my-lst (recur r (concat my-lst [f])))))

(defn get-select-line
    ([lst] (if (some? lst) (get-select-line (rest lst) [])))
    ([[f & r] my-lst]
     (if (empty? r) my-lst (recur r (concat my-lst [f])))))

; 1、常数的处理
(defn element-item [line]
    (if (some? line)
        (cond
            (contains? #{"+" "-" "*" "/"} line) {:operation_symbol line}
            (contains? #{"(" ")" "[" "]"} line) {:parenthesis_symbol line}
            (contains? #{">=" "<=" "<>" ">" "<" "=" "!="} line) {:comparison_symbol line}
            (contains? #{"and" "or" "between"} (str/lower-case line)) {:and_or_symbol (str/lower-case line)}
            (contains? #{"in" "not in"} (str/lower-case line)) {:in_symbol (str/lower-case line)}
            (contains? #{"exists" "not exists"} (str/lower-case line)) {:exists_symbol (str/lower-case line)}
            (contains? #{","} line) {:comma_symbol line}
            (some? (re-find #"^(?i)\d+$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type Integer :const true}
            (some? (re-find #"^(?i)\d+\.\d$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type BigDecimal :const true}
            ;(some? (re-find #"^(?i)\"\w*\"$|^(?i)'\w*'$|^(?i)\"\W*\"$|^(?i)'\W*'$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type String :const true}
            (some? (re-find #"^\'[\S\s]+\'$|^\"[\S\s]+\"$|^\'\'$|^\"\"$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type String :const true}
            (some? (re-find #"^(?i)\d+D$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type Double :const true}
            (some? (re-find #"^(?i)\d+L$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type Long :const true}
            (some? (re-find #"^(?i)\d+F$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type Float :const true}
            (some? (re-find #"^(?i)true$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type Boolean :const true}
            (some? (re-find #"^(?i)false$" line)) {:table_alias "" :item_name line :item_type "" :java_item_type Boolean :const true}
            :else
            {:table_alias "" :item_name line :item_type "" :java_item_type nil :const false}
            )))

; m.name 生成 {:table_alias "" :item_name line :item_type "" :java_item_type Integer :const false}
; line = "m.name"
(defn get-item-alias [line]
    (if-let [m (str/split line #"\.")]
        (if (and (some? m) (= (count m) 2))
            {:item_name (nth m 1) :table_alias (nth m 0) :const false})))

; 输入 line 获取 token
(defn get-token-line [line]
    (if (and (some? line) (= (empty? line) false))
        (cond
            ; 如果是 m.name 这种形式
            (some? (re-find #"^(?i)\w+\.\w+$" line)) (if (some? (re-find #"^(?i)\d+\.\d+$|^(?i)\d+\.\d+[DFL]$" line))
                                                         (element-item line)
                                                         (get-item-alias line))
            :else
            (element-item line)
            )))

; 判断是 () 的表达式
(defn is-operate-fn?
    ([lst] (if (and (= (first lst) "(") (= (last lst) ")")) (let [m (is-operate-fn? lst [] [] [])]
                                                                (if (and (some? m) (= (count m) 1)) (take (- (count (nth m 0)) 2) (rest (nth m 0)))))))
    ([[f & rs] stack lst result-lst]
     (if (some? f)
         (cond
             (= f "(") (recur rs (conj stack f) (conj lst f) result-lst)
             (= f ")") (if (= (count stack) 1) (recur rs (pop stack) [] (concat result-lst [(conj lst f)])) (if (> (count stack) 0) (recur rs (pop stack) (conj lst f) result-lst) (recur rs [] (conj lst f) result-lst)))
             :else
             (recur rs stack (conj lst f) result-lst)
             ) (if (> (count lst) 0) (concat result-lst [lst]) result-lst))))

; 按照 and or 切分字符串
(defn get-where-line
    ([lst] (get-where-line lst [] [] []))
    ([[f & rs] stack lst result-lst]
     (if (some? f)
         (cond (and (contains? #{"and" "or" "between"} (str/lower-case f)) (= (count stack) 0)) (if (> (count lst) 0) (recur rs stack [] (concat result-lst [lst f])) (recur rs stack [] result-lst))
               (= f "(") (recur rs (conj stack f) (conj lst f) result-lst)
               (= f ")") (recur rs (pop stack) (conj lst f) result-lst)
               :else
               (recur rs stack (conj lst f) result-lst)
               )
         (if (> (count lst) 0) (concat result-lst [lst]) result-lst))))

; 处理多个
(defn get-where-item-line
    ([lst] (get-where-item-line lst [] [] []))
    ([[f & rs] stack lst result-lst]
     (if (some? f)
         (cond (and (contains? #{">=" "<=" "<>" ">" "<" "=" "!=" "in" "exists"} (str/lower-case f)) (= (count stack) 0)) (if (> (count lst) 0) (recur rs stack [] (concat result-lst [lst f])) (recur rs stack [] result-lst))
               (and (my-lexical/is-eq? "not" f) (my-lexical/is-eq? "in" (first rs)) (= (count stack) 0)) (if (> (count lst) 0) (recur (rest rs) stack [] (concat result-lst [lst "not in"])) (recur rs stack [] result-lst))
               (and (my-lexical/is-eq? "not" f) (my-lexical/is-eq? "exists" (first rs)) (= (count stack) 0)) (if (> (count lst) 0) (recur (rest rs) stack [] (concat result-lst [lst "not exists"])) (recur rs stack [] result-lst))
               (and (my-lexical/is-eq? "is" f) (my-lexical/is-eq? "not" (first rs)) (= (count stack) 0)) (if (> (count lst) 0) (recur (rest rs) stack [] (concat result-lst [lst "is not"])) (recur rs stack [] result-lst))
               (and (my-lexical/is-eq? "is" f) (not (my-lexical/is-eq? "not" (first rs))) (= (count stack) 0)) (if (> (count lst) 0) (recur (rest rs) stack [] (concat result-lst [lst "is"])) (recur rs stack [] result-lst))
               (= f "(") (recur rs (conj stack f) (conj lst f) result-lst)
               (= f ")") (recur rs (pop stack) (conj lst f) result-lst)
               :else
               (recur rs stack (conj lst f) result-lst)
               )
         (if (> (count lst) 0) (concat result-lst [lst]) result-lst))))

; 处理算法运算符 #{"+" "-" "*" "/"}
(defn arithmetic-fn
    ([lst] (arithmetic-fn lst [] [] []))
    ([[f & rs] stack lst result-lst]
     (if (some? f)
         (cond (and (contains? #{"+" "-" "*" "/"} f) (= (count stack) 0)) (if (> (count lst) 0) (recur rs stack [] (concat result-lst [lst f])) (recur rs stack [] result-lst))
               (= f "(") (recur rs (conj stack f) (conj lst f) result-lst)
               (= f ")") (recur rs (pop stack) (conj lst f) result-lst)
               :else
               (recur rs stack (conj lst f) result-lst)
               )
         (if (> (count lst) 0) (concat result-lst [lst]) result-lst))))

; 在函数内部处理 ,
(defn my-comma-fn
    ([lst] (my-comma-fn lst [] [] []))
    ([[f & rs] stack lst result-lst]
     (if (some? f)
         (cond (and (= f ",") (= (count stack) 0)) (if (> (count lst) 0) (recur rs stack [] (concat result-lst [lst f])) (recur rs stack [] result-lst))
               (= f "(") (recur rs (conj stack f) (conj lst f) result-lst)
               (= f ")") (recur rs (pop stack) (conj lst f) result-lst)
               :else
               (recur rs stack (conj lst f) result-lst)
               )
         (if (> (count lst) 0) (concat result-lst [lst]) result-lst))))

; 处理 func lst_ps
(defn func-lst-ps [lst]
    (when-let [m (my-comma-fn lst)]
        (map get-token m)))

; 2、处理函数
(defn func-fn [[f & rs]]
    (if (some? f) (let [m (is-operate-fn? rs)]
                      (if (some? m) {:func-name f :lst_ps (func-lst-ps m)}))))

; 3、处理四则运算
; 例如：a + b * (c - d)
(defn operation [lst]
    (when-let [m (arithmetic-fn lst)]
        (if (> (count m) 1) {:operation (map get-token m)})))

(defn is-true? [f p]
    (and (some? (f p)) (> (count (f p)) 1)))

(defn is-sql-obj? [f m]
    (let [sql-obj (f m)]
        (if (and (some? sql-obj) (instance? clojure.lang.LazySeq sql-obj) (map? (first sql-obj)) (contains? (first sql-obj) :sql_obj))
            (let [{sqlObj :sql_obj} (first sql-obj)]
                (if (and (> (count (-> sqlObj :query-items)) 0) (> (count (-> sqlObj :table-items)) 0)) true false)) false)))

(defn eliminate-parentheses [lst]
                       (if (and (= (first lst) "(") (= (last lst) ")"))
                           (let [m (get-token (my-lexical/get-contain-lst lst))]
                               (if-not (nil? m)
                                   m
                                   (eliminate-parentheses (my-lexical/get-contain-lst lst))))))

; 4、对括号的处理
; 例如：(a + b * c)
(defn parenthesis
    ([lst] (if (instance? clojure.lang.LazySeq lst) (parenthesis lst (memoize my-comma-fn) (memoize arithmetic-fn) (memoize get-where-line)
                                                                 (memoize get-where-item-line) (memoize sql-to-ast))
                                                    (parenthesis (my-lexical/to-lazy lst) (memoize my-comma-fn) (memoize arithmetic-fn) (memoize get-where-line)
                                                                 (memoize get-where-item-line) (memoize sql-to-ast))))
    ([lst my-comma-fn-m arithmetic-fn-m get-where-line-m get-where-item-line-m sql-to-ast-m]
     (when-let [m (is-operate-fn? lst)]
         (cond
             (is-sql-obj? sql-to-ast-m m)  {:parenthesis (sql-to-ast-m m)}
             (is-true? get-where-line-m m) {:parenthesis (map get-token (get-where-line-m m))}
             (is-true? get-where-item-line-m m) {:parenthesis (map get-token (get-where-item-line-m m))}
             (is-true? my-comma-fn-m m) {:parenthesis (map get-token (my-comma-fn-m m))}
             (is-true? arithmetic-fn-m m) {:parenthesis (map get-token (arithmetic-fn-m m))}
             (is-true? get-token m) (get-token m)
             :else
             (eliminate-parentheses m)
             ))))

; 获取 token ast
(defn get-token
    ([lst] (get-token lst (memoize get-token-line) (memoize func-fn) (memoize operation) (memoize parenthesis)
                       (memoize get-where-line) (memoize get-where-item-line)))
    ([lst get-token-line-m func-fn-m operation-m parenthesis-m get-where-line-m
      get-where-item-line-m]
     (if (some? lst)
         (if (instance? String lst)
             (get-token-line-m lst)
             (cond (and (= (count lst) 1) (instance? String (first lst))) (get-token-line-m (first lst))
                   (is-true? get-where-line-m lst) (map get-token (get-where-line-m lst))
                   (is-true? get-where-item-line-m lst) (map get-token (get-where-item-line-m lst))
                   (some? (func-fn-m lst)) (when-let [m (func-fn-m lst)] m)
                   (some? (operation-m lst)) (when-let [m (operation-m lst)] m)
                   (some? (parenthesis-m lst)) (when-let [m (parenthesis-m lst)] m)
                   (and (my-lexical/is-eq? (first lst) "not") (my-lexical/is-eq? (second lst) "exists") (some? (parenthesis-m (rest (rest lst))))) (when-let [m (parenthesis-m (rest (rest lst)))] {:exists "not exists" :select_sql m})
                   (and (my-lexical/is-eq? (first lst) "exists") (some? (parenthesis-m (rest lst)))) (when-let [m (parenthesis-m (rest lst))] {:exists "exists" :select_sql m})
                   )))))

(defn get-item-rv [[f & rs]]
    (cond
        (= (count (concat [f] rs)) 1) {:item-lst (concat [f]) :alias nil}
        (my-lexical/is-eq? (first rs) "as") {:item-lst (reverse (rest rs)) :alias f}
        (and (= (count rs) 1) (= (first rs) (last rs))) {:item-lst (first rs) :alias f}
        (and (my-lexical/is-word? f) (= (first rs) ")")) {:item-lst (reverse rs) :alias f}
        (and (my-lexical/is-word? f) (my-lexical/is-word? (first rs)) (not (my-lexical/is-word? (second rs))) (> (count rs) 2)) {:item-lst (reverse rs) :alias f}
        :else
        {:item-lst (reverse (concat [f] rs)) :alias nil}
        ))

; 处理别名
(defn get-item-lst [lst]
    (if (> (count lst) 1) (get-item-rv (reverse lst)) {:item-lst lst :alias nil}))

; 预处理 get-query-items 输入
(defn pre-query-lst [[f & rs]]
    (if (some? f)
        (cond (instance? String (first f)) (if (my-lexical/is-eq? (first f) "distinct") (concat [{:keyword "distinct"}] (pre-query-lst (concat [(rest f)] rs))) (concat [f] (pre-query-lst rs)))
              :else
              (concat [f] (pre-query-lst rs)))))

; query-items obj
(defn get-query-items [lst]
    (when-let [[f & rs] (pre-query-lst lst)]
        (try
            (if (map? f) (concat [f] (get-query-items rs))
                         (when-let [{item-lst :item-lst alias :alias} (get-item-lst f)]
                             (concat [(assoc (get-token item-lst) :alias alias)] (get-query-items rs))))
            (catch Exception e (.getMessage e)))))

; table-items obj
; 处理 table-items
(defn get-table
    ([lst] (when-let [m (get-table (reverse lst) [] [])]
               (if (> (count m) 1) (reverse m) m)))
    ([[f & rs] stack lst]
     (if (some? f)
         (cond (and (my-lexical/is-eq? f "on") (= (count stack) 0)) (if (> (count lst) 0) (concat [{:on (reverse lst)}] (get-table rs stack [])) (get-table rs stack []))
               (and (my-lexical/is-eq? f "join") (contains? #{"left" "inner" "right"} (str/lower-case (first rs))) (= (count stack) 0)) (if (> (count lst) 0) (concat [{:tables (reverse lst)}] [{:join (str/join [(first rs) " " f])}] (get-table (rest rs) stack [])) (get-table (rest rs) stack []))
               (and (my-lexical/is-eq? f "join") (not (contains? #{"left" "inner" "right"} (str/lower-case (first rs)))) (= (count stack) 0)) (if (> (count lst) 0) (concat [{:tables (reverse lst)}] [{:join f}] (get-table rs stack [])) (get-table rs stack []))
               (= f ")") (get-table rs (conj stack f) (conj lst f))
               (= f "(") (get-table rs (pop stack) (conj lst f))
               :else
               (get-table rs stack (conj lst f))
               )
         (if (> (count lst) 0) [{:tables (reverse lst)}]))))

; 处理逗号类型的
(defn table-comma
    [lst]
    (if (and (instance? String lst) (my-lexical/is-eq? lst ","))
        (get-token-line lst)
        (cond (= (count lst) 1) (assoc (my-lexical/get-schema (first lst)) :table_alias "")
              (= (count lst) 2) (assoc (my-lexical/get-schema (nth lst 0)) :table_alias (nth lst 1))
              (and (= (count lst) 3) (my-lexical/is-eq? (nth lst 1) "as")) (assoc (my-lexical/get-schema (nth lst 0)) :table_alias (nth lst 2))
              :else
              (when-let [m (get-query-items (concat [lst]))]
                  (first m))
              )))

; 处理 join 类型的
(defn table-join [[f & rs]]
    (if (some? f)
        (cond (contains? f :tables)
              (let [{tables :tables} f]
                  (cond (= (count tables) 1)(concat [(assoc (my-lexical/get-schema (first tables)) :table_alias "")] (table-join rs))
                        (= (count tables) 2) (concat [(assoc (my-lexical/get-schema (nth tables 0)) :table_alias (nth tables 1))] (table-join rs))
                        (and (= (count tables) 3) (my-lexical/is-eq? (nth tables 1) "as")) (concat [(assoc (my-lexical/get-schema (nth tables 0)) :table_alias (nth tables 2))] (table-join rs))
                        ;:else
                        ;(when-let [m (get-query-items tables)]
                        ;    (if (empty? join)
                        ;        (concat [m] (table-join rs))
                        ;        (concat [m {:join join}] (table-join rs))))
                        ))
              (contains? f :join) (concat [{:join (-> f :join)}] (table-join rs))
              (contains? f :on) (cons {:on (map get-token (get f :on))} (table-join rs))
              )))

; 获取 table
(defn get-table-items [table-items]
    (if (= (count table-items) 1)
        (let [m (nth table-items 0)]
            (cond (instance? String m) (concat [(assoc (my-lexical/get-schema m) :table_alias nil)])
                  (and (or (vector? m) (seq? m) (list? m)) (is-select? m)) {:parenthesis (sql-to-ast (get-select-line m))}
                  :else
                  (if (my-lexical/is-contains? (nth table-items 0) "join")
                      (table-join (get-table (nth table-items 0)))
                      (map table-comma table-items))))
        (map table-comma table-items)))

; 获取 order by
(defn get-order-by
    ([lst] (let [m (reverse lst)] (get-order-by (reverse (rest m)) (first m))))
    ([f l]
     (if (and (some? f) (some? l))
         {:order-item (map get-token f) :order l})))

; 获取 limit
(defn get-limit
    ([lst] (get-limit lst (memoize my-comma-fn)))
    ([lst my-comma-fn-m]
     (if (true? (is-true? my-comma-fn-m lst))
         (let [m (my-comma-fn-m lst)]
             (if (= (count m) 3) (map get-token m))))))

; sql to ast
; 当个 select 转换成 ast
(defn sql-to-ast-single [sql-lst]
    (when-let [{query-items :query-items table-items :table-items where-items :where-items group-by :group-by having :having order-by :order-by limit :limit} (my-lexical/get-segments-list sql-lst)]
        {:query-items (get-query-items (my-lexical/to-lazy query-items)) :table-items (get-table-items (my-lexical/to-lazy table-items)) :where-items (get-token where-items) :group-by (get-token group-by) :having (get-token having) :order-by (get-order-by order-by) :limit (get-limit limit)}))

(defn to-ast [lst]
    (if (instance? String lst) {:keyword lst}
                               {:sql_obj (sql-to-ast-single lst)}))

; sql 转换到 lst
; lst 用 union 和 union all 切分
; 获取整个 select 语法树
(defn sql-to-ast [sql-lst]
    (when-let [m (my-lexical/sql-union sql-lst)]
        (map to-ast m)))

; 1、替换 function 在 select ... from table, func(a, b) where ... 中用到
(defn find-table-func [ignite ast]
    (if (some? ast)
        (cond (instance? clojure.lang.LazySeq ast) (map (partial find-table-func ignite) ast)
              (map? ast) (map-replace ignite ast))))

(defn map-replace
    ([ignite m] (map-replace ignite (keys m) m))
    ([ignite [f & rs] m]
     (if (some? f)
         (let [vs (get m f)]
             (cond (= f :table-items) (cond (instance? clojure.lang.LazySeq vs) (recur ignite rs (assoc m f (map (partial re-func ignite) vs)))
                                            (map? vs) (recur ignite rs (assoc m f (plus-func ignite vs)))
                                            :else
                                            (recur ignite rs m))
                   (not (= f :table-items)) (cond (instance? clojure.lang.LazySeq vs) (recur ignite rs (assoc m f (map (partial plus-func ignite) vs)))
                                                  (map? vs) (recur ignite rs (assoc m f (plus-func ignite vs)))
                                                  :else
                                                  (recur ignite rs m))
                   :else
                   (recur ignite rs m))) m)))

(defn re-func [ignite m]
    (if (some? m)
        (cond (or (vector? m) (seq? m) (list? m)) (find-table-func ignite m)
              (map? m) (if (contains? m :func-name)
                           (let [func (my-context/get-func-scenes ignite (get m :func-name))]
                               (if (some? func) (cond (= func "func") (throw (Exception. "自定义方法不能当作结果来查询！"))
                                                      (= func "builtin") (throw (Exception. "内置方法不能当作结果来查询！"))
                                                      (= func "scenes") (if (contains? m :alias) {:parenthesis (get-sql-ast ignite m) :alias (get m :alias)} {:parenthesis (get-sql-ast ignite m)})))) (find-table-func ignite m))
              :else
              (find-table-func ignite m))))

(defn plus-func [ignite m]
    (if (some? m)
        (cond (or (vector? m) (seq? m) (list? m)) (find-table-func ignite m)
              (map? m) (if (contains? m :func-name)
                           (let [func (my-context/get-func-scenes ignite (get m :func-name))]
                               (if (some? func) (cond (= func "func") (concat [(func_scenes_invoke m)])
                                                      (= func "scenes") (concat [(func_scenes_invoke m)])
                                                      :else
                                                      (find-table-func ignite m)) (find-table-func ignite m))) (find-table-func ignite m))
              :else
              (find-table-func ignite m))))

; 调用 func 或 scenes
(defn func_scenes_invoke [func_ast]
    (if (and (contains? func_ast :lst_ps) (> (count (get func_ast :lst_ps)) 0))
        (assoc {:func-name "myInvoke"} :lst_ps (concat [{:table_alias "", :item_name (String/format "'%s'" (object-array [(get func_ast :func-name)])), :item_type "", :java_item_type "String", :const true} {:comma_symbol ","}] (get func_ast :lst_ps)))
        (assoc {:func-name "myInvoke"} :lst_ps (concat [{:table_alias "", :item_name (String/format "'%s'" (object-array [(get func_ast :func-name)])), :item_type "", :java_item_type "String", :const true}] (get func_ast :lst_ps)))))

; 输入 func ast 提取对应的 sql ast
; 并提取 lst_ps
(defn get-sql-ast [ignite func-ast]
    (when-let [func_obj (get-scenes ignite (get func-ast :func-name))]
        (let [my_func_ast (:ast func_obj) lst_func_ps (:lst_func_ps func_obj) lst_ps (get func-ast :lst_ps)]
            (re-func-ast my_func_ast lst_ps lst_func_ps 0)
            )))

(defn re-func-ast [my_func_ast [f & rs] lst_func_ps index]
    (if (some? f)
        (if (not (contains? f :comma_symbol))
            (re-func-ast (re-func-obj f (:ps_name (nth lst_func_ps index)) my_func_ast) rs lst_func_ps (+ index 1))
            (re-func-ast my_func_ast rs lst_func_ps index))
        my_func_ast))

; 输入 ps_lst 替换掉原来对应的元素
; 例如：target_ps 参数的 ast
; resourc_ps 参数名称 :a 例如：{:table_alias "", :item_name ":a", :item_type "", :java_item_type nil, :const false} 中的 item_name
; 具体做法是遍历 ast 替换到
(defn re-func-obj [target_ps resourc_ps my_func_ast]
    (if (some? my_func_ast)
        (cond (instance? clojure.lang.LazySeq my_func_ast) (map (partial re-func-obj target_ps resourc_ps) my_func_ast)
              (map? my_func_ast) (re-func-obj-map target_ps resourc_ps my_func_ast))))

(defn re-func-obj-map [target_ps resourc_ps my_func_ast]
    (if (and (contains? my_func_ast :item_name) (my-lexical/is-eq? (get my_func_ast :item_name) resourc_ps)) target_ps
          (re-func-obj-map-sub target_ps resourc_ps (keys my_func_ast) my_func_ast)))

(defn re-func-obj-map-sub [target_ps resourc_ps [f & rs] my_func_ast]
    (if (some? f)
        (cond (instance? clojure.lang.LazySeq (get my_func_ast f))
              (recur target_ps resourc_ps rs (assoc my_func_ast f (map (partial re-func-obj target_ps resourc_ps) (get my_func_ast f))))
              (map? (get my_func_ast f))
              (let [m (re-func-obj target_ps resourc_ps (get my_func_ast f))]
                  (recur target_ps resourc_ps rs (assoc my_func_ast f m)))
              :else
              (recur target_ps resourc_ps rs my_func_ast)
              ) my_func_ast))

; 确定 query items 的权限
; 函数的参数
(defrecord table_select_view [table_name ast])

(defn replace_alias [alias ast]
    (if (some? ast)
        (cond (instance? clojure.lang.LazySeq ast) (map (partial replace_alias alias) ast)
              (map? ast) (map_replace_alias alias (keys ast) ast))))

(defn map_replace_alias
    ([alias m] (map_replace_alias alias (keys m) m))
    ([alias [f & rs] m]
     (if (some? f)
         (let [vs (get m f)]
             (cond (and (= f :item_name) (= (get m :const) false)) (assoc m :table_alias alias)
                   (instance? clojure.lang.LazySeq vs) (recur alias rs (assoc m f (map (partial replace_alias alias) vs)))
                   (map? vs) (recur alias rs (assoc m f (replace_alias alias vs)))
                   :else
                   (recur alias rs m))) m)))

; 输入参数：
; ignite
; query_ast 是 query item 的树
; group_id : 用户组 ID
(defn query_authority [ignite group_id table_alias_ast query_ast]
    (if (some? query_ast)
        (cond (instance? clojure.lang.LazySeq query_ast) (map (partial query_authority ignite group_id table_alias_ast) query_ast)
              (map? query_ast) (if (and (contains? query_ast :query-items) (contains? query_ast :table-items))
                                   (get_query_table ignite group_id query_ast)
                                   (query_map ignite group_id table_alias_ast query_ast)))))

(defn query_map
    ([ignite group_id table_alias_ast query_ast] (query_map ignite group_id table_alias_ast (keys query_ast) query_ast))
    ([ignite group_id table_alias_ast [f & rs] query_ast]
     (if (some? f)
         (let [vs (get query_ast f)]
             (cond (and (= f :item_name) (= (get query_ast :const) false)) (if (contains? table_alias_ast (get query_ast :table_alias))
                                                                               ; table_obj 是 table_select_view 的对象
                                                                               (let [table_obj (get table_alias_ast (get query_ast :table_alias))]
                                                                                   (if (and (some? table_obj) (some? (get table_obj :ast)) (some? (get (get table_obj :ast) :query-items)))
                                                                                       (if (contains? (get (:ast table_obj) :query-items) (str/lower-case (str/trim (get query_ast :item_name))))
                                                                                           (replace_alias (get query_ast :table_alias) (get (get (:ast table_obj) :query-items) (get query_ast :item_name)))
                                                                                           (throw (Exception. (String/format "没有查询字段(%s)的权限" (object-array [(get query_ast :item_name)])))))
                                                                                       query_ast)) query_ast;(throw (Exception. (String/format "没有查询字段(%s)，请仔细检查拼写是否正确？" (object-array [(get query_ast :item_name)]))))
                                                                               )
                   (instance? clojure.lang.LazySeq vs) (recur ignite group_id table_alias_ast rs (assoc query_ast f (map (partial query_authority ignite group_id table_alias_ast) vs)))
                   (map? vs) (recur ignite group_id table_alias_ast rs (assoc query_ast f (query_authority ignite group_id table_alias_ast vs)))
                   :else
                   (recur ignite group_id table_alias_ast rs query_ast))) query_ast)))

(defn get_where [[f & r] table_select_view_obj where-items]
    (if (some? f)
        (let [table_select (get table_select_view_obj f)]
            (if (some? table_select)
                (let [view_where (get (:ast table_select) :where-items)]
                    (if (some? view_where)
                        (if (some? where-items) (recur r table_select_view_obj (concat [{:parenthesis (replace_alias f view_where)} {:and_or_symbol "and"} {:parenthesis where-items}]))
                                                (recur r table_select_view_obj (replace_alias f view_where))) (recur r table_select_view_obj where-items))) (recur r table_select_view_obj where-items))) where-items))

(defn get_query_table [ignite group_id ast]
    (if (and (map? ast) (contains? ast :query-items) (contains? ast :table-items))
        (let [table_obj (get_map_table_items ignite group_id (get ast :table-items))]
            (if (some? table_obj)
                (let [qt (query_authority ignite group_id table_obj (get ast :query-items)) where (get_where (keys table_obj) table_obj (get ast :where-items))]
                    (assoc ast :query-items qt :where-items where)) ast))
        (cond (instance? clojure.lang.LazySeq ast) (map (partial get_query_table ignite group_id) ast)
              (map? ast) (get_query_table_map ignite group_id (keys ast) ast))))

(defn get_query_table_map [ignite group_id [f & rs] ast]
    (if (some? f)
        (let [vs (get ast f)]
            (cond (instance? clojure.lang.LazySeq vs) (recur ignite group_id rs (assoc ast f (map (partial get_query_table ignite group_id) vs)))
                  (map? vs) (recur ignite group_id rs (assoc ast f (get_query_table ignite group_id vs)))
                  :else
                  (recur ignite group_id rs ast))) ast))

(defn get_query_view
    ([query-items] (get_query_view query-items {}))
    ([[f & r] dic]
     (if (some? f)
         (cond (contains? f :item_name) (recur r (assoc dic (get f :item_name) nil))
               (and (contains? f :func-name) (my-lexical/is-eq? (get f :func-name) "convert_to") (= (count (get f :lst_ps)) 3)) (recur r (assoc dic (get (first (get f :lst_ps)) :item_name) (last (get f :lst_ps))))
               (contains? f :comma_symbol) (recur r dic)
               :else
               (throw (Exception. "select 权限视图中只能是字段或者是转换函数！")))
         dic)))

; 获取 table_select_view 的 ast
; 重新生成新的 ast
; 新的 ast = {query_item = {'item_name': '转换的函数'}}
(defn get_select_view [ignite group_id schema_name talbe_name]
    (when-let [code (first (.getAll (.query (.cache ignite "my_select_views") (.setArgs (SqlFieldsQuery. "select m.code from my_select_views as m, my_group_view as v where m.id = v.view_id and m.table_name = ? and v.my_group_id = ? and v.view_type = ?") (to-array [talbe_name group_id "查"])))))]
        (when-let [sql_objs (sql-to-ast (my-lexical/to-back (nth code 0)))]
            (if (= (count sql_objs) 1)
                (when-let [{query-items :query-items where-items :where-items} (get (nth sql_objs 0) :sql_obj)]
                    {:query-items (get_query_view query-items) :where-items where-items})))))

; 将 table-items 中的 table_name 和 table_alias 转换为 map
; 输入 ignite group_id
; table-items = (get select_ast :table-items)
; 返回结果：map ，key 值为 别名， value 为 table_select_view
(defn get_map_table_items
    ([ignite group_id table-items] (get_map_table_items ignite group_id table-items {}))
    ([ignite group_id [f & rs] m]
     (if (some? f)
         (if (contains? f :table_name)
             (let [table_ast (get_select_view ignite group_id (-> f :schema_name) (get f :table_name))]
                 (if (some? table_ast)
                     (if (contains? f :table_alias)
                         (recur ignite group_id rs (assoc m (get f :table_alias) (table_select_view. (str/lower-case (str/trim (get f :table_name))) table_ast)))
                         (recur ignite group_id rs (assoc m "" (table_select_view. (get f :table_name) table_ast))))
                     (if (contains? f :table_alias)
                         (recur ignite group_id rs (assoc m (get f :table_alias) (table_select_view. (get f :table_name) nil)))
                         (recur ignite group_id rs (assoc m "" (table_select_view. (get f :table_name) nil)))))) (recur ignite group_id rs m)) m)))

; sql to myAst
(defn get_my_ast [ignite group_id sql]
    (if-not (Strings/isNullOrEmpty sql)
        (when-let [ast (sql-to-ast (my-lexical/to-back sql))]
            (when-let [func_ast (find-table-func ignite ast)]
                (get_query_table ignite group_id func_ast)))))

; 在 insert, update, delete 中会用到
(defn get_my_ast_lst [ignite group_id lst]
    (when-let [ast (sql-to-ast lst)]
        (when-let [func_ast (find-table-func ignite ast)]
            (get_query_table ignite group_id func_ast))))

; 转换成查询字符串
(def sql_symbol #{"(" ")" "/" "*" "-" "+" "=" ">" "<" ">=" "<=" "<>" ","})

; array 转换为 sql
(defn ar-to-sql [[f & rs] ^StringBuilder sb]
    (cond (and (some? f) (some? (first rs))) (cond (and (my-lexical/is-eq? f "(") (my-lexical/is-eq? (first rs) "select")) (recur rs (.append sb f))
                                                   (and (my-lexical/is-eq? f ")") (my-lexical/is-eq? (first rs) "from")) (recur rs (.append (.append sb f) " "))
                                                   (and (my-lexical/is-eq? f ")") (my-lexical/is-eq? (first rs) "where")) (recur rs (.append (.append sb f) " "))
                                                   (and (my-lexical/is-eq? f ")") (my-lexical/is-eq? (first rs) "or")) (recur rs (.append (.append sb f) " "))
                                                   (and (my-lexical/is-eq? f ")") (my-lexical/is-eq? (first rs) "in")) (recur rs (.append (.append sb f) " "))
                                                   (and (my-lexical/is-eq? f ")") (my-lexical/is-eq? (first rs) "exists")) (recur rs (.append (.append sb f) " "))
                                                   (and (my-lexical/is-eq? f ")") (my-lexical/is-eq? (first rs) "and")) (recur rs (.append (.append sb f) " "))
                                                   (and (my-lexical/is-eq? f ")") (my-lexical/is-eq? (first rs) "union")) (recur rs (.append (.append sb f) " "))
                                                   (and (my-lexical/is-eq? f ")") (my-lexical/is-eq? (first rs) "union all")) (recur rs (.append (.append sb f) " "))
                                                   (and (my-lexical/is-eq? f ")") (my-lexical/is-eq? (first rs) "not in")) (recur rs (.append (.append sb f) " "))
                                                   (and (my-lexical/is-eq? f ")") (my-lexical/is-eq? (first rs) "not exists")) (recur rs (.append (.append sb f) " "))
                                                   (and (my-lexical/is-eq? f "from") (my-lexical/is-eq? (first rs) "(")) (recur rs (.append (.append sb f) " "))
                                                   (and (my-lexical/is-eq? f "where") (my-lexical/is-eq? (first rs) "(")) (recur rs (.append (.append sb f) " "))
                                                   (and (my-lexical/is-eq? f "or") (my-lexical/is-eq? (first rs) "(")) (recur rs (.append (.append sb f) " "))
                                                   (and (my-lexical/is-eq? f "and") (my-lexical/is-eq? (first rs) "(")) (recur rs (.append (.append sb f) " "))
                                                   (and (my-lexical/is-eq? f "in") (my-lexical/is-eq? (first rs) "(")) (recur rs (.append (.append sb f) " "))
                                                   (and (my-lexical/is-eq? f "union") (my-lexical/is-eq? (first rs) "(")) (recur rs (.append (.append sb f) " "))
                                                   (and (my-lexical/is-eq? f "union all") (my-lexical/is-eq? (first rs) "(")) (recur rs (.append (.append sb f) " "))
                                                   (and (my-lexical/is-eq? f "not in") (my-lexical/is-eq? (first rs) "(")) (recur rs (.append (.append sb f) " "))

                                                   (and (not (contains? sql_symbol f)) (not (contains? sql_symbol (first rs)))) (recur rs (.append (.append sb f) " "))
                                                   :else
                                                   (recur rs (.append sb f)))
          (and (some? f) (Strings/isNullOrEmpty (first rs))) (recur rs (.append sb f))
          :else
          sb))

(defn ar-to-lst [[f & rs]]
    (if (some? f)
        (try
            (if (string? f) (cons f (ar-to-lst rs))
                            (concat (ar-to-lst f) (ar-to-lst rs)))
            (catch Exception e (.getMessage e))) []))

(defn my-array-to-sql [lst]
    (if (nil? lst) nil
                   (if (string? lst) lst
                                     (.toString (ar-to-sql (ar-to-lst lst) (StringBuilder.))))))

(defn item-to-line [m]
    (let [{table_alias :table_alias item_name :item_name alias :alias} m]
        (cond
            (and (not (Strings/isNullOrEmpty table_alias)) (not (nil? alias)) (not (Strings/isNullOrEmpty alias))) (str/join [table_alias "." item_name " as " alias])
            (and (not (Strings/isNullOrEmpty table_alias)) (Strings/isNullOrEmpty alias)) (str/join [table_alias "." item_name])
            (and (Strings/isNullOrEmpty table_alias) (Strings/isNullOrEmpty alias)) item_name
            )))

; table item 转换成 line
(defn table-to-line [ignite group_id m]
    (if (some? m)
        (if-let [{schema_name :schema_name table_name :table_name table_alias :table_alias} m]
            (if (Strings/isNullOrEmpty table_alias)
                (if (Strings/isNullOrEmpty schema_name)
                    table_name
                    (format "%s.%s" schema_name table_name))
                (if (Strings/isNullOrEmpty schema_name)
                    (format "%s %s" table_name table_alias)
                    (format "%s.%s %s" schema_name table_name table_alias))
                ;(let [data_set_name (get_data_set_name ignite group_id)]
                ;    (if (Strings/isNullOrEmpty data_set_name)
                ;        table_name
                ;        (str/join [data_set_name "_" table_name])))
                ;(let [data_set_name (get_data_set_name ignite group_id)]
                ;    (if (Strings/isNullOrEmpty data_set_name)
                ;        (str/join [table_name " " table_alias])
                ;        (str/join [(str/join [data_set_name "_" table_name]) " " table_alias])))
                ))))

; on 转换成 line
(defn on-to-line [ignite group_id m]
    (if (some? m)
        (str/join ["on" (token-to-sql ignite group_id (get m :on))])))

(defn func-to-line [ignite group_id m]
    (if (and (contains? m :alias) (not (nil? (-> m :alias))))
        (concat [(-> m :func-name) "("] (map (partial token-to-sql ignite group_id) (-> m :lst_ps)) [")" " as"] [(-> m :alias)])
        (concat [(-> m :func-name) "("] (map (partial token-to-sql ignite group_id) (-> m :lst_ps)) [")"])))

(defn lst-token-to-line
    ([ignite group_id lst_token] (cond (string? lst_token) lst_token
                                       (map? lst_token) (my-array-to-sql (token-to-sql ignite group_id lst_token))
                                       :else
                                       (my-array-to-sql (lst-token-to-line ignite group_id lst_token []))))
    ([ignite group_id [f & rs] lst]
     (if (some? f)
         (recur ignite group_id rs (conj lst (my-array-to-sql (token-to-sql ignite group_id f)))) lst)))

(defn select_to_sql_single [ignite group_id ast]
    (if (and (some? ast) (map? ast))
        (when-let [{query-items :query-items table-items :table-items where-items :where-items group-by :group-by having :having order-by :order-by limit :limit} ast]
            (cond (and (some? query-items) (some? table-items) (empty? where-items) (empty? group-by) (empty? having) (empty? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (some? where-items) (empty? group-by) (empty? having) (empty? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (empty? having) (empty? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (some? having) (empty? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (some? having) (some? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having) "order by" (lst-token-to-line ignite group_id order-by)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (some? having) (some? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having) "order by" (lst-token-to-line ignite group_id order-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (some? having) (empty? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))

                  (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (empty? having) (some? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "order by" (lst-token-to-line ignite group_id order-by)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (empty? having) (empty? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (empty? having) (some? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "order by" (lst-token-to-line ignite group_id order-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))

                  (and (some? query-items) (some? table-items) (some? where-items) (empty? group-by) (empty? having) (some? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "order by" (lst-token-to-line ignite group_id order-by)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (some? where-items) (empty? group-by) (empty? having) (empty? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (some? where-items) (empty? group-by) (empty? having) (some? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "order by" (lst-token-to-line ignite group_id order-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))

                  (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (empty having) (empty? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "group by" (lst-token-to-line ignite group_id group-by)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (some? having) (empty? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (some? having) (some? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having) "order by" (lst-token-to-line ignite group_id order-by)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (some? having) (some? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having) "order by" (lst-token-to-line ignite group_id order-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))

                  (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (empty having) (empty? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "group by" (lst-token-to-line ignite group_id group-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (some? having) (empty? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))

                  (and (some? query-items) (some? table-items) (empty? where-items) (empty? group-by) (empty? having) (some? order-by) (empty? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "order by" (lst-token-to-line ignite group_id order-by)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (empty? where-items) (empty? group-by) (empty? having) (some? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "order by" (lst-token-to-line ignite group_id order-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (empty? where-items) (empty? group-by) (empty? having) (empty? order-by) (some? limit)) (.toString (ar-to-sql ["select" (lst-token-to-line ignite group_id query-items) "from" (lst-token-to-line ignite group_id table-items) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))

                  ))))

(defn select_to_sql_single_count [ignite group_id ast]
    (if (and (some? ast) (map? ast))
        (when-let [{query-items :query-items table-items :table-items where-items :where-items group-by :group-by having :having order-by :order-by limit :limit} ast]
            (cond (and (some? query-items) (some? table-items) (empty? where-items) (empty? group-by) (empty? having) (empty? order-by) (empty? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (some? where-items) (empty? group-by) (empty? having) (empty? order-by) (empty? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (empty? having) (empty? order-by) (empty? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (some? having) (empty? order-by) (empty? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (some? having) (some? order-by) (empty? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having) "order by" (lst-token-to-line ignite group_id order-by)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (some? having) (some? order-by) (some? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having) "order by" (lst-token-to-line ignite group_id order-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (some? having) (empty? order-by) (some? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))

                  (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (empty? having) (some? order-by) (empty? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "order by" (lst-token-to-line ignite group_id order-by)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (empty? having) (empty? order-by) (some? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (some? where-items) (some? group-by) (empty? having) (some? order-by) (some? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "group by" (lst-token-to-line ignite group_id group-by) "order by" (lst-token-to-line ignite group_id order-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))

                  (and (some? query-items) (some? table-items) (some? where-items) (empty? group-by) (empty? having) (some? order-by) (empty? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "order by" (lst-token-to-line ignite group_id order-by)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (some? where-items) (empty? group-by) (empty? having) (empty? order-by) (some? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (some? where-items) (empty? group-by) (empty? having) (some? order-by) (some? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "where" (lst-token-to-line ignite group_id where-items) "order by" (lst-token-to-line ignite group_id order-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))

                  (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (empty having) (empty? order-by) (empty? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "group by" (lst-token-to-line ignite group_id group-by)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (some? having) (empty? order-by) (empty? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (some? having) (some? order-by) (empty? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having) "order by" (lst-token-to-line ignite group_id order-by)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (some? having) (some? order-by) (some? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having) "order by" (lst-token-to-line ignite group_id order-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))

                  (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (empty having) (empty? order-by) (some? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "group by" (lst-token-to-line ignite group_id group-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (empty? where-items) (some? group-by) (some? having) (empty? order-by) (some? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "group by" (lst-token-to-line ignite group_id group-by) "having" (lst-token-to-line ignite group_id having) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))

                  (and (some? query-items) (some? table-items) (empty? where-items) (empty? group-by) (empty? having) (some? order-by) (empty? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "order by" (lst-token-to-line ignite group_id order-by)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (empty? where-items) (empty? group-by) (empty? having) (some? order-by) (some? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "order by" (lst-token-to-line ignite group_id order-by) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))
                  (and (some? query-items) (some? table-items) (empty? where-items) (empty? group-by) (empty? having) (empty? order-by) (some? limit)) (.toString (ar-to-sql ["select count(1) from" (lst-token-to-line ignite group_id table-items) "limit" (lst-token-to-line ignite group_id limit)] (StringBuilder.)))

                  ))))

(defn select-to-sql
    ([ignite group_id ast] (cond (and (some? ast) (instance? clojure.lang.LazySeq ast)) (.toString (ar-to-sql (select-to-sql ignite group_id ast []) (StringBuilder.)))
                                 (contains? ast :sql_obj) (select_to_sql_single ignite group_id (get ast :sql_obj))
                                 :else
                                 (throw (Exception. "select 语句错误！"))))
    ([ignite group_id [f & rs] lst_rs]
     (if (some? f)
         (if (map? f)
             (cond (contains? f :sql_obj) (recur ignite group_id rs (conj lst_rs (select_to_sql_single ignite group_id (get f :sql_obj))))
                   (contains? f :keyword) (recur ignite group_id rs (conj lst_rs (get f :keyword)))
                   :else
                   (throw (Exception. "select 语句错误！"))) (throw (Exception. "select 语句错误！"))) lst_rs)))

(defn select-to-sql-count
    ([ignite group_id ast] (cond (and (some? ast) (instance? clojure.lang.LazySeq ast)) (.toString (ar-to-sql (select-to-sql-count ignite group_id ast []) (StringBuilder.)))
                                 (contains? ast :sql_obj) (select_to_sql_single_count ignite group_id (get ast :sql_obj))
                                 :else
                                 (throw (Exception. "select 语句错误！"))))
    ([ignite group_id [f & rs] lst_rs]
     (if (some? f)
         (if (map? f)
             (cond (contains? f :sql_obj) (recur ignite group_id rs (conj lst_rs (select_to_sql_single_count ignite group_id (get f :sql_obj))))
                   (contains? f :keyword) (recur ignite group_id rs (conj lst_rs (get f :keyword)))
                   :else
                   (throw (Exception. "select 语句错误！"))) (throw (Exception. "select 语句错误！"))) lst_rs)))

(defn token-to-sql [ignite group_id m]
    (if (some? m)
        (cond (or (vector? m) (seq? m) (list? m)) (map (partial token-to-sql ignite group_id) m)
              (map? m) (map-token-to-sql ignite group_id m))))

; map token to sql
(defn map-token-to-sql
    "map token to sql"
    [ignite group_id m]
    (if (some? m)
        (cond
            (contains? m :sql_obj) (select-to-sql ignite group_id m)
            (and (contains? m :func-name) (contains? m :lst_ps)) (func-to-line ignite group_id m)
            (contains? m :and_or_symbol) (get m :and_or_symbol)
            (contains? m :keyword) (get m :keyword)
            (contains? m :operation) (map (partial token-to-sql ignite group_id) (get m :operation))
            (contains? m :comparison_symbol) (get m :comparison_symbol)
            (contains? m :in_symbol) (get m :in_symbol)
            (contains? m :operation_symbol) (get m :operation_symbol)
            (contains? m :join) (get m :join)
            (contains? m :on) (on-to-line ignite group_id m)
            (contains? m :comma_symbol) (get m :comma_symbol)
            (contains? m :order-item) (concat (token-to-sql ignite group_id (-> m :order-item)) [(-> m :order)])
            (contains? m :item_name) (item-to-line m)
            (contains? m :table_name) (table-to-line ignite group_id m)
            (contains? m :exists) (concat [(get m :exists) "("] (token-to-sql ignite group_id (get (get m :select_sql) :parenthesis)) [")"])
            (contains? m :parenthesis) (concat ["("] (token-to-sql ignite group_id (get m :parenthesis)) [")"])
            ;:else
            ;(throw (Exception. "select 语句错误！请仔细检查！"))
            )))

; 输入 select sql 转换后获取 select sql
;(defn get_plus_sql [ignite group_id sql]
;    (when-let [ast (get_my_ast ignite group_id sql)]
;        (my-array-to-sql (token-to-sql ignite group_id ast))))

; 判断表是否在 data set 中
;(defn find_table_in_data_set [ast lst_table] ())

(defn -get_plus_sql [^Ignite ignite ^Long group_id ^String sql]
    (when-let [ast (get_my_ast ignite group_id sql)]
        (select-to-sql ignite group_id ast)))

(defn my_plus_sql [^Ignite ignite ^Long group_id ^String sql]
    (if-let [ast (get_my_ast ignite group_id sql)]
        (select-to-sql ignite group_id ast)
        (throw (Exception. (format "查询字符串 %s 错误！" sql)))))

; lst to sql 在 insert, update, delete 中会用到
(defn lst_to_sql [ignite group_id lst]
    (when-let [ast (get_my_ast_lst ignite group_id lst)]
        (select-to-sql ignite group_id ast)))

(defn lst_to_sql_paras [ignite group_id lst input_paras]
    (when-let [ast (get_my_ast_lst ignite group_id lst)]
        (when-let [lst_token (select-to-sql ignite group_id ast)]
            (param_lst lst_token input_paras))
         ))

(defn lst_to_sql_paras_save [ignite group_id lst input_paras]
    (when-let [ast (get_my_ast_lst ignite group_id lst)]
        (when-let [lst_token (select-to-sql ignite group_id ast)]
            (param_lst_index lst_token input_paras))
        ))

; 具体的用法：
; 1、当用户在编辑状态下运行 sql , 直接调用 -get_plus_sql 方法即可
; 2.1、执行 -get_plus_sql 生成字符串后，
; 2.2、在执行 my-lexical/to-back 将它变成字符串数组
; 2.3、在递归里面的参数为 ？, 并形成参数列表

; 输入第一个参数 input_paras
; 输入第二个参数 :a
(defn has_input_paras [[f & r] vs]
    (if (some? f)
        (if (my-lexical/is-eq? (str/join [":" (.getPs_name f)]) vs) f
                                                                           (recur r vs))))

; 获取 param value
(defn get_vs [^MyScenesParams f]
    (cond (= (.getPs_type f) String) (.getPs_name f)
          (= (.getPs_type f) Integer) (MyConvertUtil/ConvertToInt (.getPs_name f))
          (= (.getPs_type f) Boolean) (MyConvertUtil/ConvertToBoolean (.getPs_name f))
          (= (.getPs_type f) Long) (MyConvertUtil/ConvertToLong (.getPs_name f))
          (= (.getPs_type f) Timestamp) (MyConvertUtil/ConvertToTimestamp (.getPs_name f))
          (= (.getPs_type f) BigDecimal) (MyConvertUtil/ConvertToDecimal (.getPs_name f))
          ))

; 替换 ？形成参数列表
(defn param_lst
    ([lst_token input_paras] (when-let [{lst_sql_tokens :lst_sql_tokens lst_ps :lst_ps} (param_lst lst_token input_paras [] [])]
                                 {:sql (my-array-to-sql lst_sql_tokens) :lst_ps lst_ps}))
    ([[f & r] input_paras lst_sql_tokens lst_result]
     (if (some? f)
         (if-let [mi (has_input_paras input_paras f)]
             (recur r input_paras (conj lst_sql_tokens "?") (conj lst_result (get_vs mi)))
             (recur r input_paras (conj lst_sql_tokens f) lst_result))
         {:lst_sql_tokens lst_sql_tokens :lst_ps lst_result})))

; 替换 ？形成参数列表，参数列表中用 index 来代替原来的参数
; input_paras 为 MyScenesParams 的列表
(defn param_lst_index
    ([lst_token input_paras] (when-let [{lst_sql_tokens :lst_sql_tokens lst_ps :lst_ps} (param_lst_index lst_token input_paras [] [])]
                                 {:sql (my-array-to-sql lst_sql_tokens) :lst_ps lst_ps}))
    ([[f & r] input_paras lst_sql_tokens lst_result]
     (if (some? f)
         (if-let [mi (has_input_paras input_paras f)]
             (recur r input_paras (conj lst_sql_tokens "?") (conj lst_result (.getIndex mi)))
             (recur r input_paras (conj lst_sql_tokens f) lst_result))
         {:lst_sql_tokens lst_sql_tokens :lst_ps lst_result})))

; 输入 MySences 对象获取 sql 字符串和参数
; 返回结果 {:sql "" :lst_ps ["a" "b"]}
(defn get_sql_args [^Ignite ignite ^Long group_id ^MyScenesCache sences]
    (when-let [lst_token (my-lexical/to-back (-get_plus_sql ignite group_id (.getScenes_code sences)))]
        (param_lst lst_token (.getParams sences))))

; 输入 MySences 对象获取 sql 字符串和参数
; 返回结果 {:sql "" :lst_ps [0 1 1]}
(defn get_sql_args_index [^Ignite ignite ^Long group_id ^MyScenesCache sences]
    (when-let [lst_token (my-lexical/to-back (-get_plus_sql ignite group_id (.getScenes_code sences)))]
        (param_lst_index lst_token (.getParams sences))))

; 1、方法执行
(defn get_plus_sql_func [^Ignite ignite ^Long group_id ^MyScenesCache sences]
    (when-let [lst_token (my-lexical/to-back (-get_plus_sql ignite group_id (.getScenes_code sences)))]
        (when-let [{sql :sql lst_ps :lst_ps} (param_lst lst_token (.getParams sences))]
            (if (some? lst_ps)
                (.getAll (.query (.getOrCreateCache ignite (MyDbUtil/getPublicCfg)) (.setArgs (SqlFieldsQuery. sql) lst_ps)))
                (.getAll (.query (.getOrCreateCache ignite (MyDbUtil/getPublicCfg)) (SqlFieldsQuery. sql)))
                ))))

; 获取 MyScenesPs lst
;(defn get_myScenesPs_lst [[f & rs] ^ArrayList lst]
;    (if (some? f)
;        (recur rs (doto lst (.add (MyScenesPs. (.getPs_type f) (.getIndex f)))))
;        lst))
;
;(defn get_myScenesPs_line [[f & rs] ^StringBuilder sb]
;    (if (some? f)
;        (if (some? rs)
;            (cond (= (.getPs_type f) String) (recur rs (.append sb (format "{String: %s}," (.getIndex f))))
;                  (= (.getPs_type f) Integer) (recur rs (.append sb (format "{Integer: %s}," (.getIndex f))))
;                  (= (.getPs_type f) Long) (recur rs (.append sb (format "{Long: %s}," (.getIndex f))))
;                  (= (.getPs_type f) Boolean) (recur rs (.append sb (format "{Boolean: %s}," (.getIndex f))))
;                  (= (.getPs_type f) BigDecimal) (recur rs (.append sb (format "{BigDecimal: %s}," (.getIndex f))))
;                  )
;            (cond (= (.getPs_type f) String) (recur rs (.append sb (format "{String: %s}" (.getIndex f))))
;                  (= (.getPs_type f) Integer) (recur rs (.append sb (format "{Integer: %s}" (.getIndex f))))
;                  (= (.getPs_type f) Long) (recur rs (.append sb (format "{Long: %s}" (.getIndex f))))
;                  (= (.getPs_type f) Boolean) (recur rs (.append sb (format "{Boolean: %s}" (.getIndex f))))
;                  (= (.getPs_type f) BigDecimal) (recur rs (.append sb (format "{BigDecimal: %s}" (.getIndex f))))
;                  ))
;        (.toString sb)))

; 2、存到 cache
;(defn save_select_to_db [^Ignite ignite ^Long group_id ^MyScenesCache sences]
;    (when-let [{sql :sql lst_ps :lst_ps} (get_sql_args_index ignite group_id sences)]
;        (when-let [selectScenes (doto (MySelectScenes.)
;                                    (.setSelect_sql sql)
;                                    (.setIndexs (to-array lst_ps))
;                                    (.setLst_ps (get_myScenesPs_lst (.getParams sences) (java.util.ArrayList.))))]
;            (.put (.cache ignite "my_scenes") (.getScenes_name sences) (doto (MyScenesCache.)
;                                                                           (.setScenes_name (.getScenes_name sences))
;                                                                           (.setScenes_code (.getScenes_code sences))
;                                                                           (.setPs_code (get_myScenesPs_line (.getParams sences) (StringBuilder.)))
;                                                                           (.setDescrip (.getDescrip sences))
;                                                                           (.setVersion (.getVersion sences))
;                                                                           (.setScenes (Scenes. (Scenes_type/select) selectScenes))
;                                                                           (.setIs_batch (.getIs_batch sences))
;                                                                           (.setActive true)
;                                                                           )))
;        ))

; 获取 inoutParamsEx 通过 index
(defn get_inputParamsEx_by_id [[f_inputParamsEx & rs_inputParamsEx] index]
    (if (some? f_inputParamsEx)
        (if (and (= (.getIndex f_inputParamsEx) index) (>= index 0))
            f_inputParamsEx
            (recur rs_inputParamsEx index))
        ))

; 获取实参列表
(defn get_ps
    ([indexs myInputParamEx_lst] (get_ps indexs myInputParamEx_lst []))
    ([[f & rs] myInputParamEx_lst lst_ps]
     (if (some? f)
         (when-let [m (get_inputParamsEx_by_id myInputParamEx_lst f)]
             (cond (= (.getPs_type m) String) (recur rs myInputParamEx_lst (conj lst_ps (.getParameter_value m)))
                   (= (.getPs_type m) Integer) (recur rs myInputParamEx_lst (conj lst_ps (MyConvertUtil/ConvertToInt (.getParameter_value m))))
                   (= (.getPs_type m) Boolean) (recur rs myInputParamEx_lst (conj lst_ps (MyConvertUtil/ConvertToBoolean (.getParameter_value m))))
                   (= (.getPs_type m) Long) (recur rs myInputParamEx_lst (conj lst_ps (MyConvertUtil/ConvertToLong (.getParameter_value m))))
                   (= (.getPs_type m) Timestamp) (recur rs myInputParamEx_lst (conj lst_ps (MyConvertUtil/ConvertToTimestamp (.getParameter_value m))))
                   (= (.getPs_type m) BigDecimal) (recur rs myInputParamEx_lst (conj lst_ps (MyConvertUtil/ConvertToDecimal (.getParameter_value m))))
                   )
             )
         (to-array lst_ps))))

; 3、调用在 cache 中的服务网格
(defn call [^Ignite ignite ^String scenes_name ^ArrayList myInputParamEx_lst]
    (when-let [m (.get (.cache ignite "my_scenes") scenes_name)]
        (.getAll (.query (.getOrCreateCache ignite (MyDbUtil/getPublicCfg)) (.setArgs (SqlFieldsQuery. (.getSelect_sql m)) (get_ps (.getIndexs m) myInputParamEx_lst))))))

(defn call_iterator [^Ignite ignite ^String scenes_name ^ArrayList myInputParamEx_lst]
    (when-let [m (.get (.cache ignite "my_scenes") scenes_name)]
        (.iterator (.query (.getOrCreateCache ignite (MyDbUtil/getPublicCfg)) (.setArgs (doto (SqlFieldsQuery. (.getSelect_sql m))
                                                                       (.setLazy true)) (get_ps (.getIndexs m) myInputParamEx_lst))))))

; 4、在测试环境中自动判断是否实时或者是批处理
; 判断的标准是多少数据集

; 获取 count 的
(defn test_count [^Ignite ignite ^Long group_id ^String sql]
    (when-let [ast (get_my_ast ignite group_id sql)]
        (select-to-sql-count ignite group_id ast)))
















