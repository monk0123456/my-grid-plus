(ns org.gridgain.plus.dml.my-smart-clj
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
        :name org.gridgain.plus.dml.MySmartClj
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [get_plus_sql [org.apache.ignite.Ignite Long String] String]
        ;          ^:static [getSqlToAst [org.apache.ignite.Ignite String String] clojure.lang.LazySeq]
        ;          ^:static [putAstCache [org.apache.ignite.Ignite String String String] void]]
        ))

(declare ast-to-clj body-to-clj token-to-clj token-lst-clj)

; 添加 let 定义到 my-context
(defn add-let-to-context [let-name let-vs my-context]
    (if (some? my-context)
        (let [let-params (-> my-context :let-params)]
            (assoc my-context :let-params (assoc let-params let-name let-vs)))))

; 获取 let 的定义
(defn get-let-context [let-name my-context]
    (let [let-params (-> my-context :let-params)]
        (get let-params let-name)))

; 判断 token 中是否 table item name 是否存在
(defn is-exist-in-token? [item_name token]
    (letfn [(is-exist-lst-token? [item_name [f-token & r-token]]
                (if (some? f-token)
                    (if (true? (is-exist-token? f-token))
                        true
                        (recur item_name r-token))
                    false))
            (is-exist-token? [item_name token]
                (cond (map? token) (loop [[f & r] (keys token)]
                                       (if (some? f)
                                           (cond (and (= f :item_name) (my-lexical/is-eq? item_name (-> token :item_name))) true
                                                 (map? (-> token f)) (if (true? (is-exist-token? item_name (-> token f)))
                                                                         true
                                                                         (recur r))
                                                 (or (vector? (-> token f)) (seq? (-> token f)) (list? (-> token f))) (if (true? (is-exist-lst-token? item_name (-> token f)))
                                                                                                                          true
                                                                                                                          (recur r))
                                                 :else
                                                 (recur r)
                                                 )
                                           false))
                      (or (vector? token) (seq? token) (list? token)) (if (true? (is-exist-lst-token? item_name token))
                                                                          true
                                                                          false)
                      :else
                      false
                    ))]
        (is-exist-token? item_name token)))

(defn token-to-clj [ignite group_id m is-set my-context]
    ())

; 处于同一层就返回 true 否则 false
(defn is-same-layer? [f r]
    (if (false? (is-exist-in-token? (-> f :let-name) r))
        true
        false))

(defn letLayer-to-clj
    ([^MyLetLayer letLayer] (letLayer-to-clj letLayer [] []))
    ([^MyLetLayer letLayer lst-head lst-tail]
     (if-not (nil? letLayer)
         (recur (.getUpLayer letLayer) (conj lst-head (format "(let [%s]" (str/join " " (.getLst letLayer)))) (conj lst-tail ")"))
         [(str/join " " lst-head) (str/join lst-tail)])))

(defn let-to-clj
    ([ignite group_id lst-let my-context] (let-to-clj ignite group_id lst-let my-context (MyLetLayer.)))
    ([ignite group_id [f & r] my-context letLayer]
     (if (some? f)
         (let [same-layer? (is-same-layer? f r)]
             (if (true? same-layer?)
                 (recur ignite group_id r my-context (.addLet letLayer (format "%s (MyVar. %s)" (-> f :let-name) (token-to-clj ignite group_id (-> f :let-vs) false (add-let-to-context (-> f :let-name) (-> f :let-vs) my-context)))))
                 (recur ignite group_id r my-context (MyLetLayer. (format "%s (MyVar. %s)" (-> f :let-name) (token-to-clj ignite group_id (-> f :let-vs) false (add-let-to-context (-> f :let-name) (-> f :let-vs) my-context))) letLayer))
                 ))
         (letLayer-to-clj letLayer))))

; 表达式 to clj
(defn express-to-clj [ignite group_id [f-express r-express] my-context]
    (if (some? f-express)
        (cond (and (contains? f-express :expression) (my-lexical/is-eq? (-> f-express :expression) "for")) ()
              (and (contains? f-express :expression) (my-lexical/is-eq? (-> f-express :expression) "match")) ()
              (contains? f-express :functions) ()
              (contains? f-express :express) ()
              )))

(defn body-to-clj
    ([ignite group_id lst my-context] (body-to-clj ignite group_id lst my-context []))
    ([ignite group_id [f & r] my-context lst-rs]
     (if (some? f)
         (cond (contains? f :let-name) (recur ignite group_id r my-context (conj lst-rs f))
               (and (not (empty? lst-rs)) (not (contains? f :let-name))) (if-not (nil? r)
                                                                             (if (= (count r) 1)
                                                                                 (let [[let-first let-tail] (let-to-clj ignite group_id lst-rs my-context) express-line (express-to-clj ignite group_id r my-context)]
                                                                                     (format "%s %s %s" let-first express-line let-tail))
                                                                                 (let [[let-first let-tail] (let-to-clj ignite group_id lst-rs my-context) express-line (express-to-clj ignite group_id r my-context)]
                                                                                     (format "%s (do\n    %s) %s" let-first express-line let-tail)))
                                                                             )
             ))))

; my-context 记录上下文
; :input-params 输入参数
; :let-params 定义变量
; :last-item 上一个 token
; :inner-func inner-func 名字
; :up-my-context 上一层的 my-context
; my-context: {:input-params #{} :let-params {} :last-item nil :up-my-context nil}
(defn ast-to-clj [ignite group_id ast up-my-context]
    (let [{func-name :func-name args-lst :args-lst body-lst :body-lst} ast my-context {:input-params #{} :let-params {} :last-item nil :inner-func #{} :up-my-context up-my-context}]
        (let [func-context (assoc my-context :input-params (apply conj (-> my-context :input-params) args-lst))]
            (format "(%s [%s]\n    %s)" func-name (str/join " " args-lst) (body-to-clj ignite group_id body-lst func-context)))
        ))























































