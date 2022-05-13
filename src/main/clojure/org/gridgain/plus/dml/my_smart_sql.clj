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
        :name org.gridgain.plus.dml.MySmartSql
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [get_plus_sql [org.apache.ignite.Ignite Long String] String]
        ;          ^:static [getSqlToAst [org.apache.ignite.Ignite String String] clojure.lang.LazySeq]
        ;          ^:static [putAstCache [org.apache.ignite.Ignite String String String] void]]
        ))

(declare body-segment get-ast-lst get-ast get-re-pair get-pairs get-pairs-tokens
         my-item-tokens get-pair-item-ex split-pair-item-ex)

(defn add-let-name [my-context let-name]
    (assoc my-context :let-params (conj (-> my-context :let-params) let-name)))

(defn get-pair-item-ex
    ([lst] (get-pair-item-ex lst [] nil [] []))
    ([[f & r] stack mid-small stack-lst lst]
     (if (some? f)
         (cond (= f "(") (if (or (= mid-small "mid") (= mid-small "big"))
                             (recur r stack mid-small (conj stack-lst f) lst)
                             (recur r (conj stack f) "small" (conj stack-lst f) lst))
               (= f "[") (if (or (= mid-small "small") (= mid-small "big"))
                             (recur r stack mid-small (conj stack-lst f) lst)
                             (recur r (conj stack f) "mid" (conj stack-lst f) lst))
               (= f "{") (if (or (= mid-small "mid") (= mid-small "small"))
                             (recur r stack mid-small (conj stack-lst f) lst)
                             (recur r (conj stack f) "big" (conj stack-lst f) lst))
               (= f ")") (cond (and (= (count stack) 1) (= mid-small "small")) (recur r [] nil (conj stack-lst f) lst)
                               (and (> (count stack) 1) (= mid-small "small")) (recur r (pop stack) "small" (conj stack-lst f) lst)
                               (not (= mid-small "small")) (recur r stack mid-small (conj stack-lst f) lst)
                               )
               (= f "]") (cond (and (= (count stack) 1) (= mid-small "mid")) (recur r [] nil (conj stack-lst f) lst)
                               (and (> (count stack) 1) (= mid-small "mid")) (recur r (pop stack) "mid" (conj stack-lst f) lst)
                               (not (= mid-small "mid")) (recur r stack mid-small (conj stack-lst f) lst)
                               )
               (= f "}") (cond (and (= (count stack) 1) (= mid-small "big")) (recur r [] nil (conj stack-lst f) lst)
                               (and (> (count stack) 1) (= mid-small "big")) (recur r (pop stack) "big" (conj stack-lst f) lst)
                               (not (= mid-small "big")) (recur r stack mid-small (conj stack-lst f) lst)
                               )
               (= f ";") (if (and (nil? mid-small) (empty? stack) (not (empty? stack-lst)))
                             (recur nil [] nil [] (conj lst stack-lst (concat [";"] r)))
                             (recur r stack mid-small (conj stack-lst f) lst))
               :else
               (recur r stack mid-small (conj stack-lst f) lst)
               )
         (if-not (empty? stack-lst)
             (conj lst stack-lst)
             lst))))

(defn split-pair-item-ex
    ([lst] (split-pair-item-ex lst [] nil [] [] []))
    ([[f & r] stack mid-small stack-lst k-v lst]
     (letfn [(get-vs-pair [lst]
                 (loop [index (- (count lst) 1) stack-lst [] rs []]
                     (if (> index -1)
                         (let [f (nth lst index)]
                             (cond (= f "}") (recur -1 [] (conj rs (take (+ index 1) lst) (reverse stack-lst)))
                                   (= f ";") (recur -1 [] (conj rs (take (+ index 1) lst) (reverse stack-lst)))
                                   :else
                                   (recur (- index 1) (conj stack-lst f) rs)
                                   ))
                         rs)))]
         (if (some? f)
             (cond (= f "(") (if (or (= mid-small "mid") (= mid-small "big"))
                                 (recur r stack mid-small (conj stack-lst f) k-v lst)
                                 (recur r (conj stack f) "small" (conj stack-lst f) k-v lst))
                   (= f "[") (if (or (= mid-small "small") (= mid-small "big"))
                                 (recur r stack mid-small (conj stack-lst f) k-v lst)
                                 (recur r (conj stack f) "mid" (conj stack-lst f) k-v lst))
                   (= f "{") (if (or (= mid-small "mid") (= mid-small "small"))
                                 (recur r stack mid-small (conj stack-lst f) k-v lst)
                                 (recur r (conj stack f) "big" (conj stack-lst f) k-v lst))
                   (= f ")") (cond (and (= (count stack) 1) (= mid-small "small")) (recur r [] nil (conj stack-lst f) k-v lst)
                                   (and (> (count stack) 1) (= mid-small "small")) (recur r (pop stack) "small" (conj stack-lst f) k-v lst)
                                   (not (= mid-small "small")) (recur r stack mid-small (conj stack-lst f) k-v lst)
                                   )
                   (= f "]") (cond (and (= (count stack) 1) (= mid-small "mid")) (recur r [] nil (conj stack-lst f) k-v lst)
                                   (and (> (count stack) 1) (= mid-small "mid")) (recur r (pop stack) "mid" (conj stack-lst f) k-v lst)
                                   (not (= mid-small "mid")) (recur r stack mid-small (conj stack-lst f) k-v lst)
                                   )
                   (= f "}") (cond (and (= (count stack) 1) (= mid-small "big")) (recur r [] nil (conj stack-lst f) k-v lst)
                                   (and (> (count stack) 1) (= mid-small "big")) (recur r (pop stack) "big" (conj stack-lst f) k-v lst)
                                   (not (= mid-small "big")) (recur r stack mid-small (conj stack-lst f) k-v lst)
                                   )
                   (= f ":") (if (and (nil? mid-small) (empty? stack) (not (empty? stack-lst)))
                                 (if (empty? k-v)
                                     (recur r [] nil [] (conj k-v stack-lst) lst)
                                     (let [vs-pair (get-vs-pair stack-lst)]
                                         (if (empty? vs-pair)
                                             (recur r [] nil [] [] (conj lst (conj k-v stack-lst)))
                                             (if (empty? (last vs-pair))
                                                 (recur r [] nil [] [] (conj lst (conj k-v stack-lst)))
                                                 (recur r [] nil [] [(last vs-pair)] (conj lst (conj k-v (first vs-pair))))))))
                                 (recur r stack mid-small (conj stack-lst f) k-v lst))
                   (my-lexical/is-eq? f "else") (if (and (nil? mid-small) (empty? stack) (not (empty? stack-lst)))
                                                    (if-not (empty? k-v)
                                                        (recur r [] nil [] [["else"]] (conj lst (conj k-v stack-lst)))
                                                        (throw (Exception. "match 语句块中，不能只有 else")))
                                                    (recur r stack mid-small (conj stack-lst f) k-v lst))
                   :else
                   (recur r stack mid-small (conj stack-lst f) k-v lst)
                   )
             (if (and (not (empty? k-v)) (not (empty? stack-lst)))
                 (conj lst (conj k-v stack-lst))
                 lst)))
     ))

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

(defn get-args-dic
    ([lst] (get-args-dic lst {}))
    ([[f & r] dic]
     (if (some? f)
         (recur r (assoc dic f (str "cf_ps_" f)))
         dic)))

(defn my-convert-token [dic-ps token]
    (cond (map? token) (loop [[f & r] (keys token) my-token token]
                           (if (some? f)
                               (cond (and (= f :item_name) (contains? dic-ps (-> token :item_name))) (recur r (assoc my-token :item_name (get dic-ps (-> token :item_name))))
                                     (map? (-> token f)) (recur r (assoc my-token f (my-convert-token dic-ps (-> token f))))
                                     (and (my-lexical/is-seq? (-> token f)) (not (empty? (-> token f)))) (recur r (assoc my-token f (my-convert-token dic-ps (-> token f))))
                                     :else
                                     (recur r my-token)
                                     )
                               my-token))
          (and (my-lexical/is-seq? token) (not (empty? token))) (loop [[f & r] token lst []]
                                                                    (if (some? f)
                                                                        (recur r (conj lst (my-convert-token dic-ps f)))
                                                                        lst))
          :else
          token
          ))

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

; 定义变量初始化时用到
; 例如：这种复杂的形式
; let my = [1, 2+f(a), {"name": "wudafu", "age": g(b) + 10}];
(defn my-item-tokens [lst]
    (letfn [(get-items
                ([lst] (get-items lst [] nil [] []))
                ([[f & r] stack mid-small stack-lst lst]
                 (if (some? f)
                     (cond (= f "(") (if (or (= mid-small "mid") (= mid-small "big"))
                                         (recur r stack mid-small (conj stack-lst f) lst)
                                         (recur r (conj stack f) "small" (conj stack-lst f) lst))
                           (= f "[") (if (or (= mid-small "small") (= mid-small "big"))
                                         (recur r stack mid-small (conj stack-lst f) lst)
                                         (recur r (conj stack f) "mid" (conj stack-lst f) lst))
                           (= f "{") (if (or (= mid-small "mid") (= mid-small "small"))
                                         (recur r stack mid-small (conj stack-lst f) lst)
                                         (recur r (conj stack f) "big" (conj stack-lst f) lst))
                           (= f ")") (cond (and (= (count stack) 1) (= mid-small "small")) (recur r [] nil (conj stack-lst f) lst)
                                           (and (> (count stack) 1) (= mid-small "small")) (recur r (pop stack) "small" (conj stack-lst f) lst)
                                           (not (= mid-small "small")) (recur r stack mid-small (conj stack-lst f) lst)
                                           )
                           (= f "]") (cond (and (= (count stack) 1) (= mid-small "mid")) (recur r [] nil (conj stack-lst f) lst)
                                           (and (> (count stack) 1) (= mid-small "mid")) (recur r (pop stack) "mid" (conj stack-lst f) lst)
                                           (not (= mid-small "mid")) (recur r stack mid-small (conj stack-lst f) lst)
                                           )
                           (= f "}") (cond (and (= (count stack) 1) (= mid-small "big")) (recur r [] nil (conj stack-lst f) lst)
                                           (and (> (count stack) 1) (= mid-small "big")) (recur r (pop stack) "big" (conj stack-lst f) lst)
                                           (not (= mid-small "big")) (recur r stack mid-small (conj stack-lst f) lst)
                                           )
                           (= f ",") (if (and (nil? mid-small) (empty? stack) (not (empty? stack-lst)))
                                         (recur r [] nil [] (conj lst stack-lst))
                                         (recur r stack mid-small (conj stack-lst f) lst))
                           :else
                           (recur r stack mid-small (conj stack-lst f) lst)
                           )
                     (if-not (empty? stack-lst)
                         (conj lst stack-lst)
                         lst))))
            (get-items-dic
                ([lst] (get-items-dic lst [] nil [] [] []))
                ([[f & r] stack mid-small stack-lst k-v lst]
                 (if (some? f)
                     (cond (= f "(") (if (or (= mid-small "mid") (= mid-small "big"))
                                         (recur r stack mid-small (conj stack-lst f) k-v lst)
                                         (recur r (conj stack f) "small" (conj stack-lst f) k-v lst))
                           (= f "[") (if (or (= mid-small "small") (= mid-small "big"))
                                         (recur r stack mid-small (conj stack-lst f) k-v lst)
                                         (recur r (conj stack f) "mid" (conj stack-lst f) k-v lst))
                           (= f "{") (if (or (= mid-small "mid") (= mid-small "small"))
                                         (recur r stack mid-small (conj stack-lst f) k-v lst)
                                         (recur r (conj stack f) "big" (conj stack-lst f) k-v lst))
                           (= f ")") (cond (and (= (count stack) 1) (= mid-small "small")) (recur r [] nil (conj stack-lst f) k-v lst)
                                           (and (> (count stack) 1) (= mid-small "small")) (recur r (pop stack) "small" (conj stack-lst f) k-v lst)
                                           (not (= mid-small "small")) (recur r stack mid-small (conj stack-lst f) k-v lst)
                                           )
                           (= f "]") (cond (and (= (count stack) 1) (= mid-small "mid")) (recur r [] nil (conj stack-lst f) k-v lst)
                                           (and (> (count stack) 1) (= mid-small "mid")) (recur r (pop stack) "mid" (conj stack-lst f) k-v lst)
                                           (not (= mid-small "mid")) (recur r stack mid-small (conj stack-lst f) k-v lst)
                                           )
                           (= f "}") (cond (and (= (count stack) 1) (= mid-small "big")) (recur r [] nil (conj stack-lst f) k-v lst)
                                           (and (> (count stack) 1) (= mid-small "big")) (recur r (pop stack) "big" (conj stack-lst f) k-v lst)
                                           (not (= mid-small "big")) (recur r stack mid-small (conj stack-lst f) k-v lst)
                                           )
                           (= f ",") (if (and (nil? mid-small) (empty? stack) (not (empty? stack-lst)))
                                         (if (= (count k-v) 1)
                                             (recur r [] nil [] [] (conj lst (conj k-v stack-lst)))
                                             (throw (Exception. (format "字符串格式错误 %s" (str/join lst)))))
                                         (recur r stack mid-small (conj stack-lst f) k-v lst))
                           (= f ":") (if (and (nil? mid-small) (empty? stack) (not (empty? stack-lst)))
                                         (recur r [] nil [] (conj k-v stack-lst) lst)
                                         (recur r stack mid-small (conj stack-lst f) k-v lst))
                           :else
                           (recur r stack mid-small (conj stack-lst f) k-v lst)
                           )
                     (if (and (not (empty? stack-lst)) (not (empty? k-v)))
                         (conj lst (conj k-v stack-lst))
                         lst))))
            (kv-to-token [lst-dic]
                (loop [[f-dic & r-dic] lst-dic lst-kv []]
                    (if (some? f-dic)
                        (recur r-dic (conj lst-kv {:key (to-token (first f-dic)) :value (to-token (last f-dic))}))
                        lst-kv)))
            (to-token [vs]
                (cond (and (= (first vs) "[") (= (last vs) "]")) {:seq-obj (get-item-tokens (my-lexical/get-contain-lst vs))}
                      (and (= (first vs) "{") (= (last vs) "}")) {:map-obj (get-item-tokens (my-lexical/get-contain-lst vs))}
                      :else
                      (my-select-plus/sql-to-ast vs)
                      ))
            (get-item-tokens [lst]
                (loop [[f & r] (get-items lst) lst-rs []]
                    (if (some? f)
                        (cond (and (= (first f) "[") (= (last f) "]")) (recur r (conj lst-rs {:seq-obj (get-item-tokens (my-lexical/get-contain-lst f))}))
                              (and (= (first f) "{") (= (last f) "}")) (let [lst-dic (get-items-dic (my-lexical/get-contain-lst f))]
                                                                           (recur r (conj lst-rs {:map-obj (kv-to-token lst-dic)})))
                              (and (= (first f) "-") (not (empty? (rest f)))) (recur r (conj lst-rs (my-select-plus/sql-to-ast (concat ["0"] f))))
                              :else
                              (recur r (conj lst-rs (my-select-plus/sql-to-ast f))))
                        (if (= (count lst-rs) 1)
                            (first lst-rs)
                            lst-rs))))]
        (cond (and (= (first lst) "[") (= (last lst) "]")) (get-item-tokens lst)
              (and (= (first lst) "{") (= (last lst) "}")) (get-item-tokens lst)
              :else
              (my-select-plus/sql-to-ast lst))))

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
                                                                        (my-lexical/is-seq? (-> my-last-item :pair-vs)) (let [new-peek (assoc my-last-item :pair-vs (conj (-> my-last-item :pair-vs) f))]
                                                                                                                                                        (recur r (conj (pop lst) new-peek)))
                                                                        :else
                                                                        (throw (Exception. "match 语句块语法错误！"))
                                                                        )
                                   (contains? my-last-item :else-vs) (cond (map? (-> my-last-item :else-vs)) (let [new-peek (assoc my-last-item :else-vs [(-> my-last-item :else-vs) f])]
                                                                                                                 (recur r (conj (pop lst) new-peek)))
                                                                           (my-lexical/is-seq? (-> my-last-item :else-vs)) (let [new-peek (assoc my-last-item :else-vs (conj (-> my-last-item :else-vs) f))]
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
        {:tmp_val (my-item-tokens [(first lst)]) :seq (my-item-tokens (rest (rest lst)))}))


(defn lst-to-token [lst]
    (cond (and (my-lexical/is-eq? (first lst) "let") (= (second (rest lst)) "=")) (let [my-let-vs (my-item-tokens (rest (rest (rest lst))))]
                                                                                      {:let-name (second lst) :let-vs my-let-vs})
          (and (my-lexical/is-eq? (first lst) "let") (= (count lst) 2)) {:let-name (second lst) :let-vs nil}
          (my-lexical/is-eq? (first lst) "else") (if (my-lexical/is-eq? (second lst) "break")
                                                     {:else-vs {:break-vs true}}
                                                     {:else-vs (my-item-tokens (rest lst))})
          (my-lexical/is-eq? (first lst) "break") {:break-vs true}
          :else
          {:express (my-item-tokens lst)}
          ;(let [pair-item (split-pair-item lst)]
          ;    (cond (= (count pair-item) 2) {:pair (my-select-plus/sql-to-ast (first pair-item)) :pair-vs (my-select-plus/sql-to-ast (second pair-item))}
          ;          (= (count pair-item) 1) {:express (my-select-plus/sql-to-ast (first pair-item))}
          ;          :else
          ;          (throw (Exception. "match 中的判断要成对出现！"))
          ;          ))
          ))


(defn get-re-pair
    ([lst] (get-re-pair lst []))
    ([[f & r] lst-rs]
     (if (some? f)
         (let [rs-pair (get-pair-item-ex (reverse f))]
             (if (= (count rs-pair) 2)
                 (recur r (conj lst-rs (reverse (second rs-pair)) (reverse (first rs-pair))))
                 (recur r (conj lst-rs f))))
         lst-rs)))

(defn get-pairs [lst]
    (if (even? (count lst))
        (loop [[f & r] lst stack [] lst-rs []]
            (if (some? f)
                (if-not (= (count stack) 2)
                    (recur r (conj stack f) lst-rs)
                    (recur r (conj [] f) (conj lst-rs stack)))
                (if-not (empty? stack)
                    (conj lst-rs stack)
                    lst-rs)))
        (throw (Exception. "match 里面的语句要成对出现！"))))

(defn get-pairs-tokens [lst]
    (loop [[f & r] lst lst-rs []]
        (if (some? f)
            (if (and (= (count (first f)) 1) (my-lexical/is-eq? (first (first f)) "else"))
                (recur r (conj lst-rs {:else-vs (body-segment (second f))}))
                (let [pv (body-segment (second f))]
                    (recur r (conj lst-rs {:pair (my-item-tokens (first f)) :pair-vs pv})))
                )
            lst-rs)))

(defn body-segment
    ([lst] (body-segment lst [] []))
    ([[f & r] stack-lst lst]
     (if (some? f)
         (cond (and (empty? stack-lst) (my-lexical/is-eq? f "for") (= (first r) "(")) (let [{args-lst :args-lst body-lst :body-lst} (get-small r)]
                                                                                          (if-not (empty? body-lst)
                                                                                              (let [{big-lst :big-lst rest-lst :rest-lst} (get-big body-lst)]
                                                                                                  (recur rest-lst [] (conj lst {:expression "for" :args (get-for-in-args args-lst) :body (body-segment big-lst)})))))
               (and (empty? stack-lst) (my-lexical/is-eq? f "match") (= (first r) "{")) (let [{big-lst :big-lst rest-lst :rest-lst} (get-big r)]
                                                                                            (recur rest-lst [] (conj lst {:expression "match" :pairs (get-pairs-tokens (split-pair-item-ex big-lst))}))
                                                                                            )
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

(defn re-body-lst [args-lst body-lst]
    (loop [[f & r] args-lst new-args-lst [] new-body-lst []]
        (if (some? f)
            (let [ps (str (gensym (format "c_%s_f" f)))]
                (recur r (conj new-args-lst ps ) (conj new-body-lst {:let-name f, :let-vs {:table_alias "", :item_name ps, :item_type "", :java_item_type nil, :const false}})))
            [new-args-lst (concat new-body-lst body-lst)])))

(defn re-ast
    ([lst] (re-ast lst []))
    ([[f & r] lst]
     (if (some? f)
         (let [[new-args-lst new-body-lst] (re-body-lst (-> f :args-lst) (-> f :body-lst))]
             (recur r (conj lst (assoc f :args-lst new-args-lst :body-lst new-body-lst))))
         lst)))

(defn get-ast [^String sql]
    (if-let [lst (my-lexical/to-back sql)]
        ;(get-ast-lst lst)
        (re-ast (get-ast-lst lst))
        ))

;(defn get-ast [^String sql]
;    (if-let [lst (my-lexical/to-back sql)]
;        (loop [[f & r] (get-ast-lst lst) lst-rs []]
;            (if (some? f)
;                (let [arg-dic (get-args-dic (-> f :args-lst))]
;                    (recur r (conj lst-rs (assoc f :args-lst (vals arg-dic) :body-lst (my-convert-token arg-dic (-> f :body-lst))))))
;                lst-rs))
;        ))










































