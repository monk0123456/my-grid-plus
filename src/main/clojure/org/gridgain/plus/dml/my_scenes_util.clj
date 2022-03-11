(ns org.gridgain.plus.dml.my-scenes-util
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select]
        [org.gridgain.plus.dml.my-insert :as my-insert]
        [org.gridgain.plus.dml.my-update :as my-update]
        [org.gridgain.plus.dml.my-delete :as my-delete]
        [org.gridgain.plus.dml.my-expression :as my-expression]
        [org.gridgain.plus.context.my-context :as my-context]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import
             (cn.plus.model.db MyScenesCache MyScenesParams MyScenesParamsPk)
             (org.tools MyConvertUtil)
             (java.util ArrayList List Date Iterator)
             )
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyScenesUtil
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

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

(defn get_contain [lst]
    (loop [index 1 rs []]
        (if (< (+ index 1) (count lst))
            (recur (+ index 1) (conj rs (nth lst index)))
            rs)))

; ["{" "param_name:" "'参数名字'" "," "param_type:" "'String'" "}"]
;(defn get_param_item
;    ([lst] (get_param_item lst [] [] {}))
;    ([[f & r] stack_name stack_value dic]
;     (if (some? f)
;         (cond (re-find #"^(?i)param_name\s*\:\s*$" f) (recur r (conj stack_name f) [] dic)
;               (re-find #"^(?i)param_type\s*\:\s*$" f) (recur r [] (conj stack_value f) dic)
;               (re-find #"^(?i)\s*,\s*$" f) (if-not (empty? stack_name)
;                                                (recur r [] [] (assoc dic :param_name (last stack_name)))
;                                                (throw (Exception. "tran 语句错误！")))
;               :else
;               (if (contains? dic :param_name)
;                   (recur r [] (conj stack_value f) dic)
;                   (recur r (conj stack_name f) [] dic))
;               )
;         (if-not (empty? stack_value)
;             (assoc dic :param_type (last stack_value))
;             (throw (Exception. "tran 语句错误！"))))))

; (def params '("{" "param_name:" "'参数名字'" "," "param_type:" "'String'" "}" "," "{" "param_name:" "a" "," "param_type:" "Double" "}"))
; (get_params params)
(defn get_params
    ([lst] (get_params lst [] [] []))
    ([[f & r] stack_big stack lst]
     (letfn [(get_param_item
                 ([lst] (get_param_item lst [] [] {}))
                 ([[f & r] stack_name stack_value dic]
                  (if (some? f)
                      (cond (re-find #"^(?i)param_name\s*\:\s*$" f) (recur r (conj stack_name f) [] dic)
                            (re-find #"^(?i)param_type\s*\:\s*$" f) (recur r [] (conj stack_value f) dic)
                            (re-find #"^(?i)\s*,\s*$" f) (if-not (empty? stack_name)
                                                             (recur r [] [] (assoc dic :param_name (my-lexical/get_str_value (last stack_name))))
                                                             (throw (Exception. "tran 语句错误！")))
                            :else
                            (if (contains? dic :param_name)
                                (recur r [] (conj stack_value f) dic)
                                (recur r (conj stack_name f) [] dic))
                            )
                      (if-not (empty? stack_value)
                          (assoc dic :param_type (my-lexical/get_str_value (last stack_value)))
                          (throw (Exception. "tran 语句错误！"))))))]
         (if (some? f)
             (cond (= f "{") (recur r (conj stack_big f) stack lst)
                   (= f "}") (if (= (count stack_big) 1)
                                 (recur r [] [] (conj lst (get_param_item stack)))
                                 (throw (Exception. "tran 语句错误！")))
                   (= f ",") (if (> (count stack_big) 0)
                                 (recur r stack_big (conj stack f) lst)
                                 (recur r stack_big stack lst))
                   :else
                   (recur r stack_big (conj stack f) lst)
                   )
             (if-not (empty? stack)
                 (conj lst (get_param_item stack))
                 lst)))
     ))

(defn get_line
    ([lst] (get_line lst [] []))
    ([[f & r] stack lst]
     (if (some? f)
         (cond (contains? #{\[ \] \{ \}} f) (if-not (empty? stack)
                                                (recur r [] (concat lst [(str/join stack) (.toString f)]))
                                                (recur r [] (concat lst [(.toString f)])))
               :else
               (recur r (conj stack (.toString f)) lst)
               )
         (if (empty? stack)
             lst
             (concat lst [(str/join stack)])))))

(defn get_trans_lst [lst]
    (if (and (= (first lst) "{") (= (last lst) "}"))
        (loop [i 1 rs []]
            (if (< (+ i 1) (count lst))
                (recur (+ i 1) (concat rs (get_line (nth lst i))))
                rs))))

(defn get_value [stack lst]
    (cond (and (not (empty? stack)) (empty? lst)) stack
          (and (not (empty? lst)) (empty? stack)) lst
          :else
          lst
          ))

(defn map_reduce_segment
    ([lst] (map_reduce_segment (get_trans_lst lst) [] [] [] [] [] [] []))
    ([[f & r] stack_small stack_mid stack_big stack_map lst_map stack_reduce lst_reduce]
     (if (some? f)
         (cond (my-lexical/is-eq? f "(") (cond (not (empty? stack_map)) (recur r (conj stack_small f) stack_mid stack_big (conj stack_map f) lst_map stack_reduce lst_reduce)
                                               (not (empty? stack_reduce)) (recur r (conj stack_small f) stack_mid stack_big stack_map lst_map (conj stack_reduce f) lst_reduce)
                                               )
               (my-lexical/is-eq? f ")") (cond (not (empty? stack_map)) (recur r (pop stack_small) stack_mid stack_big (conj stack_map f) lst_map stack_reduce lst_reduce)
                                               (not (empty? stack_reduce)) (recur r (pop stack_small) stack_mid stack_big stack_map lst_map (conj stack_reduce f) lst_reduce)
                                               )
               (my-lexical/is-eq? f "[") (cond (not (empty? stack_map)) (recur r stack_small (conj stack_mid f) stack_big (conj stack_map f) lst_map stack_reduce lst_reduce)
                                               (not (empty? stack_reduce)) (recur r stack_small (conj stack_mid f) stack_big stack_map lst_map (conj stack_reduce f) lst_reduce)
                                               )
               (my-lexical/is-eq? f "]") (cond (not (empty? stack_map)) (recur r stack_small (pop stack_mid) stack_big (conj stack_map f) lst_map stack_reduce lst_reduce)
                                               (not (empty? stack_reduce)) (recur r stack_small (pop stack_mid) stack_big stack_map lst_map (conj stack_reduce f) lst_reduce)
                                               )
               (my-lexical/is-eq? f "{") (cond (not (empty? stack_map)) (recur r stack_small stack_mid (conj stack_big f) (conj stack_map f) lst_map stack_reduce lst_reduce)
                                               (not (empty? stack_reduce)) (recur r stack_small stack_mid (conj stack_big f) stack_map lst_map (conj stack_reduce f) lst_reduce)
                                               )
               (my-lexical/is-eq? f "}") (cond (not (empty? stack_map)) (recur r stack_small stack_mid (pop stack_big) (conj stack_map f) lst_map stack_reduce lst_reduce)
                                               (not (empty? stack_reduce)) (recur r stack_small stack_mid (pop stack_big) stack_map lst_map (conj stack_reduce f) lst_reduce)
                                               )

               (re-find #"^(?i)map\s*\:\s*$" f) (cond (and (empty? stack_map) (empty? stack_reduce)) (recur r stack_small stack_mid stack_big (conj stack_map f) lst_map stack_reduce lst_reduce)
                                                      (and (empty? stack_map) (not (empty? stack_reduce))) (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                               (recur r stack_small stack_mid stack_big (conj stack_map f) lst_map [] stack_reduce)
                                                                                                               (recur r stack_small stack_mid stack_big stack_map lst_map (conj stack_reduce f) lst_reduce))
                                                      :else
                                                      (throw (Exception. "map, reduce 语句错误！")))
               (re-find #"^(?i)reduce\s*\:\s*$" f) (cond (and (empty? stack_map) (empty? stack_reduce)) (recur r stack_small stack_mid stack_big stack_map lst_map (conj stack_reduce f) lst_reduce)
                                                         (and (not (empty? stack_map)) (empty? stack_reduce)) (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                                                                                  (recur r stack_small stack_mid stack_big [] stack_map (conj stack_reduce f) lst_reduce)
                                                                                                                  (recur r stack_small stack_mid stack_big (conj stack_map f) lst_map stack_reduce lst_reduce))
                                                         :else
                                                         (throw (Exception. "map, reduce 语句错误！")))
               :else
               (cond (not (empty? stack_map)) (recur r stack_small stack_mid stack_big (conj stack_map f) lst_map stack_reduce lst_reduce)
                     (not (empty? stack_reduce)) (recur r stack_small stack_mid stack_big stack_map lst_map (conj stack_reduce f) lst_reduce)
                     )
               )
         (letfn [(get_vs [[f & r]]
                     (if (and (some? r) (= (last r) ","))
                         (reverse (rest (reverse r)))
                         r))]
             (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                 {:map (get_vs (get_value stack_map lst_map)) :reduce (get_vs (get_value stack_reduce lst_reduce))}
                 (throw (Exception. "map, reduce 语句错误！"))))
         )))

(defn tran_segment
    ([lst] (tran_segment (get_trans_lst lst) [] [] [] [] [] [] [] [] [] [] [] [] []))
    ([[f & r] stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch]
     (if (some? f)
         (cond (my-lexical/is-eq? f "(") (cond (not (empty? stack_name)) (recur r (conj stack_small f) stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f ")") (cond (not (empty? stack_name)) (recur r (pop stack_small) stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "[") (cond (not (empty? stack_name)) (recur r stack_small (conj stack_mid f) stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "]") (cond (not (empty? stack_name)) (recur r stack_small (pop stack_mid) stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "{") (cond (not (empty? stack_name)) (recur r stack_small stack_mid (conj stack_big f) (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "}") (cond (not (empty? stack_name)) (recur r stack_small stack_mid (pop stack_big) (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )

               (re-find #"^(?i)name\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                       (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name [] stack_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params [] stack_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans [] stack_descrip stack_batch lst_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip [] stack_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                       :else
                                                       (throw (Exception. "tran 语句错误！")))
               (re-find #"^(?i)params\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                         (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                         (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                             (recur r stack_small stack_mid stack_big [] stack_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                             (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                         (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                         (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params [] stack_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                         (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                         (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans [] stack_descrip stack_batch lst_batch)
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                         (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                         (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip [] stack_batch)
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                         :else
                                                         (throw (Exception. "tran 语句错误！")))
               (re-find #"^(?i)trans\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                        (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                        (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                            (recur r stack_small stack_mid stack_big [] stack_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                            (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                        (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                        (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name [] stack_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                        (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                        (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans [] stack_descrip stack_batch lst_batch)
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                        (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                        (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip [] stack_batch)
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                        :else
                                                        (throw (Exception. "tran 语句错误！")))

               (re-find #"^(?i)descrip\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                          (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                          (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                              (recur r stack_small stack_mid stack_big [] stack_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                              (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                          (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                          (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name [] stack_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                          (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                          (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params [] stack_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                          (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                          (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip [] stack_batch)
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                          :else
                                                          (throw (Exception. "tran 语句错误！")))

               (re-find #"^(?i)is_batch\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                           (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                           (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                               (recur r stack_small stack_mid stack_big [] stack_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                               (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                           (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                           (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                               (recur r stack_small stack_mid stack_big stack_name lst_name [] stack_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                               (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                           (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                           (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                               (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params [] stack_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                               (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                           (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                           (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                               (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans [] stack_descrip (conj stack_batch f) lst_batch)
                                                               (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                           :else
                                                           (throw (Exception. "tran 语句错误！")))
               :else
               (cond (not (empty? stack_name)) (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                     (not (empty? stack_params)) (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                     (not (empty? stack_trans)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                     (not (empty? stack_descrip)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                     (not (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                     )
               )
         (letfn [(get_vs [[f & r]]
                     (if (and (some? r) (= (last r) ","))
                         (reverse (rest (reverse r)))
                         r))
                 (get_value [stack lst]
                     (cond (and (not (empty? stack)) (empty? lst)) stack
                           (and (not (empty? lst)) (empty? stack)) lst
                           :else
                           lst
                           ))]
             (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                 {:name (get_vs (get_value stack_name lst_name)) :params (get_vs (get_value stack_params lst_params)) :trans (get_vs (get_value stack_trans lst_trans)) :descrip (get_vs (get_value stack_descrip lst_descrip)) :is_batch (get_vs (get_value stack_batch lst_batch))}
                 (throw (Exception. "tran 语句错误！"))))
         )))

; scenes 的 segment
(defn scenes_segment
    ([lst] (scenes_segment (get_trans_lst lst) [] [] [] [] [] [] [] [] [] [] [] [] []))
    ([[f & r] stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch]
     (if (some? f)
         (cond (my-lexical/is-eq? f "(") (cond (not (empty? stack_name)) (recur r (conj stack_small f) stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f ")") (cond (not (empty? stack_name)) (recur r (pop stack_small) stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "[") (cond (not (empty? stack_name)) (recur r stack_small (conj stack_mid f) stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "]") (cond (not (empty? stack_name)) (recur r stack_small (pop stack_mid) stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "{") (cond (not (empty? stack_name)) (recur r stack_small stack_mid (conj stack_big f) (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "}") (cond (not (empty? stack_name)) (recur r stack_small stack_mid (pop stack_big) (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )

               (re-find #"^(?i)name\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                       (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name [] stack_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params [] stack_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans [] stack_descrip stack_batch lst_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip [] stack_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                       :else
                                                       (throw (Exception. "场景定义语句错误！")))
               (re-find #"^(?i)params\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                         (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                         (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                             (recur r stack_small stack_mid stack_big [] stack_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                             (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                         (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                         (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params [] stack_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                         (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                         (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans [] stack_descrip stack_batch lst_batch)
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                         (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                         (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip [] stack_batch)
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                         :else
                                                         (throw (Exception. "场景定义语句错误！")))
               (re-find #"^(?i)sql\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                      (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                      (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                          (recur r stack_small stack_mid stack_big [] stack_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                          (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                      (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                      (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                          (recur r stack_small stack_mid stack_big stack_name lst_name [] stack_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                          (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                      (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                      (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                          (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans [] stack_descrip stack_batch lst_batch)
                                                          (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                      (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                      (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                          (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip [] stack_batch)
                                                          (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                      :else
                                                      (throw (Exception. "tran 语句错误！")))

               (re-find #"^(?i)descrip\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                          (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                          (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                              (recur r stack_small stack_mid stack_big [] stack_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                              (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                          (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                          (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name [] stack_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                          (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                          (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params [] stack_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                          (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                          (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip [] stack_batch)
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                          :else
                                                          (throw (Exception. "场景定义语句错误！")))

               (re-find #"^(?i)is_batch\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                           (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                           (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                               (recur r stack_small stack_mid stack_big [] stack_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                               (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                           (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                           (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                               (recur r stack_small stack_mid stack_big stack_name lst_name [] stack_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                               (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                           (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                           (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                               (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params [] stack_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                               (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                           (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                           (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                               (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans [] stack_descrip (conj stack_batch f) lst_batch)
                                                               (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                           :else
                                                           (throw (Exception. "场景定义语句错误！")))
               :else
               (cond (not (empty? stack_name)) (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                     (not (empty? stack_params)) (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                     (not (empty? stack_trans)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                     (not (empty? stack_descrip)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                     (not (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                     )
               )
         (letfn [(get_vs [[f & r]]
                     (if (and (some? r) (= (last r) ","))
                         (reverse (rest (reverse r)))
                         r))
                 (get_value [stack lst]
                     (cond (and (not (empty? stack)) (empty? lst)) stack
                           (and (not (empty? lst)) (empty? stack)) lst
                           :else
                           lst
                           ))]
             (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                 {:name (get_vs (get_value stack_name lst_name)) :params (get_vs (get_value stack_params lst_params)) :sql (get_vs (get_value stack_trans lst_trans)) :descrip (get_vs (get_value stack_descrip lst_descrip)) :is_batch (get_vs (get_value stack_batch lst_batch))}
                 (throw (Exception. "场景定义语句错误！"))))
         )))

; cron 的 segment
(defn cron_segment
    ([lst] (cron_segment (get_trans_lst lst) [] [] [] [] [] [] [] [] [] [] [] [] []))
    ([[f & r] stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch]
     (if (some? f)
         (cond (my-lexical/is-eq? f "(") (cond (not (empty? stack_name)) (recur r (conj stack_small f) stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r (conj stack_small f) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f ")") (cond (not (empty? stack_name)) (recur r (pop stack_small) stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r (pop stack_small) stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "[") (cond (not (empty? stack_name)) (recur r stack_small (conj stack_mid f) stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small (conj stack_mid f) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "]") (cond (not (empty? stack_name)) (recur r stack_small (pop stack_mid) stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small (pop stack_mid) stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "{") (cond (not (empty? stack_name)) (recur r stack_small stack_mid (conj stack_big f) (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small stack_mid (conj stack_big f) stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )
               (my-lexical/is-eq? f "}") (cond (not (empty? stack_name)) (recur r stack_small stack_mid (pop stack_big) (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_params)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_trans)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_descrip)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                               (not (empty? stack_batch)) (recur r stack_small stack_mid (pop stack_big) stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                               )

               (re-find #"^(?i)name\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                       (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name [] stack_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params [] stack_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans [] stack_descrip stack_batch lst_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip [] stack_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                       :else
                                                       (throw (Exception. "批处理定义错误！")))
               (re-find #"^(?i)params\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                         (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                         (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                             (recur r stack_small stack_mid stack_big [] stack_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                             (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                         (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                         (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params [] stack_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                         (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                         (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans [] stack_descrip stack_batch lst_batch)
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                         (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                         (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip [] stack_batch)
                                                             (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                         :else
                                                         (throw (Exception. "批处理定义错误！")))
               (re-find #"^(?i)batch\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                        (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                        (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                            (recur r stack_small stack_mid stack_big [] stack_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                            (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                        (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                        (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name [] stack_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                        (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                        (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans [] stack_descrip stack_batch lst_batch)
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                        (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                        (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip [] stack_batch)
                                                            (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                        :else
                                                        (throw (Exception. "批处理定义错误！")))

               (re-find #"^(?i)descrip\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                          (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                          (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                              (recur r stack_small stack_mid stack_big [] stack_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                              (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                          (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                          (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name [] stack_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                          (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                          (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params [] stack_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                          (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (not (empty? stack_batch)))
                                                          (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip [] stack_batch)
                                                              (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch))
                                                          :else
                                                          (throw (Exception. "批处理定义错误！")))

               (re-find #"^(?i)cron\s*\:\s*$" f) (cond (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                       (and (not (empty? stack_name)) (empty? stack_params) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big [] stack_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                           (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (not (empty? stack_params)) (empty? stack_trans) (empty? stack_descrip) (empty? stack_batch))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name [] stack_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (empty? stack_params) (not (empty? stack_trans)) (empty? stack_descrip) (empty? stack_batch))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params [] stack_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch))
                                                       (and (empty? stack_name) (empty? stack_params) (empty? stack_trans) (not (empty? stack_descrip)) (empty? stack_batch))
                                                       (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans [] stack_descrip (conj stack_batch f) lst_batch)
                                                           (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch))
                                                       :else
                                                       (throw (Exception. "批处理定义错误！")))
               :else
               (cond (not (empty? stack_name)) (recur r stack_small stack_mid stack_big (conj stack_name f) lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                     (not (empty? stack_params)) (recur r stack_small stack_mid stack_big stack_name lst_name (conj stack_params f) lst_params stack_trans lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                     (not (empty? stack_trans)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params (conj stack_trans f) lst_trans stack_descrip lst_descrip stack_batch lst_batch)
                     (not (empty? stack_descrip)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans (conj stack_descrip f) lst_descrip stack_batch lst_batch)
                     (not (empty? stack_batch)) (recur r stack_small stack_mid stack_big stack_name lst_name stack_params lst_params stack_trans lst_trans stack_descrip lst_descrip (conj stack_batch f) lst_batch)
                     )
               )
         (letfn [(get_vs [[f & r]]
                     (if (and (some? r) (= (last r) ","))
                         (reverse (rest (reverse r)))
                         r))
                 (get_value [stack lst]
                     (cond (and (not (empty? stack)) (empty? lst)) stack
                           (and (not (empty? lst)) (empty? stack)) lst
                           :else
                           lst
                           ))]
             (if (and (empty? stack_small) (empty? stack_mid) (empty? stack_big))
                 (let [{my-map :map reduce :reduce} (map_reduce_segment (get_vs (get_value stack_trans lst_trans)))]
                     ;(println (get_vs (get_value stack_trans lst_trans)))
                     (let [my-map-1 (filter #(not (string? %)) (my-comma-fn (reverse (rest (reverse (rest my-map))))))]
                         {:name (get_vs (get_value stack_name lst_name)) :params (get_vs (get_value stack_params lst_params)) :batch {:map my-map-1 :reduce reduce} :descrip (get_vs (get_value stack_descrip lst_descrip)) :cron (get_vs (get_value stack_batch lst_batch))}))
                 ;{:name (get_vs (get_value stack_name lst_name)) :params (get_vs (get_value stack_params lst_params)) :batch (get_vs (get_value stack_trans lst_trans)) :descrip (get_vs (get_value stack_descrip lst_descrip)) :cron (get_vs (get_value stack_batch lst_batch))}
                 (throw (Exception. "批处理定义错误！"))))
         )))

(defn to_params [lst_params]
    (loop [[f & r] lst_params index 0 lst (ArrayList.)]
        (if (some? f)
            (recur r (+ index 1) (doto lst (.add (MyScenesParams. (-> f :param_name) (-> f :param_type) (MyConvertUtil/ConvertToInt (+ index 1))))))
            lst)
        ))

; 获取 tran obj
(defn tran_obj [lst-sql]
    (let [{name :name params :params trans :trans descrip :descrip is_batch :is_batch} (tran_segment lst-sql)]
        (try
            {:name (my-lexical/get_str_value (first name)) :params (to_params (get_params (get_contain params))) :trans trans :descrip descrip :is_batch is_batch}
            (catch Exception ex
                nil))
        ))

; 获取 scenes obj
(defn scenes_obj [lst-sql]
    (let [{name :name params :params sql :sql descrip :descrip is_batch :is_batch} (scenes_segment lst-sql)]
        (try
            {:name (my-lexical/get_str_value (first name)) :params (to_params (get_params (get_contain params))) :sql sql :descrip descrip :is_batch is_batch}
            (catch Exception ex
                nil))
        ))

; 获取 cron obj
(defn cron_obj [lst-sql]
    (let [{name :name params :params batch :batch descrip :descrip cron :cron} (cron_segment lst-sql)]
        (try
            {:name (my-lexical/get_str_value (first name)) :params (to_params (get_params (get_contain params))) :batch batch :descrip descrip :cron cron}
            (catch Exception ex
                nil))
        ))

; 获取场景对象
(defn my_scenes_obj [lst-sql]
    (if-let [s_obj (scenes_obj lst-sql)]
        (if-not (empty? (-> s_obj :sql))
            {:scenes_type "scenes" :obj s_obj}
            (if-let [t_obj (tran_obj lst-sql)]
                (if-not (empty? (-> t_obj :trans))
                    {:scenes_type "tran" :obj t_obj}
                    (if-let [c_obj (cron_obj lst-sql)]
                        {:scenes_type "cron" :obj c_obj})
                    )
                (if-let [c_obj (cron_obj lst-sql)]
                    {:scenes_type "cron" :obj c_obj})))
        (if-let [t_obj (tran_obj lst-sql)]
            (if-not (empty? (-> t_obj :trans))
                {:scenes_type "tran" :obj t_obj}
                (if-let [c_obj (cron_obj lst-sql)]
                    {:scenes_type "cron" :obj c_obj})
                )
            (if-let [c_obj (cron_obj lst-sql)]
                {:scenes_type "cron" :obj c_obj}))
        ))




















































