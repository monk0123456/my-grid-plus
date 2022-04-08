(ns org.gridgain.plus.dml.my-expression
    (:require
        [org.gridgain.plus.dml.select-lexical :as my-lexical]
        [org.gridgain.plus.dml.my-select-plus :as my-select-plus]
        [clojure.core.reducers :as r]
        [clojure.string :as str])
    (:import (org.apache.ignite Ignite IgniteCache)
             (org.apache.ignite.internal IgnitionEx)
             (com.google.common.base Strings)
             (org.log MyCljLogger)
             (org.tools MyConvertUtil KvSql)
             (java.math BigDecimal)
             (org.gridgain.myservice MyScenesService)
             (org.apache.ignite.binary BinaryObjectBuilder BinaryObject)
             (java.util ArrayList Date Iterator)
             (java.sql Timestamp))
    (:gen-class
        ; 生成 class 的类名
        :name org.gridgain.plus.dml.MyExpression
        ; 是否生成 class 的 main 方法
        :main false
        ; 生成 java 静态的方法
        ;:methods [^:static [getPlusInsert [org.apache.ignite.Ignite Long String] clojure.lang.PersistentArrayMap]]
        ))

(declare func_eval mid_to_forwrod func_lst_ps_eval func_lst_ps_eval_binaryObject func_eval_binary mid_to_forwrod_binaryObject
         get_plus_value_tokens get_value_tokens get_plus_value_tokens_fun get_plus_value get_plus_value_fun
         get_value_tokens_fun mid_to_forwrod_fun item_type_binaryObj_fun)

(defn str_item_value [item_value]
    (if (or (and (= (first item_value) \') (= (last item_value) \')) (and (= (first item_value) \") (= (last item_value) \")))
        (str/join (reverse (rest (reverse (rest item_value)))))))

(defn item_type [java_item_type item_value]
    (cond (= Integer java_item_type) (MyConvertUtil/ConvertToInt item_value)
          (= String java_item_type) (str_item_value item_value)
          (= Boolean java_item_type) (MyConvertUtil/ConvertToBoolean item_value)
          (= Long java_item_type) (MyConvertUtil/ConvertToLong item_value)
          (= Float java_item_type) (MyConvertUtil/ConvertToDouble item_value)
          (= Double java_item_type) (MyConvertUtil/ConvertToDouble item_value)
          (= Timestamp java_item_type) (MyConvertUtil/ConvertToTimestamp item_value)
          (= BigDecimal java_item_type) (MyConvertUtil/ConvertToDecimal item_value)
          (= "byte[]" java_item_type) (MyConvertUtil/ConvertToByte item_value)
          (= Object java_item_type) item_value
          ))

(defn get_value_tokens [^Ignite ignite ^Long group_id vs_tokens]
    (cond (and (= String (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (my-lexical/get_str_value (-> vs_tokens :item_name))
          (and (= Integer (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToInt (my-lexical/get_str_value (-> vs_tokens :item_name)))
          (and (= Boolean (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToBoolean (my-lexical/get_str_value (-> vs_tokens :item_name)))
          (and (= Long (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToLong (my-lexical/get_str_value (-> vs_tokens :item_name)))
          (and (= Float (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToDouble (my-lexical/get_str_value (-> vs_tokens :item_name)))
          (and (= Double (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToDouble (my-lexical/get_str_value (-> vs_tokens :item_name)))
          (and (= Timestamp (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToTimestamp (my-lexical/get_str_value (-> vs_tokens :item_name)))
          (and (= BigDecimal (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToDecimal (my-lexical/get_str_value (-> vs_tokens :item_name)))
          (contains? vs_tokens :operation) (-> (first (mid_to_forwrod ignite group_id (reverse (-> vs_tokens :operation)))) :express)
          (contains? vs_tokens :parenthesis) (-> (first (mid_to_forwrod ignite group_id (reverse (-> vs_tokens :parenthesis)))) :express)
          (contains? vs_tokens :func-name) (-> (first (mid_to_forwrod ignite group_id [vs_tokens])) :express)
          :else
          (throw (Exception. "数据类型出错！"))
          ))

(defn get_value_tokens_fun [^Ignite ignite ^Long group_id vs_tokens ^clojure.lang.PersistentArrayMap dic_paras]
    (cond (and (= String (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (my-lexical/get_str_value (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
          (and (= Integer (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToInt (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
          (and (= Boolean (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToBoolean (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
          (and (= Long (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToLong (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
          (and (= Float (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToDouble (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
          (and (= Double (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToDouble (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
          (and (= Timestamp (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToTimestamp (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
          (and (= BigDecimal (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToDecimal (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
          (contains? vs_tokens :operation) (-> (first (mid_to_forwrod_fun ignite group_id dic_paras (reverse (-> vs_tokens :operation)))) :express)
          (contains? vs_tokens :parenthesis) (-> (first (mid_to_forwrod_fun ignite group_id dic_paras (reverse (-> vs_tokens :parenthesis)))) :express)
          (contains? vs_tokens :func-name) (-> (first (mid_to_forwrod_fun ignite group_id dic_paras [vs_tokens])) :express)
          :else
          (throw (Exception. "数据类型出错！"))
          ))

(defn get_plus_value_tokens [^Ignite ignite ^Long group_id ^String vs_line]
    (if-let [vs_tokens (my-select-plus/sql-to-ast (my-lexical/to-back vs_line))]
        (cond (and (= String (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (my-lexical/get_str_value (-> vs_tokens :item_name))
              (and (= Integer (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToInt (my-lexical/get_str_value (-> vs_tokens :item_name)))
              (and (= Boolean (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToBoolean (my-lexical/get_str_value (-> vs_tokens :item_name)))
              (and (= Long (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToLong (my-lexical/get_str_value (-> vs_tokens :item_name)))
              (and (= Float (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToDouble (my-lexical/get_str_value (-> vs_tokens :item_name)))
              (and (= Double (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToDouble (my-lexical/get_str_value (-> vs_tokens :item_name)))
              (and (= Timestamp (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToTimestamp (my-lexical/get_str_value (-> vs_tokens :item_name)))
              (and (= BigDecimal (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToDecimal (my-lexical/get_str_value (-> vs_tokens :item_name)))
              (contains? vs_tokens :operation) (-> (first (mid_to_forwrod ignite group_id (reverse (-> vs_tokens :operation)))) :express)
              (contains? vs_tokens :parenthesis) (-> (first (mid_to_forwrod ignite group_id (reverse (-> vs_tokens :parenthesis)))) :express)
              (contains? vs_tokens :func-name) (-> (first (mid_to_forwrod ignite group_id [vs_tokens])) :express)
              )))

(defn plus_value_tokens [^Ignite ignite ^Long group_id vs]
    (if-let [vs_tokens (my-select-plus/sql-to-ast vs)]
        (cond (and (= String (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (my-lexical/get_str_value (-> vs_tokens :item_name))
              (and (= Integer (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToInt (my-lexical/get_str_value (-> vs_tokens :item_name)))
              (and (= Boolean (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToBoolean (my-lexical/get_str_value (-> vs_tokens :item_name)))
              (and (= Long (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToLong (my-lexical/get_str_value (-> vs_tokens :item_name)))
              (and (= Float (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToDouble (my-lexical/get_str_value (-> vs_tokens :item_name)))
              (and (= Double (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToDouble (my-lexical/get_str_value (-> vs_tokens :item_name)))
              (and (= Timestamp (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToTimestamp (my-lexical/get_str_value (-> vs_tokens :item_name)))
              (and (= BigDecimal (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToDecimal (my-lexical/get_str_value (-> vs_tokens :item_name)))
              (contains? vs_tokens :operation) (-> (first (mid_to_forwrod ignite group_id (reverse (-> vs_tokens :operation)))) :express)
              (contains? vs_tokens :parenthesis) (-> (first (mid_to_forwrod ignite group_id (reverse (-> vs_tokens :parenthesis)))) :express)
              (contains? vs_tokens :func-name) (-> (first (mid_to_forwrod ignite group_id [vs_tokens])) :express)
              )))

; 在调用的时候，形成 dic 参数的名字做 key, 值和数据类型做为 value 调用的方法是  my-lexical/get_scenes_dic
; dic_paras = {user_name {:value "吴大富" :type String} pass_word {:value "123" :type String}}
(defn get_plus_value_tokens_fun [^Ignite ignite ^Long group_id ^String vs_line ^clojure.lang.PersistentArrayMap dic_paras]
    (if-let [vs_tokens (my-select-plus/sql-to-ast (my-lexical/to-back vs_line))]
        (cond (and (= String (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (my-lexical/get_str_value (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
              (and (= Integer (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToInt (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
              (and (= Boolean (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToBoolean (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
              (and (= Long (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToLong (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
              (and (= Float (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToDouble (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
              (and (= Double (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToDouble (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
              (and (= Timestamp (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToTimestamp (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
              (and (= BigDecimal (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToDecimal (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
              (contains? vs_tokens :operation) (-> (first (mid_to_forwrod_fun ignite group_id dic_paras (reverse (-> vs_tokens :operation)))) :express)
              (contains? vs_tokens :parenthesis) (-> (first (mid_to_forwrod_fun ignite group_id dic_paras (reverse (-> vs_tokens :parenthesis)))) :express)
              (contains? vs_tokens :func-name) (-> (first (mid_to_forwrod_fun ignite group_id dic_paras [vs_tokens])) :express)
              )))

(defn plus_value_tokens_fun [^Ignite ignite ^Long group_id vs ^clojure.lang.PersistentArrayMap dic_paras]
    (if-let [vs_tokens (my-select-plus/sql-to-ast vs)]
        (cond (and (= String (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (my-lexical/get_str_value (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
              (and (= Integer (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToInt (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
              (and (= Boolean (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToBoolean (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
              (and (= Long (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToLong (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
              (and (= Float (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToDouble (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
              (and (= Double (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToDouble (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
              (and (= Timestamp (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToTimestamp (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
              (and (= BigDecimal (-> vs_tokens :java_item_type)) (true? (-> vs_tokens :const))) (MyConvertUtil/ConvertToDecimal (my-lexical/get_dic_vs dic_paras (my-lexical/get_str_value (-> vs_tokens :item_name))))
              (contains? vs_tokens :operation) (-> (first (mid_to_forwrod_fun ignite group_id dic_paras (reverse (-> vs_tokens :operation)))) :express)
              (contains? vs_tokens :parenthesis) (-> (first (mid_to_forwrod_fun ignite group_id dic_paras (reverse (-> vs_tokens :parenthesis)))) :express)
              (contains? vs_tokens :func-name) (-> (first (mid_to_forwrod_fun ignite group_id dic_paras [vs_tokens])) :express)
              )))

; 输入字符串，获取在 plus 中的值
; vs_line 字符串 (可以是值，函数，表达式)
(defn get_plus_value [^Ignite ignite ^Long group_id ^String column_type ^String vs_line]
    (cond (re-find #"^(?i)varchar$|^(?i)varchar\(\d+\)$|^(?i)char$|^(?i)char\(\d+\)$" column_type) (cond (re-find #"^\'[\S\s]+\'$|^\"[\S\s]+\"$|^\'\'$|^\"\"$" vs_line) (my-lexical/get_str_value vs_line)
                                                                                                         (my-lexical/is-eq? vs_line "null") ""
                                                                                                         :else
                                                                                                         (MyConvertUtil/ConvertToString (get_plus_value_tokens ignite group_id vs_line)))
          (re-find #"^(?i)BOOLEAN$" column_type) (cond (re-find #"^(?i)true$|^(?i)\'true\'$|^(?i)\"true\"$" vs_line) true
                                                       (re-find #"^(?i)false$|^(?i)\'false\'$|^(?i)\"false\"$" vs_line) false
                                                       (re-find #"^(?i)1$|^(?i)\'1\'$|^(?i)\"1\"$" vs_line) true
                                                       (re-find #"^(?i)0$|^(?i)\'0\'$|^(?i)\"0\"$|^(?i)-1$|^(?i)\'-1\'$|^(?i)\"-1\"$" vs_line) false
                                                       :else
                                                       (MyConvertUtil/ConvertToBoolean (get_plus_value_tokens ignite group_id vs_line))
                                                       )
          (re-find #"^(?i)integer$|^(?i)int$|^(?i)SMALLINT$" column_type) (if-let [m (re-find #"^(?i)\d+$|^(?i)\'\d+\'$|^(?i)\"\d+\"$" vs_line)]
                                                                              (MyConvertUtil/ConvertToInt (my-lexical/get_str_value m))
                                                                              (MyConvertUtil/ConvertToInt (get_plus_value_tokens ignite group_id vs_line)))
          (re-find #"^(?i)BIGINT$|^(?i)long$" column_type) (if-let [m (re-find #"^(?i)\d+$|^(?i)\'\d+\'$|^(?i)\"\d+\"$" vs_line)]
                                                    (MyConvertUtil/ConvertToLong (my-lexical/get_str_value m))
                                                    (MyConvertUtil/ConvertToLong (get_plus_value_tokens ignite group_id vs_line)))
          (re-find #"^(?i)FLOAT$" column_type) (if-let [m (re-find #"^(?i)\d+$|^(?i)\'\d+\'$|^(?i)\"\d+\"$" vs_line)]
                                                    (MyConvertUtil/ConvertToDouble (my-lexical/get_str_value m))
                                                    (MyConvertUtil/ConvertToDouble (get_plus_value_tokens ignite group_id vs_line)))
          (re-find #"^(?i)double$" column_type) (if-let [m (re-find #"^(?i)\d+$|^(?i)\'\d+\'$|^(?i)\"\d+\"$" vs_line)]
                                                    (MyConvertUtil/ConvertToDouble (my-lexical/get_str_value m))
                                                    (MyConvertUtil/ConvertToDouble (get_plus_value_tokens ignite group_id vs_line)))
          (re-find #"^(?i)DECIMAL\(\s*\d+\s*,\s*\d+\s*\)$|^(?i)DECIMAL\(\s*\d+\s*\)$|^(?i)DECIMAL$|^(?i)REAL$" column_type) (if-let [m (re-find #"^(?i)\d+\.\d+$|^(?i)\'\d+\.\d+\'$|^(?i)\"\d+\.\d+\"$" vs_line)]
                                                    (MyConvertUtil/ConvertToDecimal (my-lexical/get_str_value m))
                                                    (MyConvertUtil/ConvertToDecimal (get_plus_value_tokens ignite group_id vs_line)))
          (re-find #"^(?i)TIMESTAMP$|^(?i)Date$|^(?i)DATETIME$|^(?i)TIME$" column_type) (MyConvertUtil/ConvertToTimestamp (get_plus_value_tokens ignite group_id vs_line))
          )
    )

(defn plus_value [^Ignite ignite ^Long group_id ^String column_type vs]
    (if (= (count vs) 1)
        (get_plus_value ignite group_id column_type (nth vs 0))
        (cond (re-find #"^(?i)varchar$|^(?i)varchar\(\d+\)$|^(?i)char$|^(?i)char\(\d+\)$" column_type) (MyConvertUtil/ConvertToString (plus_value_tokens ignite group_id vs))
              (re-find #"^(?i)BOOLEAN$" column_type) (MyConvertUtil/ConvertToBoolean (plus_value_tokens ignite group_id vs))
              (re-find #"^(?i)integer$|^(?i)int$|^(?i)SMALLINT$" column_type) (MyConvertUtil/ConvertToInt (plus_value_tokens ignite group_id vs))
              (re-find #"^(?i)BIGINT$|^(?i)long$" column_type) (MyConvertUtil/ConvertToLong (plus_value_tokens ignite group_id vs))
              (re-find #"^(?i)FLOAT$" column_type) (MyConvertUtil/ConvertToDouble (plus_value_tokens ignite group_id vs))
              (re-find #"^(?i)double$" column_type) (MyConvertUtil/ConvertToDouble (plus_value_tokens ignite group_id vs))
              (re-find #"^(?i)DECIMAL\(\s*\d+\s*,\s*\d+\s*\)$|^(?i)DECIMAL\(\s*\d+\s*\)$|^(?i)DECIMAL$|^(?i)REAL$" column_type) (MyConvertUtil/ConvertToDecimal (plus_value_tokens ignite group_id vs))
              (re-find #"^(?i)TIMESTAMP$|^(?i)Date$|^(?i)DATETIME$|^(?i)TIME$" column_type) (MyConvertUtil/ConvertToTimestamp (plus_value_tokens ignite group_id vs))
              )))

; 在调用的时候，形成 dic 参数的名字做 key, 值和数据类型做为 value 调用的方法是  my-lexical/get_scenes_dic
; dic_paras = {user_name {:value "吴大富" :type String} pass_word {:value "123" :type String}}
; name 字符串
;(defn my-lexical/get_dic_vs [^clojure.lang.PersistentArrayMap dic_paras ^String name]
;    (if (and (= (first name) \:) (contains? dic_paras (str/join (rest name))))
;        (let [{value :value type :type} (get dic_paras (str/join (rest name)))]
;            (cond (= Integer type) (MyConvertUtil/ConvertToInt value)
;                  (= String type) (str_item_value value)
;                  (= Boolean type) (MyConvertUtil/ConvertToBoolean value)
;                  (= Long type) (MyConvertUtil/ConvertToLong value)
;                  (= Timestamp type) (MyConvertUtil/ConvertToTimestamp value)
;                  (= BigDecimal type) (MyConvertUtil/ConvertToDecimal value)
;                  (= "byte[]" type) (MyConvertUtil/ConvertToByte value)
;                  (= Object type) value
;                  ))
;        name))

; 在调用的时候，形成 dic 参数的名字做 key, 值和数据类型做为 value 调用的方法是  my-lexical/get_scenes_dic
; dic_paras = {user_name {:value "吴大富" :type String} pass_word {:value "123" :type String}}
(defn get_plus_value_fun [^Ignite ignite ^Long group_id ^String column_type ^String vs_line ^clojure.lang.PersistentArrayMap dic_paras]
    (cond (re-find #"^(?i)varchar$|^(?i)varchar\(\d+\)$|^(?i)char$|^(?i)char\(\d+\)$" column_type) (cond (re-find #"^\'[\S\s]+\'$|^\"[\S\s]+\"$|^\'\'$|^\"\"$" vs_line) (my-lexical/get_str_value vs_line)
                                                                                                         :else
                                                                                                         (MyConvertUtil/ConvertToString (get_plus_value_tokens_fun ignite group_id vs_line dic_paras)))
          (re-find #"^(?i)BOOLEAN$" column_type) (cond (re-find #"^(?i)true$|^(?i)\'true\'$|^(?i)\"true\"$" vs_line) true
                                                       (re-find #"^(?i)false$|^(?i)\'false\'$|^(?i)\"false\"$" vs_line) false
                                                       (re-find #"^(?i)1$|^(?i)\'1\'$|^(?i)\"1\"$" vs_line) true
                                                       (re-find #"^(?i)0$|^(?i)\'0\'$|^(?i)\"0\"$|^(?i)-1$|^(?i)\'-1\'$|^(?i)\"-1\"$" vs_line) false
                                                       :else
                                                       (MyConvertUtil/ConvertToBoolean (get_plus_value_tokens_fun ignite group_id vs_line dic_paras))
                                                       )
          (re-find #"^(?i)integer$|^(?i)int$|^(?i)SMALLINT$" column_type) (if-let [m (re-find #"^(?i)\d+$|^(?i)\'\d+\'$|^(?i)\"\d+\"$" vs_line)]
                                                                              (MyConvertUtil/ConvertToInt (my-lexical/get_str_value m))
                                                                              (MyConvertUtil/ConvertToInt (get_plus_value_tokens_fun ignite group_id vs_line dic_paras)))
          (re-find #"^(?i)BIGINT$|^(?i)long$" column_type) (if-let [m (re-find #"^(?i)\d+$|^(?i)\'\d+\'$|^(?i)\"\d+\"$" vs_line)]
                                                    (MyConvertUtil/ConvertToLong (my-lexical/get_str_value m))
                                                    (MyConvertUtil/ConvertToLong (get_plus_value_tokens_fun ignite group_id vs_line dic_paras)))
          (re-find #"^(?i)FLOAT$" column_type) (if-let [m (re-find #"^(?i)\d+$|^(?i)\'\d+\'$|^(?i)\"\d+\"$" vs_line)]
                                                    (MyConvertUtil/ConvertToDouble (my-lexical/get_str_value m))
                                                    (MyConvertUtil/ConvertToDouble (get_plus_value_tokens_fun ignite group_id vs_line dic_paras)))
          (re-find #"^(?i)double$" column_type) (if-let [m (re-find #"^(?i)\d+$|^(?i)\'\d+\'$|^(?i)\"\d+\"$" vs_line)]
                                                    (MyConvertUtil/ConvertToDouble (my-lexical/get_str_value m))
                                                    (MyConvertUtil/ConvertToDouble (get_plus_value_tokens_fun ignite group_id vs_line dic_paras)))
          (re-find #"^(?i)DECIMAL\(\s*\d+\s*,\s*\d+\s*\)$|^(?i)DECIMAL\(\s*\d+\s*\)$|^(?i)DECIMAL$|^(?i)REAL$" column_type) (if-let [m (re-find #"^(?i)\d+\.\d+$|^(?i)\'\d+\.\d+\'$|^(?i)\"\d+\.\d+\"$" vs_line)]
                                                                                                                                (MyConvertUtil/ConvertToDecimal (my-lexical/get_str_value m))
                                                                                                                                (MyConvertUtil/ConvertToDecimal (get_plus_value_tokens_fun ignite group_id vs_line dic_paras)))
          (re-find #"^(?i)TIMESTAMP$|^(?i)Date$|^(?i)DATETIME$|^(?i)TIME$" column_type) (MyConvertUtil/ConvertToTimestamp (get_plus_value_tokens_fun ignite group_id vs_line dic_paras))
          )
    )

(defn plus_value_fun [^Ignite ignite ^Long group_id ^String column_type vs ^clojure.lang.PersistentArrayMap dic_paras]
    (if (= (count vs) 1)
        (get_plus_value_fun ignite group_id column_type (nth vs 0) dic_paras)
        (cond (re-find #"^(?i)varchar$|^(?i)varchar\(\d+\)$|^(?i)char$|^(?i)char\(\d+\)$" column_type) (MyConvertUtil/ConvertToString (plus_value_tokens_fun ignite group_id vs dic_paras))
              (re-find #"^(?i)BOOLEAN$" column_type) (MyConvertUtil/ConvertToBoolean (plus_value_tokens_fun ignite group_id vs dic_paras))
              (re-find #"^(?i)integer$|^(?i)int$|^(?i)SMALLINT$" column_type) (MyConvertUtil/ConvertToInt (plus_value_tokens_fun ignite group_id vs dic_paras))
              (re-find #"^(?i)BIGINT$|^(?i)long$" column_type) (MyConvertUtil/ConvertToLong (plus_value_tokens_fun ignite group_id vs dic_paras))
              (re-find #"^(?i)FLOAT$" column_type) (MyConvertUtil/ConvertToDouble (plus_value_tokens_fun ignite group_id vs dic_paras))
              (re-find #"^(?i)double$" column_type) (MyConvertUtil/ConvertToDouble (plus_value_tokens_fun ignite group_id vs dic_paras))
              (re-find #"^(?i)DECIMAL\(\s*\d+\s*,\s*\d+\s*\)$|^(?i)DECIMAL\(\s*\d+\s*\)$|^(?i)DECIMAL$|^(?i)REAL$" column_type) (MyConvertUtil/ConvertToDecimal (plus_value_tokens_fun ignite group_id vs dic_paras))
              (re-find #"^(?i)TIMESTAMP$|^(?i)Date$|^(?i)DATETIME$|^(?i)TIME$" column_type) (MyConvertUtil/ConvertToTimestamp (plus_value_tokens_fun ignite group_id vs dic_paras))
              ))
    )

; 判断 item 是否在 input_paras 中
(defn has_input_paras [[f & r] item_value]
    (if (some? f)
        (if (my-lexical/is-eq? (str/join [":" (.getParameter_value f)]) item_value) f
                                                                  (recur r item_value))))

(defn item_type_fun [java_item_type item_value ^ArrayList input_paras]
    (if-let [mi (has_input_paras input_paras item_value)]
        (.getParameter_value mi)
        (cond (= Integer java_item_type) (MyConvertUtil/ConvertToInt item_value)
              (= String java_item_type) (str_item_value item_value)
              (= Boolean java_item_type) (MyConvertUtil/ConvertToBoolean item_value)
              (= Long java_item_type) (MyConvertUtil/ConvertToLong item_value)
              (= Float java_item_type) (MyConvertUtil/ConvertToDouble item_value)
              (= Double java_item_type) (MyConvertUtil/ConvertToDouble item_value)
              (= Timestamp java_item_type) (MyConvertUtil/ConvertToTimestamp item_value)
              (= BigDecimal java_item_type) (MyConvertUtil/ConvertToDecimal item_value)
              (= "byte[]" java_item_type) (MyConvertUtil/ConvertToByte item_value)
              (= Object java_item_type) item_value
              )))

(defn item_type_binaryObj [java_item_type item_value ^BinaryObject binaryObject dic]
    (if (and (contains? dic item_value) (.hasField binaryObject item_value))
        (cond (= Integer java_item_type) (MyConvertUtil/ConvertToInt (.field binaryObject item_value))
              (= String java_item_type) (str_item_value (.field binaryObject item_value))
              (= Boolean java_item_type) (MyConvertUtil/ConvertToBoolean (.field binaryObject item_value))
              (= Long java_item_type) (MyConvertUtil/ConvertToLong (.field binaryObject item_value))
              (= Float java_item_type) (MyConvertUtil/ConvertToDouble (.field binaryObject item_value))
              (= Double java_item_type) (MyConvertUtil/ConvertToDouble (.field binaryObject item_value))
              (= Timestamp java_item_type) (MyConvertUtil/ConvertToTimestamp (.field binaryObject item_value))
              (= BigDecimal java_item_type) (MyConvertUtil/ConvertToDecimal (.field binaryObject item_value))
              (= "byte[]" java_item_type) (MyConvertUtil/ConvertToByte (.field binaryObject item_value))
              (= Object java_item_type) item_value
              )
        (cond (= Integer java_item_type) (MyConvertUtil/ConvertToInt item_value)
              (= String java_item_type) (str_item_value item_value)
              (= Boolean java_item_type) (MyConvertUtil/ConvertToBoolean item_value)
              (= Long java_item_type) (MyConvertUtil/ConvertToLong item_value)
              (= Float java_item_type) (MyConvertUtil/ConvertToDouble item_value)
              (= Double java_item_type) (MyConvertUtil/ConvertToDouble item_value)
              (= Timestamp java_item_type) (MyConvertUtil/ConvertToTimestamp item_value)
              (= BigDecimal java_item_type) (MyConvertUtil/ConvertToDecimal item_value)
              (= "byte[]" java_item_type) (MyConvertUtil/ConvertToByte item_value)
              (= Object java_item_type) item_value
              )))

(defn item_type_binaryObj_fun [java_item_type item_value ^BinaryObject binaryObject dic ^clojure.lang.PersistentArrayMap dic_paras]
    (cond (and (some? dic_paras) (= (first item_value) \:)) (my-lexical/get_dic_vs dic_paras item_value)
          (and (some? binaryObject) (contains? dic item_value) (.hasField binaryObject item_value)) (cond (= Integer java_item_type) (MyConvertUtil/ConvertToInt (.field binaryObject item_value))
                                                                                                          (= String java_item_type) (str_item_value (.field binaryObject item_value))
                                                                                                          (= Boolean java_item_type) (MyConvertUtil/ConvertToBoolean (.field binaryObject item_value))
                                                                                                          (= Long java_item_type) (MyConvertUtil/ConvertToLong (.field binaryObject item_value))
                                                                                                          (= Float java_item_type) (MyConvertUtil/ConvertToDouble (.field binaryObject item_value))
                                                                                                          (= Double java_item_type) (MyConvertUtil/ConvertToDouble (.field binaryObject item_value))
                                                                                                          (= Timestamp java_item_type) (MyConvertUtil/ConvertToTimestamp (.field binaryObject item_value))
                                                                                                          (= BigDecimal java_item_type) (MyConvertUtil/ConvertToDecimal (.field binaryObject item_value))
                                                                                                          (= "byte[]" java_item_type) (MyConvertUtil/ConvertToByte (.field binaryObject item_value))
                                                                                                          (= Object java_item_type) (.field binaryObject item_value)
                                                                                                          )
          :else
          (cond (= Integer java_item_type) (MyConvertUtil/ConvertToInt (my-lexical/get_dic_vs dic_paras item_value))
                (= String java_item_type) (str_item_value (my-lexical/get_dic_vs dic_paras item_value))
                (= Boolean java_item_type) (MyConvertUtil/ConvertToBoolean (my-lexical/get_dic_vs dic_paras item_value))
                (= Long java_item_type) (MyConvertUtil/ConvertToLong (my-lexical/get_dic_vs dic_paras item_value))
                (= Float java_item_type) (MyConvertUtil/ConvertToDouble (my-lexical/get_dic_vs dic_paras item_value))
                (= Double java_item_type) (MyConvertUtil/ConvertToDouble (my-lexical/get_dic_vs dic_paras item_value))
                (= Timestamp java_item_type) (MyConvertUtil/ConvertToTimestamp (my-lexical/get_dic_vs dic_paras item_value))
                (= BigDecimal java_item_type) (MyConvertUtil/ConvertToDecimal (my-lexical/get_dic_vs dic_paras item_value))
                (= "byte[]" java_item_type) (MyConvertUtil/ConvertToByte (my-lexical/get_dic_vs dic_paras item_value))
                (= Object java_item_type) (.field binaryObject (my-lexical/get_dic_vs dic_paras item_value))
                )
          ))

(defn str_item_value_line [item_value]
    (cond (and (= (first item_value) \') (= (last item_value) \')) (str/join (concat ["\""] (reverse (rest (reverse (rest item_value)))) ["\""]))
          :else
          item_value))

; 获取 java type 的优先级
; decimal 的优先级大于 long, 大于 integer
(defn get_type [f m]
    (cond (= (-> f :java_type) BigDecimal) (if (= (-> m :java_type) BigDecimal) (cond (and (contains? m :p) (contains? f :p)) (assoc m :p (max (-> m :p) (-> f :p)) :d (max (-> m :d) (-> f :d)))
                                                                                      (and (contains? m :p) (not (contains? f :p))) m
                                                                                      (and (contains? f :p) (not (contains? m :p))) f
                                                                                      :else
                                                                                      f)
                                                                                f)
          (= (-> m :java_type) BigDecimal) (if (= (-> f :java_type) BigDecimal) (cond (and (contains? m :p) (contains? f :p)) (assoc f :p (max (-> f :p) (-> m :p)) :d (max (-> f :d) (-> m :d)))
                                                                                      (and (contains? m :p) (not (contains? f :p))) m
                                                                                      (and (contains? f :p) (not (contains? m :p))) f
                                                                                      :else
                                                                                      f)
                                                                                m)
          (and (= (-> f :java_type) Long) (contains? #{Long Integer Object String} (-> m :java_type))) f
          (and (= (-> m :java_type) Long) (contains? #{Long Integer Object String} (-> f :java_type))) m
          (and (= (-> f :java_type) Integer) (contains? #{Integer Object String} (-> m :java_type))) f
          (and (= (-> m :java_type) Integer) (contains? #{Integer Object String} (-> f :java_type))) m
          :else
          (do
              (throw (Exception. "在四则运算中不能有非数字的数据类型")))
          ))

; 获取 express_type 的数据类型返回值为 my-lexical/convert_to_java_type 的返回值
; 输入：stack_number
(defn express_type
    ([lst_express] (express_type lst_express nil))
    ([[f & r] java_type]
     (if (some? f)
         (if (nil? java_type) (recur r f)
                              (recur r (get_type f java_type)))
         java_type)))

(defn get_express_obj [top_symbol first_item second_item]
    (when-let [jt (express_type [first_item second_item])]
        (cond (= (-> top_symbol :operation_symbol) "+") (cond (= (-> jt :java_type) BigDecimal) {:express (.add (MyConvertUtil/ConvertToDecimal (-> first_item :express)) (MyConvertUtil/ConvertToDecimal (-> second_item :express))) :java_type (-> jt :java_type) :item_type (-> jt :item_type)}
                                                              (= (-> jt :java_type) Long) {:express (+ (MyConvertUtil/ConvertToLong (-> first_item :express)) (MyConvertUtil/ConvertToLong (-> second_item :express))) :java_type (-> jt :java_type) :item_type (-> jt :item_type)}
                                                              (= (-> jt :java_type) Integer) {:express (+ (MyConvertUtil/ConvertToInt (-> first_item :express)) (MyConvertUtil/ConvertToInt (-> second_item :express))) :java_type (-> jt :java_type) :item_type (-> jt :item_type)})
              (= (-> top_symbol :operation_symbol) "-") (cond (= (-> jt :java_type) BigDecimal) {:express (.subtract (MyConvertUtil/ConvertToDecimal (-> first_item :express)) (MyConvertUtil/ConvertToDecimal (-> second_item :express))) :java_type (-> jt :java_type) :item_type (-> jt :item_type)}
                                                              (= (-> jt :java_type) Long) {:express (- (MyConvertUtil/ConvertToLong (-> first_item :express)) (MyConvertUtil/ConvertToLong (-> second_item :express))) :java_type (-> jt :java_type) :item_type (-> jt :item_type)}
                                                              (= (-> jt :java_type) Integer) {:express (- (MyConvertUtil/ConvertToInt (-> first_item :express)) (MyConvertUtil/ConvertToInt (-> second_item :express))) :java_type (-> jt :java_type) :item_type (-> jt :item_type)})
              (= (-> top_symbol :operation_symbol) "*") (cond (= (-> jt :java_type) BigDecimal) {:express (.multiply (MyConvertUtil/ConvertToDecimal (-> first_item :express)) (MyConvertUtil/ConvertToDecimal (-> second_item :express))) :java_type (-> jt :java_type) :item_type (-> jt :item_type)}
                                                              (= (-> jt :java_type) Long) {:express (* (MyConvertUtil/ConvertToLong (-> first_item :express)) (MyConvertUtil/ConvertToLong (-> second_item :express))) :java_type (-> jt :java_type) :item_type (-> jt :item_type)}
                                                              (= (-> jt :java_type) Integer) {:express (* (MyConvertUtil/ConvertToInt (-> first_item :express)) (MyConvertUtil/ConvertToInt (-> second_item :express))) :java_type (-> jt :java_type) :item_type (-> jt :item_type)})
              (= (-> top_symbol :operation_symbol) "/") (if (contains? jt :d) {:express (.divide (MyConvertUtil/ConvertToDecimal (-> first_item :express)) (MyConvertUtil/ConvertToDecimal (-> second_item :express)) (MyConvertUtil/ConvertToInt (-> jt :p)) (BigDecimal/ROUND_HALF_UP)) :java_type BigDecimal :item_type (-> jt :item_type)}
                                                                              {:express (.divide (MyConvertUtil/ConvertToDecimal (-> first_item :express)) (MyConvertUtil/ConvertToDecimal (-> second_item :express)) 10 (BigDecimal/ROUND_HALF_UP)) :java_type BigDecimal :item_type (-> jt :item_type)})
              )
        ))

; 对表达式求值，惰性
(defn run-express [stack_number stack_symbo]
    (if (some? (peek stack_symbo))
        (let [first_item (peek stack_number) second_item (peek (pop stack_number)) top_symbol (peek stack_symbo)]
            (recur (conj (pop (pop stack_number)) (get_express_obj top_symbol first_item second_item)) (pop stack_symbo)))
        stack_number))

;(defn run-express [stack_number stack_symbo]
;    (if (some? (peek stack_symbo))
;        (let [first_item (peek stack_number) second_item (peek (pop stack_number)) top_symbol (peek stack_symbo)]
;            (recur (conj (pop (pop stack_number)) (concat [(get_express_obj top_symbol first_item second_item)])) (pop stack_symbo))) stack_number))

; 判断符号优先级
; f symbol 的优先级大于等于 s 返回 true 否则返回 false
(defn is-symbol-priority [f s]
    (cond (or (= (-> f :operation_symbol) "*") (= (-> f :operation_symbol) "/")) true
          (and (or (= (-> f :operation_symbol) "+") (= (-> f :operation_symbol) "-")) (or (= (-> s :operation_symbol) "+") (= (-> s :operation_symbol) "-"))) true
          :else
          false))

; 对函数求值
(defn func_eval [ignite group_id func_obj]
    (if (some? func_obj)
        {:express (.myInvoke (.getMyScenes (MyScenesService/getInstance)) ignite (-> func_obj :func-name) (to-array (cons group_id (func_lst_ps_eval ignite group_id (-> func_obj :lst_ps))))) :item_type nil :java_type Object}))

; 对函数求值
; dic 表示 名字: 数据类型的键值对
; :dic {"discount" "DECIMAL", "orderid" "integer", "productid" "integer", "quantity" "INTEGER", "unitprice" "decimal"}
(defn func_eval_binary [ignite group_id func_obj ^BinaryObject binaryObject dic]
    (if (some? func_obj)
        {:express (.myInvoke (.getMyScenes (MyScenesService/getInstance)) ignite (-> func_obj :func-name) (to-array (func_lst_ps_eval_binaryObject ignite group_id binaryObject dic (-> func_obj :lst_ps)))) :item_type nil :java_type Object}))

; 处理函数的参数
(defn func_lst_ps_eval
    ([ignite group_id lst_tokens] (func_lst_ps_eval ignite group_id lst_tokens []))
    ([ignite group_id [f & r] lst_ps_rs]
     (if (some? f)
         (cond (contains? f :operation) (when-let [mt (mid_to_forwrod ignite group_id (reverse (-> f :operation)))]
                                            (recur ignite group_id r (conj lst_ps_rs (-> mt :express))))
               (contains? f :parenthesis) (when-let [mt (mid_to_forwrod ignite group_id (reverse (-> f :parenthesis)))]
                                              (recur ignite group_id r (conj lst_ps_rs (-> mt :express))))
               (contains? f :func-name) (when-let [mt (func_eval group_id ignite f)]
                                            (recur ignite group_id r (conj lst_ps_rs (-> mt :express))))
               (contains? f :item_name) (cond (and (= (-> f :const) true) (not (= (-> f :java_item_type) String))) (recur ignite group_id r (conj lst_ps_rs (-> f :item_name)))
                                              (and (= (-> f :const) true) (not (= (-> f :java_item_type) String))) (str_item_value_line (-> f :item_name))
                                              :else
                                              (recur ignite group_id r (conj lst_ps_rs (item_type (-> f :java_item_type) (-> f :item_name)))))
               :else
               (recur ignite group_id r lst_ps_rs)
               ) lst_ps_rs)))

; dic 表示 名字: 数据类型的键值对
; :dic {"discount" "DECIMAL", "orderid" "integer", "productid" "integer", "quantity" "INTEGER", "unitprice" "decimal"}
(defn func_lst_ps_eval_binaryObject
    ([ignite group_id ^BinaryObject binaryObject dic lst_tokens] (func_lst_ps_eval_binaryObject ignite group_id ^BinaryObject binaryObject dic lst_tokens []))
    ([ignite group_id ^BinaryObject binaryObject dic [f & r] lst_ps_rs]
     (if (some? f)
         (cond (contains? f :operation) (when-let [mt (mid_to_forwrod_binaryObject ignite group_id binaryObject dic (reverse (-> f :operation)))]
                                            (recur ignite group_id binaryObject dic r (conj lst_ps_rs (-> mt :express))))
               (contains? f :parenthesis) (when-let [mt (mid_to_forwrod_binaryObject ignite group_id binaryObject dic (reverse (-> f :parenthesis)))]
                                              (recur ignite group_id binaryObject dic r (conj lst_ps_rs (-> mt :express))))
               (contains? f :func-name) (when-let [mt (func_eval_binary ignite group_id f binaryObject dic)]
                                            (recur ignite group_id binaryObject dic r (conj lst_ps_rs (-> mt :express))))
               (contains? f :item_name) (cond (and (= (-> f :const) true) (not (= (-> f :java_item_type) String))) (recur ignite group_id binaryObject dic r (conj lst_ps_rs (-> f :item_name)))
                                              (and (= (-> f :const) true) (not (= (-> f :java_item_type) String))) (str_item_value_line (-> f :item_name))
                                              :else
                                              (recur ignite group_id binaryObject dic r (conj lst_ps_rs (item_type (-> f :java_item_type) (-> f :item_name)))))
               :else
               (recur ignite group_id binaryObject dic r lst_ps_rs)
               ) lst_ps_rs)))

; 将运算表达式中缀转前缀
; 返回的结果是一个 express_obj = {:express 表达式 :java_type java 是数据类型 :item_type 数据库的数据类型}
;(defn mid_to_forwrod
;    ([ignite lst_tokens] (mid_to_forwrod ignite lst_tokens [] []))
;    ([ignite [f & r] stack_number stack_symbol]
;     (if (some? f)
;         (cond (contains? f :operation_symbol) (cond
;                                                   ; 若符号栈为空，则符号直接压入符号栈
;                                                   (= (count stack_symbol) 0) (recur ignite r stack_number (conj stack_symbol f))
;                                                   ; f 符号的优先级高于或等于符号栈栈顶的优先级，则直接入栈
;                                                   (is-symbol-priority f (peek stack_symbol)) (recur ignite r stack_number (conj stack_symbol f))
;                                                   ; f 符号的优先级低于栈顶的优先级，则将符号栈顶，弹出参与计算后，在压入，数据栈
;                                                   :else
;                                                   (let [first_item (peek stack_number) second_item (peek (pop stack_number)) top_symbol (peek stack_symbol)]
;                                                       (recur ignite r (conj (pop (pop stack_number)) (get_express_obj top_symbol first_item second_item)) (conj (pop stack_symbol) f)))
;                                                   )
;               (contains? f :parenthesis) (when-let [m (mid_to_forwrod ignite (reverse (-> f :parenthesis)))]
;                                              (recur ignite r (conj stack_number (first m)) stack_symbol))
;               (contains? f :item_name) (let [{table_alias :table_alias item_name :item_name java_item_type :java_item_type item_type :item_type} f]
;                                            (if (= table_alias "")
;                                                (recur ignite r (conj stack_number {:express item_name :java_type java_item_type :item_type item_type}) stack_symbol)))
;               (contains? f :func-name) (recur ignite r (conj stack_number (func_eval ignite f)) stack_symbol)
;               :else
;               (recur ignite r (conj stack_number f) stack_symbol))
;         (run-express stack_number stack_symbol))))

(defn mid_to_forwrod
    ([ignite group_id lst_tokens] (mid_to_forwrod ignite group_id lst_tokens [] []))
    ([ignite group_id lst_tokens stack_number stack_symbol]
     (cond (map? lst_tokens) (cond (contains? lst_tokens :parenthesis) (mid_to_forwrod ignite group_id (reverse (-> lst_tokens :parenthesis)))
                                   (contains? lst_tokens :operation) (mid_to_forwrod ignite group_id (reverse (-> lst_tokens :operation)))
                                   (and (contains? lst_tokens :item_name) (= String (-> lst_tokens :java_item_type))) {:express (item_type (-> lst_tokens :java_item_type) (-> lst_tokens :item_name)) :java_type java.lang.String :item_type ""}
                                   :else
                                   (throw (Exception. "字符串串错误！")))
           (list? lst_tokens) (let [f (first lst_tokens) r (rest lst_tokens)]
                                  (if (some? f)
                                      (cond (contains? f :operation_symbol) (cond
                                                                                ; 若符号栈为空，则符号直接压入符号栈
                                                                                (= (count stack_symbol) 0) (recur ignite group_id r stack_number (conj stack_symbol f))
                                                                                ; f 符号的优先级高于或等于符号栈栈顶的优先级，则直接入栈
                                                                                (is-symbol-priority f (peek stack_symbol)) (recur ignite group_id r stack_number (conj stack_symbol f))
                                                                                ; f 符号的优先级低于栈顶的优先级，则将符号栈顶，弹出参与计算后，在压入，数据栈
                                                                                :else
                                                                                (let [first_item (peek stack_number) second_item (peek (pop stack_number)) top_symbol (peek stack_symbol)]
                                                                                    (recur ignite group_id r (conj (pop (pop stack_number)) (get_express_obj top_symbol first_item second_item)) (conj (pop stack_symbol) f)))
                                                                                )
                                            (contains? f :parenthesis) (when-let [m (mid_to_forwrod ignite group_id (reverse (-> f :parenthesis)))]
                                                                           (recur ignite group_id r (conj stack_number (first m)) stack_symbol))
                                            (contains? f :item_name) (let [{table_alias :table_alias item_name :item_name java_item_type :java_item_type item_type :item_type} f]
                                                                         (if (= table_alias "")
                                                                             (recur ignite group_id r (conj stack_number {:express item_name :java_type java_item_type :item_type item_type}) stack_symbol)))
                                            (contains? f :func-name) (recur ignite group_id r (conj stack_number (func_eval ignite group_id f)) stack_symbol)
                                            :else
                                            (recur ignite group_id r (conj stack_number f) stack_symbol))
                                      (run-express stack_number stack_symbol))))))

; 输入的内容是四则运算的逆序
; 输入 (my-expression/mid_to_forwrod ignite (reverse (-> (my-select-plus/sql-to-ast (my-lexical/to-back "5/2+1 - (3-1)*2")) :operation)))
; 在调用的时候，形成 dic 参数的名字做 key, 值和数据类型做为 value 调用的方法是  my-lexical/get_scenes_dic
; dic_paras = {user_name {:value "吴大富" :type String} pass_word {:value "123" :type String}}
(defn mid_to_forwrod_fun
    ([ignite group_id dic_paras lst_tokens] (mid_to_forwrod_fun ignite group_id dic_paras lst_tokens [] []))
    ([ignite group_id dic_paras lst_tokens stack_number stack_symbol]
     (cond (map? lst_tokens) (cond (contains? lst_tokens :parenthesis) (mid_to_forwrod_fun ignite group_id dic_paras (reverse (-> lst_tokens :parenthesis)))
                                   (contains? lst_tokens :operation) (mid_to_forwrod_fun ignite group_id dic_paras (reverse (-> lst_tokens :operation)))
                                   (and (contains? lst_tokens :item_name) (= String (-> lst_tokens :java_item_type))) {:express (item_type (-> lst_tokens :java_item_type) (-> lst_tokens :item_name)) :java_type java.lang.String :item_type ""}
                                   :else
                                   (throw (Exception. "字符串串错误！")))
           (list? lst_tokens) (let [f (first lst_tokens) r (rest lst_tokens)]
                                  (if (some? f)
                                      (cond (contains? f :operation_symbol) (cond
                                                                                ; 若符号栈为空，则符号直接压入符号栈
                                                                                (= (count stack_symbol) 0) (recur ignite group_id dic_paras r stack_number (conj stack_symbol f))
                                                                                ; f 符号的优先级高于或等于符号栈栈顶的优先级，则直接入栈
                                                                                (is-symbol-priority f (peek stack_symbol)) (recur ignite group_id dic_paras r stack_number (conj stack_symbol f))
                                                                                ; f 符号的优先级低于栈顶的优先级，则将符号栈顶，弹出参与计算后，在压入，数据栈
                                                                                :else
                                                                                (let [first_item (peek stack_number) second_item (peek (pop stack_number)) top_symbol (peek stack_symbol)]
                                                                                    (recur ignite group_id dic_paras r (conj (pop (pop stack_number)) (get_express_obj top_symbol first_item second_item)) (conj (pop stack_symbol) f)))
                                                                                )
                                            (contains? f :parenthesis) (when-let [m (mid_to_forwrod_fun ignite group_id dic_paras (reverse (-> f :parenthesis)))]
                                                                           (recur ignite group_id dic_paras r (conj stack_number (first m)) stack_symbol))
                                            (contains? f :item_name) (let [{table_alias :table_alias item_name :item_name java_item_type :java_item_type item_type :item_type} f]
                                                                         (if (and (= (first item_name) \:) (contains? dic_paras (str/join (rest item_name))))
                                                                             (let [{value :value type :type} (get dic_paras (str/join (rest name)))]
                                                                                 (recur ignite group_id dic_paras r (conj stack_number {:express value :java_type type :item_type nil}) stack_symbol))
                                                                             (if (= table_alias "")
                                                                                 (recur ignite group_id dic_paras r (conj stack_number {:express item_name :java_type java_item_type :item_type item_type}) stack_symbol)))
                                                                         )
                                            (contains? f :func-name) (recur ignite group_id dic_paras r (conj stack_number (func_eval ignite group_id f)) stack_symbol)
                                            :else
                                            (recur ignite group_id dic_paras r (conj stack_number f) stack_symbol))
                                      (run-express stack_number stack_symbol))))))

; binaryObject 为 update 的行
; dic 表示 名字: 数据类型的键值对
; :dic {"discount" "DECIMAL", "orderid" "integer", "productid" "integer", "quantity" "INTEGER", "unitprice" "decimal"}
(defn mid_to_forwrod_binaryObject
    ([^Ignite ignite group_id ^BinaryObject binaryObject dic lst_tokens] (mid_to_forwrod_binaryObject ignite group_id binaryObject dic lst_tokens [] []))
    ([^Ignite ignite group_id ^BinaryObject binaryObject dic lst_tokens stack_number stack_symbol]
     (cond (map? lst_tokens) (cond (contains? lst_tokens :parenthesis) (mid_to_forwrod_binaryObject ignite group_id binaryObject dic (reverse (-> lst_tokens :parenthesis)))
                                   (contains? lst_tokens :operation) (mid_to_forwrod_binaryObject ignite group_id binaryObject dic (reverse (-> lst_tokens :operation)))
                                   (and (contains? lst_tokens :item_name) (= String (-> lst_tokens :java_item_type))) {:express (item_type (-> lst_tokens :java_item_type) (-> lst_tokens :item_name)) :java_type java.lang.String :item_type ""}
                                   :else
                                   (throw (Exception. "字符串串错误！")))
           (list? lst_tokens) (let [f (first lst_tokens) r (rest lst_tokens)]
                                  (if (some? f)
                                      (cond (contains? f :operation_symbol) (cond
                                                                                ; 若符号栈为空，则符号直接压入符号栈
                                                                                (= (count stack_symbol) 0) (recur ignite group_id binaryObject dic r stack_number (conj stack_symbol f))
                                                                                ; f 符号的优先级高于或等于符号栈栈顶的优先级，则直接入栈
                                                                                (is-symbol-priority f (peek stack_symbol)) (recur ignite group_id binaryObject dic r stack_number (conj stack_symbol f))
                                                                                ; f 符号的优先级低于栈顶的优先级，则将符号栈顶，弹出参与计算后，在压入，数据栈
                                                                                :else
                                                                                (let [first_item (peek stack_number) second_item (peek (pop stack_number)) top_symbol (peek stack_symbol)]
                                                                                    (recur ignite group_id binaryObject dic r (conj (pop (pop stack_number)) (get_express_obj top_symbol first_item second_item)) (conj (pop stack_symbol) f)))
                                                                                )
                                            (contains? f :parenthesis) (when-let [m (mid_to_forwrod_binaryObject ignite group_id binaryObject dic (reverse (-> f :parenthesis)))]
                                                                           (recur ignite group_id binaryObject dic r (conj stack_number (first m)) stack_symbol))
                                            (contains? f :item_name) (let [{table_alias :table_alias item_name :item_name java_item_type :java_item_type item_type :item_type} f]
                                                                         (if (and (contains? dic item_name) (.hasField binaryObject item_name))
                                                                             (recur ignite group_id binaryObject dic r (conj stack_number (assoc (my-lexical/convert_to_java_type (.get dic item_name)) :express (.field binaryObject item_name) :item_type (.get dic item_name))) stack_symbol)
                                                                             (if (= table_alias "")
                                                                                 (recur ignite group_id binaryObject dic r (conj stack_number {:express item_name :java_type java_item_type :item_type item_type}) stack_symbol)))
                                                                         )
                                            (contains? f :func-name) (recur ignite group_id binaryObject dic r (conj stack_number (func_eval_binary ignite group_id f binaryObject dic)) stack_symbol)
                                            :else
                                            (recur ignite group_id binaryObject dic r (conj stack_number f) stack_symbol))
                                      (run-express stack_number stack_symbol))))))

; binaryObject 为 update 的行
; dic 表示 名字: 数据类型的键值对
; :dic {"discount" "DECIMAL", "orderid" "integer", "productid" "integer", "quantity" "INTEGER", "unitprice" "decimal"}
(defn mid_to_forwrod_binaryObject_fun
    ([^Ignite ignite group_id ^BinaryObject binaryObject dic lst_tokens ^clojure.lang.PersistentArrayMap dic_paras] (mid_to_forwrod_binaryObject_fun ignite group_id binaryObject dic lst_tokens [] [] dic_paras))
    ([^Ignite ignite group_id ^BinaryObject binaryObject dic lst_tokens stack_number stack_symbol ^clojure.lang.PersistentArrayMap dic_paras]
     (cond (map? lst_tokens) (cond (contains? lst_tokens :parenthesis) (mid_to_forwrod_binaryObject_fun ignite group_id binaryObject dic (reverse (-> lst_tokens :parenthesis)) dic_paras)
                                   (contains? lst_tokens :operation) (mid_to_forwrod_binaryObject_fun ignite group_id binaryObject dic (reverse (-> lst_tokens :operation)) dic_paras)
                                   (and (contains? lst_tokens :item_name) (= String (-> lst_tokens :java_item_type))) {:express (item_type (-> lst_tokens :java_item_type) (-> lst_tokens :item_name)) :java_type java.lang.String :item_type ""}
                                   :else
                                   (throw (Exception. "字符串串错误！")))
           (list? lst_tokens) (let [f (first lst_tokens) r (rest lst_tokens)]
                                  (if (some? f)
                                      (cond (contains? f :operation_symbol) (cond
                                                                                ; 若符号栈为空，则符号直接压入符号栈
                                                                                (= (count stack_symbol) 0) (recur ignite group_id binaryObject dic r stack_number (conj stack_symbol f) dic_paras)
                                                                                ; f 符号的优先级高于或等于符号栈栈顶的优先级，则直接入栈
                                                                                (is-symbol-priority f (peek stack_symbol)) (recur ignite group_id binaryObject dic r stack_number (conj stack_symbol f) dic_paras)
                                                                                ; f 符号的优先级低于栈顶的优先级，则将符号栈顶，弹出参与计算后，在压入，数据栈
                                                                                :else
                                                                                (let [first_item (peek stack_number) second_item (peek (pop stack_number)) top_symbol (peek stack_symbol)]
                                                                                    (recur ignite group_id binaryObject dic r (conj (pop (pop stack_number)) (get_express_obj top_symbol first_item second_item)) (conj (pop stack_symbol) f) dic_paras))
                                                                                )
                                            (contains? f :parenthesis) (when-let [m (mid_to_forwrod_binaryObject_fun ignite group_id binaryObject dic (reverse (-> f :parenthesis)) dic_paras)]
                                                                           (recur ignite group_id binaryObject dic r (conj stack_number (first m)) stack_symbol dic_paras))
                                            (contains? f :item_name) (let [{table_alias :table_alias item_name :item_name java_item_type :java_item_type item_type :item_type} f]
                                                                         (cond (and (contains? dic item_name) (.hasField binaryObject item_name)) (recur ignite group_id binaryObject dic r (conj stack_number (assoc (my-lexical/convert_to_java_type (.get dic item_name)) :express (.field binaryObject item_name) :item_type (.get dic item_name))) stack_symbol dic_paras)
                                                                               (and (= (first item_name) \:) (contains? dic_paras (str/join (rest item_name))))
                                                                               (let [{value :value type :type} (get dic_paras (str/join (rest name)))]
                                                                                   (recur ignite group_id binaryObject dic r (conj stack_number {:express value :java_type type :item_type nil}) stack_symbol dic_paras))
                                                                               :else
                                                                               (if (= table_alias "")
                                                                                   (recur ignite group_id binaryObject dic r (conj stack_number {:express item_name :java_type java_item_type :item_type item_type}) stack_symbol dic_paras)))
                                                                         )
                                            (contains? f :func-name) (recur ignite group_id binaryObject dic r (conj stack_number (func_eval_binary ignite group_id f binaryObject dic)) stack_symbol dic_paras)
                                            :else
                                            (recur ignite group_id binaryObject dic r (conj stack_number f) stack_symbol dic_paras))
                                      (run-express stack_number stack_symbol))))))


















































