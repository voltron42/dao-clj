(ns cqn.compile.core
  (:require [cqn.compile.simple :as s]
            [cqn.compile.expression :as x])
  (:import (clojure.lang ExceptionInfo)))

(defn build-column-compiler [column-spec]
  (cond
    (symbol? column-spec) (s/build-column-name-compiler column-spec)
    (list? column-spec) (x/build-expression-compiler column-spec)
    (map? column-spec) (let [[[alias expression]] (seq column-spec)
                             expression-compiler (x/build-expression-compiler expression)]
                         (s/optimize-compiler expression-compiler #(str % " as " alias)))
    :else (throw (ExceptionInfo. "Invalid Column Spec:" column-spec))))

(defn build-select-compiler [select-spec distinct?]
  (let [header (str "Select " (if (true? distinct?) "distinct " ""))]
    (if (nil? select-spec)
      (str header "*")
      (let [columns (if (vector? select-spec) select-spec [select-spec])
            column-compilers (map build-column-compiler columns)]
        (s/optimize-compilers column-compilers #(str header (clojure.string/join ", " %)))))))

(defn build-from-compiler [from-spec])

(defn build-where-compiler [where-spec]
  (x/build-where-expression-compiler where-spec))
(defn build-group-by-compiler [group-by-spec])
(defn build-having-compiler [having-spec]
  (build-where-compiler having-spec))
(defn build-order-by-compiler [order-by-spec])
(defn build-limit-offset-compiler [limit-spec offset-spec])

(defn build-simple-query-compiler [{:keys [select from where group-by having order-by limit offset distinct] :as simple-query-spec}]
  (let [select-compiler (build-select-compiler select distinct)
        from-compiler (build-from-compiler from)
        where-compiler (build-where-compiler where)
        group-by-compiler (build-group-by-compiler group-by)
        having-compiler (build-having-compiler having)
        order-by-compiler (build-order-by-compiler order-by)
        compilers [select-compiler from-compiler where-compiler group-by-compiler having-compiler order-by-compiler]
        simple-query-compiler (s/optimize-compilers compilers (partial clojure.string/join "\n"))]
    (build-limit-offset-compiler limit offset)))
