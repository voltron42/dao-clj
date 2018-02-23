(ns cqn.compile.core
  (:require [cqn.compile.simple :as s]
            [cqn.compile.expression :as x]
            [cqn.compile.select :as select]
            [cqn.compile.from :as f])
  (:import (clojure.lang ExceptionInfo)))

(defn build-where-compiler [where-spec]
  (s/optimize-compiler (build-where-compiler where-spec) #(str "Where " %))
  )

(defn build-group-by-compiler [group-by-spec])

(defn build-having-compiler [having-spec]
  (s/optimize-compiler (build-where-compiler having-spec) #(str "Having " %))
  )

(defn build-order-by-compiler [order-by-spec]
  (let [order-by-spec (if (vector? order-by-spec) order-by-spec [order-by-spec])]
    (s/optimize-compilers (map s/build-table-name-compiler order-by-spec) #(str "Order By " (clojure.string/join ", " %))))
  )

(defn build-limit-offset-compiler [limit-spec offset-spec simple-query-spec])

(defn build-simple-query-compiler [{:keys [select from where group-by having order-by limit offset distinct] :as simple-query-spec}]
  (let [select-compiler (select/build-select-compiler select distinct)
        from-compiler (f/build-from-compiler from)
        where-compiler (build-where-compiler where)
        group-by-compiler (build-group-by-compiler group-by)
        having-compiler (build-having-compiler having)
        order-by-compiler (build-order-by-compiler order-by)
        compilers [select-compiler from-compiler where-compiler group-by-compiler having-compiler order-by-compiler]
        simple-query-compiler (s/optimize-compilers compilers (partial clojure.string/join "\n"))]
    (build-limit-offset-compiler limit offset simple-query-compiler)))
