(ns cqn.compile.core
  (:require [cqn.compile.simple :as s]
            [cqn.compile.expression :as x]
            [cqn.compile.select :as select]
            [cqn.compile.from :as f])
  (:import (clojure.lang ExceptionInfo)))

(defn build-where-compiler [build-query-compiler where-spec]
  (s/optimize-compiler
    (x/build-where-expression-compiler build-query-compiler where-spec)
    #(str "Where " %))
  )

(defn build-group-by-compiler [group-by-spec])

(defn build-having-compiler [build-query-compiler having-spec]
  (s/optimize-compiler
    (x/build-where-expression-compiler build-query-compiler having-spec)
    #(str "Having " %))
  )

(defn build-order-by-compiler [order-by-spec]
  (let [order-by-spec (if (vector? order-by-spec) order-by-spec [order-by-spec])]
    (s/optimize-compilers (mapv s/build-table-name-compiler order-by-spec)
                          #(str "Order By " (clojure.string/join ", " %))))
  )

(defn build-limit-offset-compiler [limit-spec offset-spec simple-query-compiler]
  ;todo
    simple-query-compiler)

(defn build-simple-query-compiler [{:keys [select from where group-by having order-by limit offset distinct] :as simple-query-spec}]
  (let [select-compiler (select/build-select-compiler select distinct)
        from-compiler (f/build-from-compiler from)
        compilers (reduce-kv
                    (fn [out k v]
                      (if (contains? simple-query-spec k)
                        (conj out (v))
                        out))
                    [select-compiler from-compiler]
                    {:where #(build-where-compiler build-simple-query-compiler where)
                     :group-by #(build-group-by-compiler group-by)
                     :having #(build-having-compiler build-simple-query-compiler having)
                     :order-by #(build-order-by-compiler order-by)})
        simple-query-compiler (s/optimize-compilers compilers (partial clojure.string/join "\n"))]
    (build-limit-offset-compiler limit offset simple-query-compiler)))
