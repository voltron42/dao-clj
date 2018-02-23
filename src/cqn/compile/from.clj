(ns cqn.compile.from
  (:require [cqn.compile.simple :as s]
            [cqn.compile.expression :as x]))

(defn- additional-table
  ([join-type table on]
   {:join join-type
    :table table
    :on on})
  ([table on]
   {:table table
    :on on}))

(defn- on-kv [kv]
  (apply list (cons '= kv)))

(defn- build-table-compiler [table]
  (if (keyword? table)
    (fn [args]
      (s/build-table-name-compiler (get args table)))
    (s/build-table-name-compiler table)))

(defn- build-addl-table-compiler [addl-table-spec]
  (let [{:keys [join table on]} (apply additional-table addl-table-spec)
        join-str (if (nil? join) ", " (str (clojure.string/lower-case (name join)) "-join "))
        table-compiler (build-table-compiler table)
        on-expr (x/build-expression-compiler (if (< 1 (count on))
                                               (cons 'and (map on-kv on))
                                               (on-kv (first on))))]
    (s/optimize-compiler table-compiler #(str join-str % " on " on-expr))))

(defn build-from-compiler [from-spec]
  (let [[table & additional] (if (vector? from-spec) from-spec [from-spec])
        from-compilers (into [(build-table-compiler table)] (map build-addl-table-compiler additional))]
    (s/optimize-compilers from-compilers #(str "From " (clojure.string/join "\n" %)))
    ))

