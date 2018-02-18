(ns cqn.core
  (:require [clojure.spec.alpha :as s])
  (:import (clojure.lang ExceptionInfo)))

(defn- validate-query
  ([query-spec]
   (validate-query query-spec {} {}))
  ([query-spec arg-spec arg-types]
    (if-not (s/valid? :cqn.spec/query query-spec)
      (throw (ExceptionInfo.
               "Invalid Query:"
               (s/explain-data :cqn.spec/query query-spec)))
      (let []
        ; TODO
        ))))

(defn- swap-args [query-spec args]
  ; TODO
  )

(defn- format-query [query-spec]
  ; TODO
  )

(defn- validate-args-and-format [query-spec args arg-validator]
  (arg-validator args)
  (let [[swapped-query-spec args] (swap-args query-spec args)
        _ (validate-query swapped-query-spec)
        query (format-query swapped-query-spec)]
    (into [query] args)))

(defn build-query
  ([query-spec args]
   (build-query query-spec args {}))
  ([query-spec args arg-spec]
   (build-query query-spec args arg-spec {}))
  ([query-spec args arg-spec table-schema]
   (let [arg-validator (validate-query query-spec arg-spec table-schema)]
     (validate-args-and-format query-spec args arg-validator))))

(defn build-inquisitor
  ([query-spec]
   (build-inquisitor query-spec {}))
  ([query-spec arg-spec]
   (build-inquisitor query-spec arg-spec {}))
  ([query-spec arg-spec table-schema]
   (let [arg-validator (validate-query query-spec arg-spec table-schema)]
     (fn [args]
       (validate-args-and-format query-spec args arg-validator)))))