(ns dao.core
  (:require [clojure.spec.alpha :as s])
  (:import (clojure.lang ExceptionInfo)))

(defn- validator [spec-name error-message]
  (fn [spec]
    (when-not (s/valid? spec-name spec)
      (throw
        (ExceptionInfo.
          error-message
          (s/explain-data spec-name spec))))))

(def ^:private validate-query-spec (validator :cqn.spec/query "Invalid Query:"))

(def ^:private validate-inserter-spec (validator :table/inserter "Invalid Inserter:"))

(def ^:private validate-updater-spec (validator :table/updater "Invalid Updater:"))

(def ^:private validate-deleter-spec (validator :table/deleter "Invalid Deleter:"))

(def ^:private validate-getter-spec (validator :table/getter "Invalid Getter:"))

(def ^:private validate-inquisitor-spec (validator :service/inquisitor "Invalid Inquisitor Service:"))

(defn- validate-query
  ([query-spec]
   (validate-query query-spec {} {}))
  ([query-spec arg-spec arg-types]
   (validate-query-spec query-spec)
   (let []
     ; TODO
     )))

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

(defn build-inquistor-service [inquisitor-spec]
  (validate-inquisitor-spec inquisitor-spec)
  (fn [query-name args]
    ;todo
    ))

(defn build-inserter [inserter-spec]
  (validate-inserter-spec inserter-spec)
  (fn [args]
    ;todo
    ))

(defn build-updater [updater-spec]
  (validate-updater-spec updater-spec)
  (fn [args]
    ;todo
    ))

(defn build-deleter [deleter-spec]
  (validate-deleter-spec deleter-spec)
  (fn [args]
    ;todo
    ))

(defn build-getter [getter-spec]
  (validate-getter-spec getter-spec)
  (fn [args]
    ;todo
    ))

