(ns dao.core
  (:require [clojure.spec.alpha :as s]
            [clojure.java.jdbc :as jdbc]
            [common.validator :as v]
            [cqn.compile.core :as c]
            [cqn.spec :refer :all]
            [clojure.set :as set])
  (:import (clojure.lang ExceptionInfo)))

(def ^:private validate-query-spec (v/validator :cqn.spec/query "Invalid Query:"))

(def ^:private validate-inserter-spec (v/validator :table/inserter "Invalid Inserter:"))

(def ^:private validate-updater-spec (v/validator :table/updater "Invalid Updater:"))

(def ^:private validate-deleter-spec (v/validator :table/deleter "Invalid Deleter:"))

(def ^:private validate-getter-spec (v/validator :table/getter "Invalid Getter:"))

(defn- get-var-set [query-spec]
  (if (coll? query-spec)
    (let [my-coll (if (map? query-spec)
                    (vec (vals query-spec))
                    (vec query-spec))]
      (set (apply concat (map get-var-set my-coll))))
    (if (keyword? query-spec)
      #{query-spec}
      #{})))

(defn- validate-query
  ([query-spec]
   (validate-query query-spec {} {}))
  ([query-spec arg-spec arg-types]
   (validate-query-spec query-spec)
   (let [vars (get-var-set query-spec)]
     ;;todo - further validation
     (fn [args]
       (let [arg-key-set (set (keys args))]
         (when-not (set/subset? vars arg-key-set)
           (throw (ExceptionInfo.
                    "Variables in query not accounted for."
                    {:missing-vars (set/difference vars arg-key-set)}))))))))

(defn- build-query-compiler [query-spec]
  (let [compiled-query (c/build-simple-query-compiler query-spec)]
    (cond
      (or
        (string? compiled-query)
        (and (vector? compiled-query)
             (every? #(not (keyword? %))
                     compiled-query))) (constantly [compiled-query])
      (and (vector? compiled-query)
           (some keyword? compiled-query))
      (fn query ([args]
           (map (fn [elem]
                  (if (contains? args elem)
                    (get args elem)
                    elem))
                compiled-query))
        ([] (query {})))
      :else (fn query ([] (query {}))
              ([args] (let [result (compiled-query args)]
                        (if (string? result)
                          [result]
                          result)))))))

(defn build-query
  ([query-spec]
   (build-query query-spec {}))
  ([query-spec args]
   (build-query query-spec args {}))
  ([query-spec args arg-spec]
   (build-query query-spec args arg-spec {}))
  ([query-spec args arg-spec table-schema]
   (let [arg-validator (validate-query query-spec arg-spec table-schema)
         query-compiler (build-query-compiler query-spec)]
     (arg-validator args)
     (query-compiler args))))

(defn build-inquiry
  ([query-spec]
   (build-inquiry query-spec {}))
  ([query-spec arg-spec]
   (build-inquiry query-spec arg-spec {}))
  ([query-spec arg-spec table-schema]
   (let [arg-validator (validate-query query-spec arg-spec table-schema)
         query-compiler (build-query-compiler query-spec)]
     (fn query
       ([] (query {}))
       ([args]
        (arg-validator args)
        (query-compiler args))))))

(defn build-inquiry-for-func
  ([query-func]
   (build-inquiry-for-func query-func {}))
  ([query-func arg-spec]
   (build-inquiry-for-func query-func arg-spec {}))
  ([query-func arg-spec table-schema]
   (fn query
     ([] (query {}))
     ([args]
      (let [query-spec (query-func args)
            arg-validator (validate-query query-spec arg-spec table-schema)
            query-compiler (build-query-compiler query-spec)]
        (arg-validator args)
        (query-compiler args))))))

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

(defn build-getter [{:keys [name get]}]
  (build-inquiry (assoc get :from name)))
