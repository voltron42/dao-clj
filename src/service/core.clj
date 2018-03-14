(ns service.core
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.java.jdbc :as jdbc])
  (:import (clojure.lang ExceptionInfo)))

(defn- validate-args [arg-spec args]
  (let [arg-diff (set/difference (set (keys arg-spec)) (set (keys args)))
        present-args (set/difference (set (keys arg-spec)) arg-diff)
        errors (reduce-kv
                 (fn [out arg spec]
                   (if-let [arg-errors (s/explain-data spec (get args arg))]
                     (let [val (-> arg-errors :clojure.spec.alpha/problems first :val)
                           pred (-> arg-errors :clojure.spec.alpha/problems first :pred)
                           spec (-> arg-errors :clojure.spec.alpha/spec)
                           condition (if (= pred :clojure.spec.alpha/unknown) spec pred)]
                       (concat out [{:arg arg
                                     :val val
                                     :cond condition}]))
                     out))
                 (if (empty? arg-diff) [] [{:missing-args arg-diff}])
                 (select-keys arg-spec present-args))]
    (when-not (empty? errors)
      (throw (ExceptionInfo. "Validation Errors" {:errors (vec errors)})))))

(defn- substitute-args [query args]
  (let [missing-args (set/difference (set (rest query)) (set (keys args)))]
    (when-not (empty? missing-args)
      (throw (ExceptionInfo. "Missing Args" {:missing-args missing-args})))
    (into [(first query)] (mapv (partial get args) (rest query)))))

(defn- build-basic
  ([jdbc-func sql-params arg-spec]
    (build-basic jdbc-func [arg-spec {}] sql-params arg-spec {}))
  ([jdbc-func arg-select-objects sql-params arg-spec fixed-values]
   (let [sql-params (if (string? sql-params)
                      [sql-params]
                      sql-params)
         func (if (coll? sql-params)
                (partial substitute-args sql-params)
                (fn [args]
                  (let [result (sql-params args)
                        result (if (string? result) [result] result)]
                    (when-not (vector? result)
                      (throw (ExceptionInfo. "Resulting of sql building function must be either a string or vector" {:sql result :type (type result)})))
                    result)))]
     (fn my-func
       ([] (my-func fixed-values))
       ([args]
        (let [args (merge args fixed-values)]
          (validate-args arg-spec args)
          (let [[args cols] (mapv #(select-keys args (keys %)) arg-select-objects)
                query (func args)]
            (try
              (jdbc-func cols query)
              (catch Throwable e
                (throw (ExceptionInfo.
                         "Database Error"
                         {:message (.getMessage e)
                          :type (type e)
                          :stack-trace (.getStackTrace e)})))))))))))

(defn- fixed-validator [fixed-vals]
  (reduce-kv #(assoc %1 %2 (set (vector %3))) {} fixed-vals))

(defn build-inquisitor [db sql-params & {:keys [opts arg-spec] :or {opts {} arg-spec {}}}]
  (build-basic #(jdbc/query db %2 opts) sql-params arg-spec))

(defn build-deleter [db table where-clause & {:keys [opts arg-spec] :or {opts {} arg-spec {}}}]
  (build-basic #(jdbc/delete! db table %2 opts) where-clause arg-spec))

(defn build-updater [db table column-spec where-clause & {:keys [opts arg-spec fixed-values] :or {opts {} arg-spec {} fixed-values {}}}]
  (build-basic #(jdbc/update! db table %1 %2 opts) [arg-spec (merge column-spec fixed-values)] where-clause (merge column-spec arg-spec (fixed-validator fixed-values)) fixed-values))

(defn build-executor [db sql-params & {:keys [opts arg-spec] :or {opts {} arg-spec {}}}]
  (build-basic #(jdbc/execute! db %2 opts) sql-params arg-spec))

(defn build-inserter [db table column-spec & {:keys [opts] :or {opts {}}}]
  (fn insert
    ([] (insert {}))
    ([args]
     (let [func (if (vector? args)
                  (let [errors (reduce
                                 (fn [out record]
                                   (try
                                     (validate-args column-spec record)
                                     out
                                     (catch ExceptionInfo e
                                       (concat out (vector (.getData e))))))
                                 [] args)]
                    (when-not (empty? errors)
                      (throw (ExceptionInfo. "Validation Errors" {:errors errors})))
                    #(jdbc/insert-multi! db table args opts))
                  (do
                    (validate-args column-spec args)
                    #(jdbc/insert! db table args opts)))]
       (try
         (func)
         (catch Throwable e
           (throw (ExceptionInfo.
                    "Database Error"
                    {:message (.getMessage e)
                     :type (type e)
                     :stack-trace (.getStackTrace e)}))))))))
