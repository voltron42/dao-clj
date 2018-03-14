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
                     (concat out [{:arg arg
                                   :errors arg-errors}])
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
    (build-basic jdbc-func [arg-spec {}] sql-params arg-spec))
  ([jdbc-func arg-select-objects sql-params arg-spec]
   (let [sql-params (if (string? sql-params)
                      [sql-params]
                      sql-params)
         func (if (coll? sql-params)
                (partial substitute-args sql-params)
                sql-params)]
     (fn my-func
       ([] (my-func {}))
       ([args]
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
                        :stack-trace (.getStackTrace e)}))))))))))

(defn build-inquisitor [db sql-params & {:keys [opts arg-spec] :or {opts {} arg-spec {}}}]
  (build-basic #(jdbc/query db %2 opts) sql-params arg-spec))

(defn build-deleter [db table where-clause & {:keys [opts arg-spec] :or {opts {} arg-spec {}}}]
  (build-basic #(jdbc/delete! db table %2 opts) where-clause arg-spec))

(defn build-updater [db table column-spec where-clause & {:keys [opts arg-spec] :or {opts {} arg-spec {}}}]
  (build-basic #(jdbc/update! db table %1 %2 opts) [arg-spec column-spec] where-clause (merge column-spec arg-spec)))

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
