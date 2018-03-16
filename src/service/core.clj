(ns service.core
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.java.jdbc :as jdbc])
  (:import (clojure.lang ExceptionInfo)
           (java.sql SQLException)))

(defn- validate-item [spec item label]
  (if-let [{out-spec :clojure.spec.alpha/spec problems :clojure.spec.alpha/problems} (s/explain-data spec item)]
    (mapv (fn [{:keys [val pred]}]
            (let [condition (if (= pred :clojure.spec.alpha/unknown) spec pred)]
              {:label label
               :val val
               :cond condition}))
          problems)
    []))

(defn- validate-args [arg-spec args]
  (let [arg-diff (set/difference (set (keys arg-spec)) (set (keys args)))
        present-args (set/difference (set (keys arg-spec)) arg-diff)]
    (reduce-kv
      (fn [out arg spec]
        (let [errors (validate-item spec (get args arg) arg)]
          (concat out errors)))
      (if (empty? arg-diff) [] [{:missing-args arg-diff}])
      (select-keys arg-spec present-args))))

(defn- substitute-args [query args]
  (let [missing-args (set/difference (set (rest query)) (set (keys args)))]
    (when-not (empty? missing-args)
      (throw (ExceptionInfo. "Missing Args" {:missing-args missing-args})))
    (into [(first query)] (mapv (partial get args) (rest query)))))

(defn- build-errors [x message next-fn obj-map]
  (throw (ExceptionInfo.
           message
           {:errors
            (loop [e x
                   out []]
              (if (nil? e)
                out
                (recur (next-fn e)
                       (concat out
                               (vector
                                 (reduce-kv
                                   #(if-let [result (%3 e)]
                                      (assoc %1 %2 (%3 e))
                                      %1)
                                   {} obj-map))))))})))

(defn stack-trace-elem [^StackTraceElement elem]
  {:class (.getClassName elem)
   :method (.getMethodName elem)
   :file (.getFileName elem)
   :line (.getLineNumber elem)})

(defn wrap-try [func]
  (try
     (func)
     (catch SQLException e
       (build-errors e "SQLException thrown"
                     #(.getNextException ^SQLException %)
                     {:error-code #(.getErrorCode ^SQLException %)
                      :sql-state #(.getSQLState ^SQLException %)
                      :message #(.getMessage ^SQLException %)
                      :stack-trace #(mapv stack-trace-elem (.getStackTrace ^SQLException %))}))
     (catch Throwable e
       (build-errors e "Database Error"
                     #(.getCause %)
                     {:type #(type %)
                      :message #(.getMessage %)
                      :stack-trace #(mapv stack-trace-elem (.getStackTrace %))}))))

(defn- build-basic
  ([db jdbc-func sql-params arg-spec obj-spec]
    (build-basic db jdbc-func [arg-spec {}] sql-params arg-spec obj-spec {}))
  ([db jdbc-func arg-select-objects sql-params arg-spec obj-spec fixed-values]
   (let [sql-params (if (string? sql-params) [sql-params] sql-params)
         func (if (coll? sql-params)
                (partial substitute-args sql-params)
                (fn [args]
                  (let [result (sql-params args)
                        result (if (string? result) [result] result)]
                    (when-not (vector? result)
                      (throw (ExceptionInfo. "Resulting of sql building function must be either a string or vector" {:sql result :type (type result)})))
                    result)))]
     (fn my-func
       ([& args]
        (let [[args & {:keys [trx] :or {trx db}}] (if (even? (count args)) (into [{}] args) args)
              args (merge args fixed-values)
              errors (validate-args arg-spec args)]
          (when-not (empty? errors)
            (throw (ExceptionInfo. "Validation Errors" {:errors errors})))
          (let [[args cols] (mapv #(select-keys args (keys %)) arg-select-objects)
                query (func args)]
            (wrap-try #(jdbc-func trx cols query)))))))))

(defn- fixed-validator [fixed-vals]
  (reduce-kv #(assoc %1 %2 (set (vector %3))) {} fixed-vals))

(defn build-inquisitor [db sql-params & {:keys [opts arg-spec obj-spec] :or {opts {} arg-spec {} obj-spec any?}}]
  (build-basic db #(jdbc/query %1 %3 opts) sql-params arg-spec obj-spec))

(defn build-deleter [db table where-clause & {:keys [opts arg-spec obj-spec] :or {opts {} arg-spec {}}}]
  (build-basic db #(jdbc/delete! %1 table %3 opts) where-clause arg-spec obj-spec))

(defn build-updater [db table column-spec where-clause & {:keys [opts arg-spec fixed-values record-spec] :or {opts {} arg-spec {} fixed-values {}}}]
  (build-basic db #(jdbc/update! %1 table %2 %3 opts) [arg-spec (merge column-spec fixed-values)] where-clause (merge column-spec arg-spec (fixed-validator fixed-values)) record-spec fixed-values))

(defn build-executor [db sql-params & {:keys [opts arg-spec obj-spec] :or {opts {} arg-spec {} obj-spec any?}}]
  (build-basic db #(jdbc/execute! %1 %3 opts) sql-params arg-spec obj-spec))

(defn build-inserter [db table column-spec & {:keys [opts record-spec] :or {opts {} record-spec any?}}]
  (fn [& args]
    (let [[args & {:keys [trx] :or {trx db}}] (if (even? (count args)) (into [{}] args) args)
          func (if (vector? args)
                 (let [errors (reduce
                                (fn [out record]
                                  (concat out
                                          (validate-item record-spec record :full-record)
                                          (validate-args column-spec record)))
                                [] args)]
                   (when-not (empty? errors)
                     (throw (ExceptionInfo. "Validation Errors" {:errors errors})))
                   #(jdbc/insert-multi! trx table args opts))
                 (let [errors (validate-args column-spec args)]
                   (when-not (empty? errors)
                     (throw (ExceptionInfo. "Validation Errors" {:errors errors})))
                   #(jdbc/insert! trx table args opts)))]
      (wrap-try #(func)))))

(def ^:private build-functions {:create build-inserter
                                :read build-inquisitor
                                :update build-updater
                                :delete build-deleter
                                :execute build-executor})

(defn build-dao-service [db dao-spec]
  (let [{:keys [functions errors]} (reduce-kv
                    (fn [{:keys [functions errors]} label [call-type & args]]
                      (if-let [func (get build-functions call-type)]
                        {:functions (assoc functions label (apply func db args))
                         :errors errors}
                        {:functions functions
                         :errors (conj errors label)}))
                    {:functions {}
                     :errors []}
                    dao-spec)]
    (when-not (empty? errors)
      (throw (ExceptionInfo. "Invalid action types" {:labels errors})))
    (fn [label & args]
      (if-let [func (get functions label)]
        (apply func args)
        (throw (ExceptionInfo. "Invalid action in dao service" {:label label}))))))
