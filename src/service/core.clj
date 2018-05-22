(ns service.core
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.java.jdbc :as jdbc]
            [util.exceptions :as x]
            [clojure.string :as str])
  (:import (clojure.lang ExceptionInfo)))

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


(defn- build-basic
  ([db jdbc-func sql-params arg-spec obj-spec default-flag-map opt-names]
    (build-basic db jdbc-func [arg-spec {}] sql-params arg-spec obj-spec {} default-flag-map opt-names))
  ([db jdbc-func arg-select-objects sql-params arg-spec obj-spec fixed-values default-flag-map opt-names]
   (let [default-opts (select-keys default-flag-map opt-names)
         sql-params (if (string? sql-params) [sql-params] sql-params)
         func (if (coll? sql-params)
                (partial substitute-args sql-params)
                (fn [args]
                  (let [result (sql-params args)
                        result (if (string? result) [result] result)]
                    (when-not (vector? result)
                      (throw (ExceptionInfo. "Resulting of sql building function must be either a string or vector"
                                             {:sql result :type (type result)})))
                    result)))]
     (fn my-func
       ([& args]
        (let [[args & {:keys [trx] :or {trx db} :as flag-map}] (if (even? (count args)) (into [{}] args) args)
              opts (merge default-opts (select-keys flag-map opt-names))
              args (merge args fixed-values)
              errors (validate-args arg-spec args)]
          (when-not (empty? errors)
            (throw (ExceptionInfo. "Validation Errors" {:errors errors})))
          (let [[args cols] (mapv #(select-keys args (keys %)) arg-select-objects)
                query (func args)]
            (x/try-catch (jdbc-func trx cols query opts)))))))))

(defn- fixed-validator [fixed-vals]
  (reduce-kv #(assoc %1 %2 (set (vector %3))) {} fixed-vals))

(defn build-inquisitor [db sql-params & {:keys [arg-spec obj-spec] :or {arg-spec {} obj-spec any?} :as flags}]
  (build-basic db #(jdbc/query %1 %3 %4) sql-params arg-spec obj-spec flags [:as-arrays? :identifiers :keywordize? :qualifier :result-set-fn :row-fn]))

(defn build-deleter [db table where-clause & {:keys [arg-spec obj-spec] :or {arg-spec {}} :as flags}]
  (build-basic db #(jdbc/delete! %1 table %3 %4) where-clause arg-spec obj-spec flags [:transaction? :multi?]))

(defn build-updater [db table column-spec where-clause & {:keys [arg-spec fixed-values record-spec] :or {arg-spec {} fixed-values {}} :as flags}]
  (build-basic db #(jdbc/update! %1 table %2 %3 %4)
               [arg-spec (merge column-spec fixed-values)]
               where-clause
               (merge column-spec arg-spec (fixed-validator fixed-values))
               record-spec fixed-values flags [:transaction? :multi?]))

(defn build-executor [db sql-params & {:keys [arg-spec obj-spec] :or {arg-spec {} obj-spec any?} :as flags}]
  (build-basic db #(jdbc/execute! %1 %3 %4) sql-params arg-spec obj-spec flags [:transaction? :multi?]))

(defn- build-insert-all-builder [row-into]
  (fn [row-count]
    (str "insert all \n " (str/join " \n " (repeat row-count row-into)) " \n select * from dual")))

(defn- build-arg-list-builder [arg-cols]
  (fn [out obj]
    (concat out (mapv #(get obj %) arg-cols))))

(defn build-inserter [db table column-spec & {:keys [record-spec special-cols] :or {record-spec any? special-cols {}} :as flags-default}]
  (let [opt-names [:transaction? :entities]
        opts-default (select-keys flags-default opt-names)]
    (if-not (empty? special-cols)
      (let [special-col-list (apply sorted-set (keys special-cols))
            arg-cols (apply sorted-set (keys column-spec))
            cols (concat special-col-list arg-cols)
            values-clause (str/join "," (mapv #(get special-cols % "?") cols))
            row-into (str "into " (name table) " (" (str/join "," (mapv name cols)) ") values (" values-clause ")")
            build-insert-all (build-insert-all-builder row-into)
            build-arg-list (build-arg-list-builder arg-cols)]
        (fn [& args]
          (let [[args & {:keys [trx] :or {trx db} :as flags}] (if (even? (count args)) (into [[]] args) args)
                args (if (vector? args) args [args])
                opts (merge opts-default (select-keys flags opt-names))
                errors (reduce
                         (fn [out record]
                           (concat out
                                   (validate-item record-spec record :full-record)
                                   (validate-args column-spec record)))
                         [] args)]
            (when-not (empty? errors)
              (throw (ExceptionInfo. "Validation Errors" {:errors errors})))
            (when-not (empty? args)
              (jdbc/execute! db (reduce build-arg-list [(build-insert-all (count args))] args) opts)))))
      (fn [& args]
        (let [[args & {:keys [trx] :or {trx db} :as flags}] (if (even? (count args)) (into [{}] args) args)
              opts (merge opts-default (select-keys flags opt-names))
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
          (x/try-catch (func)))))))

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
