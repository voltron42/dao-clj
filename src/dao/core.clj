(ns dao.core
  (:require [clojure.spec.alpha :as s]
            [clojure.java.jdbc :as jdbc]
            [cqn.compile.core :as c]
            [cqn.spec :refer :all]
            [clojure.set :as set])
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

(def ^:private validate-dao-spec (validator :table/dao "Invalid Table Dao:"))

(def ^:private validate-dao-service-spec (validator :service/dao "Invalid Dao Service:"))

(defn- get-var-set [query-spec]
  (if (coll? query-spec)
    (let [my-coll (if (map? query-spec)
                    (vec (vals query-spec))
                    (vec query-spec))]
      (set (apply concat (map get-var-set query-spec))))
    (if (keyword? query-spec)
      #{query-spec}
      #{})))

(defn- validate-query
  ([query-spec]
   (validate-query query-spec {} {}))
  ([query-spec arg-spec arg-types]
   (validate-query-spec query-spec)
   (let [vars (get-var-set query-spec)]
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
                     compiled-query))) (constantly compiled-query)
      (and (vector? compiled-query)
           (some keyword? compiled-query))
      (fn query ([args]
           (map (fn [elem]
                  (if (contains? args elem)
                    (get args elem)
                    elem))
                compiled-query))
        ([] (query {})))
      :else compiled-query)))

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

(defn inquire
  ([db query-spec]
   (inquire db query-spec {}))
  ([db query-spec opts]
   (inquire db query-spec opts {}))
  ([db query-spec opts args]
   (inquire db query-spec opts args {}))
  ([db query-spec opts args arg-spec]
   (inquire db query-spec opts args arg-spec {}))
  ([db query-spec opts args arg-spec table-schema]
   (let [query-args (build-query query-spec args arg-spec table-schema)]
     (jdbc/query db query-args opts))))

(defn build-inquiry
  ([query-spec]
   (build-inquiry query-spec {}))
  ([query-spec arg-spec]
   (build-inquiry query-spec arg-spec {}))
  ([query-spec arg-spec table-schema]
   (let [arg-validator (validate-query query-spec arg-spec table-schema)
         query-compiler (build-query-compiler query-spec)]
     (fn [args]
       (arg-validator args)
       (query-compiler args)))))

(defn build-inquisitor
  ([db query-spec]
   (build-inquisitor db query-spec {}))
  ([db query-spec opts]
   (build-inquisitor db query-spec opts {}))
  ([db query-spec opts arg-spec]
   (build-inquisitor query-spec opts arg-spec {}))
  ([db query-spec opts arg-spec table-schema]
   (let [inquire (build-inquiry query-spec arg-spec table-schema)]
     (fn [args]
       (jdbc/query db (inquire args) opts)))))

(defn build-inquiry-for-func
  ([query-func]
   (build-inquiry-for-func query-func {}))
  ([query-func arg-spec]
   (build-inquiry-for-func query-func arg-spec {}))
  ([query-func arg-spec table-schema]
   (fn [args]
     (let [query-spec (query-func args)
           arg-validator (validate-query query-spec arg-spec table-schema)
           query-compiler (build-query-compiler query-spec)]
       (arg-validator args)
       (query-compiler args)))))

(defn build-inquisitor-for-func
  ([db query-func]
   (build-inquisitor-for-func query-func {}))
  ([db query-func opts]
   (build-inquisitor-for-func query-func opts {}))
  ([db query-func opts arg-spec]
   (build-inquisitor-for-func query-func opts arg-spec {}))
  ([db query-func opts arg-spec table-schema]
   (let [inquire (build-inquiry-for-func query-func arg-spec table-schema)]
     (fn [args]
       (jdbc/query db (inquire args) opts)))))

(defn build-inquiry-service [inquiry-service-spec]
  (validate-inquisitor-spec inquiry-service-spec)
  (let [service (reduce-kv #(assoc %1 %2 (build-inquiry %3)) {} inquiry-service-spec)]
    (fn [query-name args]
      (if-not (contains? service query-name)
        (throw (ExceptionInfo.
                  "Query does not exist in service:"
                  {:query query-name :queries (keys service)}))
        ((get service query-name) args)))))

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

(defn build-dao [{:keys [name columns insert get update delete] :as dao-spec}]
  (validate-dao-spec dao-spec)
  (merge {}
         (if-not (nil? insert)
           {:create (let [inserters (reduce-kv #(assoc %1 %2 (build-inserter %3)) {} insert)]
                      (fn [label args]
                        (if-not (contains? inserters label)
                          (throw (ExceptionInfo. "Inserter does not exist for label"
                                                 {:label label :inserters (keys inserters)}))
                          ((get inserters label) args))))}
           {})
         (if-not (nil? update)
           {:update (let [updaters (reduce-kv #(assoc %1 %2 (build-updater %3)) {} update)]
                      (fn [label args]
                        (if-not (contains? updaters label)
                          (throw (ExceptionInfo. "Updater does not exist for label"
                                                 {:label label :updaters (keys updaters)}))
                          ((get updaters label) args))))}
           {})
         (if-not (nil? delete)
           {:delete (let [deleters (reduce-kv #(assoc %1 %2 (build-deleter %3)) {} delete)]
                      (fn [label args]
                        (if-not (contains? deleters label)
                          (throw (ExceptionInfo. "Deleter does not exist for label"
                                                 {:label label :deleters (keys deleters)}))
                          ((get deleters label) args))))}
           {})
         (if-not (nil? get)
           {:get (let [getters (reduce-kv #(assoc %1 %2 (build-getter %3)) {} get)]
                      (fn [label args]
                        (if-not (contains? getters label)
                          (throw (ExceptionInfo. "Getter does not exist for label"
                                                 {:label label :getters (keys getters)}))
                          ((get getters label) args))))}
           {})))

(defn build-dao-service [dao-service-spec]
  (validate-dao-service-spec dao-service-spec)
  (let [service (reduce-kv #(assoc %1 %2 (build-dao (assoc %3 :name %2)))
                           {} dao-service-spec)
        build-fn (fn [action]
                   (fn [table label args]
                     (if-not (contains? service table)
                       (throw (ExceptionInfo.
                                "Table not in service:"
                                {:table table :tables (keys service)}))
                       (let [dao (get service table)]
                         (if-not (contains? dao action)
                           (throw (ExceptionInfo.
                                    "Action not available for table:"
                                    {:action action :table table}))
                           ((get dao action) label args))))))]
    (reduce #(assoc %1 %2 (build-fn %2))
            {} [:create :update :delete :get])))