(ns dao.service
  (:require [clojure.java.jdbc :as jdbc]
            [dao.core :as c]
            [cqn.spec :refer :all]
            [common.validator :as v])
  (:import (clojure.lang ExceptionInfo)))

(def ^:private validate-inquisitor-spec (v/validator :service/inquisitor "Invalid Inquisitor Service:"))

(def ^:private validate-dao-spec (v/validator :table/dao "Invalid Table Dao:"))

(def ^:private validate-dao-service-spec (v/validator :service/dao "Invalid Dao Service:"))

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
   (let [query-args (c/build-query query-spec args arg-spec table-schema)]
     (jdbc/query db query-args opts))))

(defn build-inquisitor
  ([db query-spec]
   (build-inquisitor db query-spec {}))
  ([db query-spec opts]
   (build-inquisitor db query-spec opts {}))
  ([db query-spec opts arg-spec]
   (build-inquisitor query-spec opts arg-spec {}))
  ([db query-spec opts arg-spec table-schema]
   (let [inquire (c/build-inquiry query-spec arg-spec table-schema)]
     (fn query
       ([] (query {}))
       ([args]
        (jdbc/query db (inquire args) opts))))))

(defn build-inquisitor-for-func
  ([db query-func]
   (build-inquisitor-for-func query-func {}))
  ([db query-func opts]
   (build-inquisitor-for-func query-func opts {}))
  ([db query-func opts arg-spec]
   (build-inquisitor-for-func query-func opts arg-spec {}))
  ([db query-func opts arg-spec table-schema]
   (let [inquire (c/build-inquiry-for-func query-func arg-spec table-schema)]
     (fn query
       ([] (query {}))
       ([args] (jdbc/query db (inquire args) opts))))))

(defn build-inquiry-service [inquiry-service-spec]
  (validate-inquisitor-spec inquiry-service-spec)
  (let [service (reduce-kv #(assoc %1 %2 (c/build-inquiry %3)) {} inquiry-service-spec)]
    (fn [query-name args]
      (if-not (contains? service query-name)
        (throw (ExceptionInfo.
                 "Query does not exist in service:"
                 {:query query-name :queries (keys service)}))
        ((get service query-name) args)))))


(defn build-dao [{:keys [name columns insert get update delete] :as dao-spec}]
  (validate-dao-spec dao-spec)
  (merge {}
         (if-not (nil? insert)
           {:create (let [inserters (reduce-kv #(assoc %1 %2 (c/build-inserter %3)) {} insert)]
                      (fn [label args]
                        (if-not (contains? inserters label)
                          (throw (ExceptionInfo. "Inserter does not exist for label"
                                                 {:label label :inserters (keys inserters)}))
                          ((get inserters label) args))))}
           {})
         (if-not (nil? update)
           {:update (let [updaters (reduce-kv #(assoc %1 %2 (c/build-updater %3)) {} update)]
                      (fn [label args]
                        (if-not (contains? updaters label)
                          (throw (ExceptionInfo. "Updater does not exist for label"
                                                 {:label label :updaters (keys updaters)}))
                          ((get updaters label) args))))}
           {})
         (if-not (nil? delete)
           {:delete (let [deleters (reduce-kv #(assoc %1 %2 (c/build-deleter %3)) {} delete)]
                      (fn [label args]
                        (if-not (contains? deleters label)
                          (throw (ExceptionInfo. "Deleter does not exist for label"
                                                 {:label label :deleters (keys deleters)}))
                          ((get deleters label) args))))}
           {})
         (if-not (nil? get)
           {:get (let [getters (reduce-kv #(assoc %1 %2 (c/build-getter %3)) {} get)]
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