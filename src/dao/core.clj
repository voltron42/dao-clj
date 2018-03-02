(ns dao.core
  (:require [clojure.spec.alpha :as a]
            [clojure.java.jdbc :as jdbc]
            [common.validator :as v]
            [cqn.compile.core :as c]
            [cqn.spec :refer :all]
            [clojure.set :as set]
            [cqn.compile.expression :as x]
            [cqn.compile.simple :as s])
  (:import (clojure.lang ExceptionInfo)))


(defn- get-var-set [query-spec]
  (if (coll? query-spec)
    (let [my-coll (if (map? query-spec)
                    (vec (vals query-spec))
                    (vec query-spec))]
      (set (apply concat (map get-var-set my-coll))))
    (if (keyword? query-spec)
      #{query-spec}
      #{})))

(defn build-api [validate-spec label build-compiler-fn]
  (let [validate (fn validate-func
                   ([spec] (validate-func spec {} {}))
                   ([spec arg-spec table-schema]
                    (validate-spec spec)
                    (let [vars (get-var-set spec)]
                      ;;todo - further validation
                      (fn [args]
                        (let [arg-key-set (set (keys args))]
                          (when-not (set/subset? vars arg-key-set)
                            (throw (ExceptionInfo.
                                     (str "Variables in " label " not accounted for.")
                                     {:missing-vars (set/difference vars arg-key-set)}))))))))
        build-compiler (fn [spec]
                         (let [compiled (build-compiler-fn spec)]
                           (cond
                             (or
                               (string? compiled)
                               (and (vector? compiled)
                                    (every? #(not (keyword? %))
                                            compiled))) (constantly [compiled])
                             (and (vector? compiled)
                                  (some keyword? compiled))
                             (fn query ([args]
                                        (map (fn [elem]
                                               (if (contains? args elem)
                                                 (get args elem)
                                                 elem))
                                             compiled))
                               ([] (query {})))
                             :else (fn query ([] (query {}))
                                     ([args] (let [result (compiled args)]
                                               (if (string? result)
                                                 [result]
                                                 result)))))))]
    {:build-script (fn build-script
                     ([spec]
                      (build-script spec {}))
                     ([spec args]
                      (build-script spec args {}))
                     ([spec args arg-spec]
                      (build-script spec args arg-spec {}))
                     ([spec args arg-spec table-spec]
                      (let [arg-validator (validate spec arg-spec table-spec)
                            compile (build-compiler spec)]
                        (arg-validator args)
                        (compile args))))
     :build-func (fn build-func
                   ([spec]
                    (build-func spec {}))
                   ([spec arg-spec]
                    (build-func spec arg-spec {}))
                   ([spec arg-spec table-spec]
                    (let [arg-validator (validate spec arg-spec table-spec)
                          compile (build-compiler spec)]
                      (fn func
                        ([] (func {}))
                        ([args]
                         (arg-validator args)
                         (compile args))))))
     :build-func-for-func (fn build-func-for-func
                            ([func]
                             (build-func-for-func func {}))
                            ([func arg-spec]
                             (build-func-for-func func arg-spec {}))
                            ([func arg-spec table-spec]
                             (fn func-for-func
                               ([] (func-for-func {}))
                               ([args]
                                (let [spec (func args)
                                      arg-validator (validate spec arg-spec table-spec)
                                      compile (build-compiler spec)]
                                  (arg-validator args)
                                  (compile args))))))}))

(def ^:private query-api (build-api
                           (v/validator :cqn.spec/query "Invalid Query:")
                           "query"
                           c/build-simple-query-compiler))

(def ^:private delete-api (build-api
                            (v/validator :table/deleter "Invalid Deleter:")
                            "delete"
                            (fn [{:keys [table where]}]
                              (let [compile
                                    (x/build-where-expression-compiler
                                      c/build-simple-query-compiler where)]
                                (fn [args] [table (compile args)])))))

(def ^:private update-api (build-api
                            (v/validator :table/updater "Invalid Updater:")
                            "update"
                            (fn [{:keys [table columns where]}]
                              (let [updates
                                    (set/difference
                                      (set (map #(keyword (name %)) columns))
                                      (get-var-set where))
                                    compile
                                    (x/build-where-expression-compiler
                                      c/build-simple-query-compiler where)]
                                (fn [args] [table (select-keys args updates) (compile args)])))))

(def ^:private validate-inserter-spec (v/validator :table/inserter "Invalid Inserter:"))

;; PUBLIC API

;; query

(def build-query (:build-script query-api))

(def build-inquiry (:build-func query-api))

(def build-inquiry-for-func (:build-func-for-func query-api))

;; update

(def build-update (:build-script update-api))

(def build-updater (:build-func update-api))

(def build-updater-for-func (:build-func-for-func update-api))

;; delete

(def build-delete (:build-script delete-api))

(def build-deleter (:build-func delete-api))

(def build-deleter-for-func (:build-func-for-func delete-api))


;; insert

(defn build-inserter [inserter-spec]
  (validate-inserter-spec inserter-spec)
  (fn [args]
    ;todo
    ))

;; getter

(defn build-get [{:keys [name get]} & args]
  (apply build-query (into [(assoc get :from name)] args)))

(defn build-getter [{:keys [name get]} & args]
  (apply build-inquiry (into [(assoc get :from name)] args)))
