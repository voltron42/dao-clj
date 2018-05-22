(ns dao.common.validations
  (:require [clojure.spec.alpha :as s]))

(defn exact-count [bound]
  (fn [value]
    (try
      (= bound (count value))
      (catch Throwable t
        false))))

(defn min-count [bound]
  (fn [value]
    (try
      (<= bound (count value))
      (catch Throwable t
        false))))

(defn matches? [regex]
  (fn [value]
    (let [matches (re-matches regex value)
          matches (if (coll? matches) matches [matches])]
      (contains? (set matches) value))))

(defn named-as
  ([ns-regex name-regex]
   (fn [value]
     (try
       (and ((matches? ns-regex) (namespace value)) ((matches? name-regex) (name value)))
       (catch Throwable t
         false))))
  ([name-regex]
   (fn [value]
     (try
       (and (nil? (namespace value)) ((matches? name-regex) (name value)))
       (catch Throwable t
         false)))))

(def ^:private date-format-pattern #"/.*")
;todo - create pattern

(defn valid-date-format []
  (matches? date-format-pattern))
