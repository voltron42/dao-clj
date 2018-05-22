(ns dao.service.honey
  (:require [honeysql.format :as fmt])
  (:import (org.joda.time DateTime)))

(defn list-limit [] 1000)

(defn- primitive? [item]
  (loop [checks [int? decimal? string? (partial instance? DateTime)]]
    (if (empty? checks) false
      (if ((first checks) item) true
        (recur (rest checks))))))

(defn in-not-in [field list-or-query fn-name conjunction]
  (let [clause-fn #(str (fmt/to-sql field) fn-name (fmt/to-sql %))]
    (if (and (vector? list-or-query)
             (every? primitive? list-or-query))
      (if-let [my-limit (list-limit)]
        (loop [my-list list-or-query
               out "("]
          (if (>= my-limit (count my-list))
            (str out (if (zero? (count my-list))
                       "" (clause-fn my-list)) ")")
            (recur (drop my-limit my-list)
                   (str out (clause-fn (take my-limit my-list))
                        (if (< my-limit (count my-list)) conjunction "")))))
        (str (clause-fn list-or-query)))
      (str (clause-fn list-or-query)))))

(defmethod fmt/fn-handler "not in" [_ field list-or-query]
  (in-not-in field list-or-query " NOT IN " " AND "))

(defmethod fmt/fn-handler "in" [_ field list-or-query]
  (in-not-in field list-or-query " IN " " OR "))

