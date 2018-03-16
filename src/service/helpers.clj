(ns service.helpers
  (:require [clojure.string :as str]))

(defn tpl [query-str & vars]
  (fn [args]
    (let [values (mapv
                   #(let [result (if (and (keyword? %) (= "+" (namespace %))) [nil (get args (keyword (name %)))] (% args))
                          [query & vars] (if (string? result) [result] result)
                          vars (if (nil? vars) [] vars)]
                      {:query query :vars vars}) vars)
          query-vals (filter #(not (nil? %)) (mapv :query values))
          extras (flatten (mapv :vars values))]
      (into [(apply format query-str query-vals)] extras))))

(defn- single-in [col-name negate size]
  (str col-name (if negate " not" "") " in (" (str/join "," (repeat size "?")) ")"))

(defn where-in-list [col-name list-var & {:keys [list-limit negate] :or {list-limit 0 negate false}}]
  (if (< 0 list-limit)
    (fn [args]
      (let [values (get args list-var)
            c (count values)
            q (int (/ c list-limit))
            r (mod c list-limit)]
        (into [(str "(" (str/join (str ") " (if negate "and" "or") " (")
                                  (into (repeat q (single-in col-name negate list-limit))
                                        (if (zero? r) [] [(single-in col-name negate (count values))]))) ")")]
              values)))
    (fn [args]
      (let [values (get args list-var)]
        (into [(single-in col-name negate (count values))] values)))))

