(ns cqn.compile.expression
  (:require [cqn.compile.simple :as s]))

(defn compile-params [build-query-compiler resolver var-resolver params]
  (mapv (fn [param]
          (let [result (cond
                         (keyword? param) (var-resolver param)
                         (symbol? param) (name param)
                        (list? param) (s/optimize-compiler ((partial resolver build-query-compiler) param) #(str "(" % ")"))
                        (string? param) (s/build-string-literal param)
                        :else (str param))]
            result)) params))

(defn func [func-name]
  (fn [build-query-compiler resolver var-resolver]
    (fn [& params]
      (let [compiled-params (compile-params build-query-compiler resolver var-resolver params)]
        (s/optimize-compilers compiled-params #(str func-name "(" (clojure.string/join "," %) ")"))))))

(defn binary [op]
  (fn [build-query-compiler resolver var-resolver]
    (fn [left right]
      (s/optimize-compilers (compile-params build-query-compiler resolver var-resolver [left right]) (partial clojure.string/join (str " " op " "))))))

(defn n-ary [op]
  (fn [build-query-compiler resolver var-resolver]
    (fn [& params]
      (s/optimize-compilers (compile-params build-query-compiler resolver var-resolver params) (partial clojure.string/join (str " " op " "))))))

(defn build-map [func symbols]
  (reduce #(assoc %1 %2 (func (str %2))) {} symbols))

(defn compile-in-expression [build-query-compiler col-name where]
  (if (every? #(not (coll? %)) where)
    (into [(str col-name " in (" (clojure.string/join "," (repeat (count where) "?")) ")")] where)
    (str col-name " in (" (build-query-compiler where) ")")
    )
  )

(defn wrap-constantly [func-map]
  (reduce-kv #(assoc %1 %2 (constantly %3)) {} func-map))

(def ^:private expression-func (merge
                                 {'in (fn [build-query-compiler _ _]
                                        (fn [col-name where]
                                          (if (keyword? where)
                                            (fn [args]
                                              (compile-in-expression build-query-compiler col-name (get args where)))
                                            (compile-in-expression build-query-compiler col-name where))))
                                   }
                                 (wrap-constantly
                                   {'count (fn ([] "count(*)")
                                             ([col-name] (str "count(" col-name ")")))
                                    'custom-fn (fn [func & args]
                                                 (str func "(" (clojure.string/join "," (map s/stringify args)) ")"))
                                    'nil? (fn [col-name]
                                            (str col-name " is null"))
                                    'between (fn [& args]
                                               (let [[col-name low high] (map s/stringify args)]
                                                 (str col-name " between " low " and " high)))
                                    })
                                 (build-map binary '[= < > <> <= >= like])
                                 (build-map n-ary '[and or])
                                 (build-map func '[avg min max median sum concat])))

(defn build-expression-compiler [build-query-compiler [func & params]]
  (let [func ((expression-func func) build-query-compiler build-expression-compiler #(fn [args]
                                                                  (s/stringify
                                                                    (get args %))))]
    (apply func params)))

(defn build-where-expression-compiler [build-query-compiler [func & params]]
  (let [func ((expression-func func) build-query-compiler build-where-expression-compiler #(vector "?" %))]
    (apply func params)))

