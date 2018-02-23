(ns cqn.compile.expression
  (:require [cqn.compile.simple :as s]))

(defn compile-params [resolver var-resolver params]
  (mapv (fn [param]
          (println "param: " param)
          (println "param type: " (type param))
          (let [result (cond
                         (keyword? param) (var-resolver param)
                         (symbol? param) (name param)
                        (list? param) (s/optimize-compiler (resolver param) #(str "(" % ")"))
                        (string? param) (s/build-string-literal param)
                        :else (str param))]
            (println "result: " result)
            (println "result type: " (type result))
            result)) params))

(defn func [func-name]
  (fn [resolver var-resolver]
    (fn [& params]
      (println "func-name: " func-name)
      (println "params: " params)
      (let [compiled-params (compile-params resolver var-resolver params)]
        (println "compiled-params: " compiled-params)
        (s/optimize-compilers compiled-params #(str func-name "(" (clojure.string/join "," %) ")"))))))

(defn binary [op]
  (fn [resolver var-resolver]
    (fn [left right]
      (s/optimize-compilers (compile-params resolver var-resolver [left right]) (partial clojure.string/join (str " " op " "))))))

(defn n-ary [op]
  (fn [resolver var-resolver]
    (fn [& params]
      (s/optimize-compilers (compile-params resolver var-resolver params) (partial clojure.string/join (str " " op " "))))))

(defn build-map [func symbols]
  (reduce #(assoc %1 %2 (func (str %2))) {} symbols))

(defn compile-in-expression [col-name where]
  (let [vars (filter keyword? where)
        str-out (str col-name " in (" (clojure.string/join "," (map #(if (keyword? %) "?" (s/stringify %)) where)) ")")]
    (if (empty? vars)
      str-out
      (into [str-out] vars))))

(defn wrap-constantly [func-map]
  (reduce-kv #(assoc %1 %2 (constantly %3)) {} func-map))

(def ^:private expression-func (merge
                                 {'in (fn [resolver _]
                                        (fn [col-name where]
                                          (if (keyword? where)
                                            (fn [args]
                                              (compile-in-expression col-name (get args where)))
                                            (compile-in-expression col-name where))))
                                   }
                                 (wrap-constantly
                                   {'count (fn ([] "count(*)")
                                             ([col-name] (str "count(" col-name ")")))
                                    'custom-fn (fn [func & args]
                                                 (str func "(" (clojure.string/join "," (map s/stringify args)) ")"))
                                    })
                                 (build-map binary '[= < > <> <= >=])
                                 (build-map n-ary '[and or])
                                 (build-map func '[avg min max median sum])))

(defn build-expression-compiler [[func & params]]
  (println "func: " func)
  (let [func ((expression-func func) build-expression-compiler #(fn [args]
                                                                  (s/stringify
                                                                    (get args %))))]
    (apply func params)))

(defn build-where-expression-compiler [[func & params]]
  (println "func: " func)
  (let [func ((expression-func func) build-where-expression-compiler #(vector "?" %))]
    (apply func params)))

