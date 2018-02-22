(ns cqn.compile
  (:import (clojure.lang ExceptionInfo)))

(defn parse-name [named]
  {:my-ns (namespace named)
   :my-name (name named)})

(defn compile-w-args[args]
  (fn [compiler]
    (if (string? compiler)
      compiler
      (compiler args))))

(defn build-literal [val]
  (str val))

(defn build-string-literal [str-val]
  (str "'" str-val "'"))

(defn build-variable-compiler [var-name]
  (fn [args]
    (get args var-name)))

(defn build-column-name-compiler [col-name]
  (let [{:keys [my-ns my-name]} (parse-name col-name)]
    (if (nil? my-ns)
      my-name
      (str my-ns " as " my-name))))

(defn build-table-name-compiler [table-name]
  (let [{:keys [my-ns my-name]} (parse-name table-name)]
    (if (nil? my-ns)
      my-name
      (str my-ns " " my-name))))

(defn build-join-type-compiler [join-type]
  (str join-type "-JOIN"))

(defn build-concat-type-compiler [concat-type]
  (str concat-type))

(defn optimize-compiler [compiler func]
  (if (string? compiler)
    (func compiler)
    (fn [args]
      (func ((compile-w-args args) compiler)))))

(defn func [func-name]
  (fn [& params]
    (optimize-compiler params #(str func-name "(" (clojure.string/join "," %) ")"))))

(defn binary [op]
  (fn [left right]
    (optimize-compiler [left right] (partial clojure.string/join op))))

(defn n-ary [op]
  (fn [& params]
    (optimize-compiler params (partial clojure.string/join op))))

(defn build-map [func symbols]
  (reduce #(assoc %1 %2 (func (str %2))) {} symbols))

(defn compile-params [params]
  (map (fn [param]
         (cond
           (symbol? param) (name param)
           (list? param) (build-expression-compiler param)
           (keyword? param) ())) params))

(def ^:private expression-func (merge
                                 {'in (fn [col-name where]
                                        (if (keyword? where)
                                          ()
                                          ()))
                                  'count (fn ([] "count(*)")
                                           ([col-name] (str "count(" col-name ")")))}
                                 (build-map binary '[= < > <> <= >=])
                                 (build-map n-ary '[and or])
                                 (build-map func '[avg min max median sum])))

(defn build-expression-compiler [[func & params]]
  (let [func (expression-func func)]
    (apply func params)))

(defn optimize-compilers [compilers func]
  (if (every? string? compilers)
    (func compilers)
    (fn [args]
        (func ((compile-w-args args) compilers)))))

(defn build-column-compiler [column-spec]
  (cond
    (symbol? column-spec) (build-column-name-compiler column-spec)
    (list? column-spec) (build-expression-compiler column-spec)
    (map? column-spec) (let [[[alias expression]] column-spec
                             expression-compiler (build-expression-compiler expression)]
                         (optimize-compiler expression-compiler #(str % " as " alias)))
    :else (throw (ExceptionInfo. "Invalid Column Spec:" column-spec))))

(defn build-select-compiler [select-spec distinct?]
  (let [header (str "Select " (if (true? distinct?) "distinct " ""))]
    (if (nil? select-spec)
      (str header "*")
      (let [columns (if (vector? select-spec) select-spec [select-spec])
            column-compilers (map build-column-compiler columns)]
        (optimize-compilers column-compilers #(str header (clojure.string/join ", " %)))))))

(defn build-from-compiler [from-spec])

(defn build-where-compiler [where-spec])
(defn build-group-by-compiler [group-by-spec])
(defn build-having-compiler [having-spec]
  (build-where-compiler having-spec))
(defn build-order-by-compiler [order-by-spec])
(defn build-limit-offset-compiler [limit-spec offset-spec])

(defn build-simple-query-compiler [{:keys [select from where group-by having order-by limit offset distinct] :as simple-query-spec}]
  (let [select-compiler (build-select-compiler select distinct)
        from-compiler (build-from-compiler from)
        where-compiler (build-where-compiler where)
        group-by-compiler (build-group-by-compiler group-by)
        having-compiler (build-having-compiler having)
        order-by-compiler (build-order-by-compiler order-by)
        compilers [select-compiler from-compiler where-compiler group-by-compiler having-compiler order-by-compiler]
        simple-query-compiler (optimize-compilers compilers (partial clojure.string/join "\n"))]
    (build-limit-offset-compiler limit offset)))
