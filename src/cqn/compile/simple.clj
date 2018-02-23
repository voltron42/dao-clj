(ns cqn.compile.simple)

(defn parse-name [named]
  {:my-ns (namespace named)
   :my-name (name named)})

(defn compile-w-args [args]
  (fn [compiler]
    (if (string? compiler)
      compiler
      (compiler args))))

(defn build-string-literal [str-val]
  (str "'" str-val "'"))

(defn stringify [value]
  (cond (string? value) (str "'" value "'")
        :else (str value)))

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
    (if (vector? compiler)
      (into [(func (first compiler))] (rest compiler))
      (fn [args]
        (func ((compile-w-args args) compiler))))))

(defn merge-expressions [func compilers]
  (let [compilers (map #(if (vector? %) % [%]) compilers)
        remaining (apply concat (map rest compilers))
        query (func (map first compilers))]
    (if (empty? remaining)
      query
      (into [query] remaining))))

(defn optimize-compilers [compilers func]
  (if (every? #(or (vector? %) (string? %)) compilers)
    (merge-expressions func compilers)
    (fn [args]
      (func (map (compile-w-args args) compilers)))))

