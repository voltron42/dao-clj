(ns cqn.compile.select
  (:require [cqn.compile.simple :as s]
            [cqn.compile.expression :as x])
  (:import (clojure.lang ExceptionInfo)))

(defn build-column-compiler [column-spec]
  (cond
    (symbol? column-spec) (s/build-column-name-compiler column-spec)
    (list? column-spec) (x/build-expression-compiler column-spec)
    (map? column-spec) (let [[[alias expression]] (seq column-spec)
                             expression-compiler (x/build-expression-compiler expression)]
                         (s/optimize-compiler expression-compiler #(str % " as " alias)))
    :else (throw (ExceptionInfo. "Invalid Column Spec:" column-spec))))

(defn build-select-compiler [select-spec distinct?]
  (let [header (str "Select " (if (true? distinct?) "Distinct " ""))]
    (if (nil? select-spec)
      (str header "*")
      (let [columns (if (vector? select-spec) select-spec [select-spec])
            column-compilers (map build-column-compiler columns)]
        (s/optimize-compilers column-compilers #(str header (clojure.string/join ", " %)))))))

