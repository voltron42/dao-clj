(ns common.spec
  (:require [common.validations :as v]
            [clojure.spec.alpha :as s]))
(def alias-pattern #"[a-zA-Z][a-zA-Z0-9]*([_][a-zA-Z][a-zA-Z0-9]*)*")

(def variable-name-pattern #"[a-zA-Z][a-zA-Z0-9]*([-][a-zA-Z][a-zA-Z0-9]*)*")

(def custom-function-name-pattern #"[a-zA-Z][a-zA-Z0-9]*([_][a-zA-Z][a-zA-Z0-9]*)*")

(def column-name-pattern #"[a-zA-Z][a-zA-Z0-9]*([_][a-zA-Z][a-zA-Z0-9]*)*")

(def table-dot-star-pattern #"[a-zA-Z][a-zA-Z0-9]*[\.][\*]")

(def table-dot-column-name-pattern #"([a-zA-Z][a-zA-Z0-9]*[\.])?[a-zA-Z][a-zA-Z0-9]*([_][a-zA-Z][a-zA-Z0-9]*)*")

(def table-name-pattern #"([a-zA-Z][a-zA-Z0-9]*[\.])?[a-zA-Z][a-zA-Z0-9]*([_][a-zA-Z][a-zA-Z0-9]*)*")

(def order-pattern #"(asc|desc)")

(s/def ::alias (s/and symbol? (v/named-as alias-pattern)))

(s/def ::variable-name (s/and keyword? (v/named-as variable-name-pattern)))

(s/def ::column-name (s/and symbol? (v/named-as column-name-pattern)))

(s/def ::table-dot-column-name (s/and symbol? (v/named-as table-dot-column-name-pattern)))

(s/def ::aliased-column-name (s/and symbol? (v/named-as table-dot-column-name-pattern alias-pattern)))

(s/def ::table-dot-star (s/and symbol? (v/named-as table-dot-star-pattern)))

(s/def ::number number?)

(s/def ::string string?)

(s/def ::boolean #{true false})

(s/def ::nil nil?)

