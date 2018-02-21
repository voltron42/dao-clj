(ns cqn.spec
  (:require [clojure.spec.alpha :as s]
            [common.validations :as v]
            [common.spec :as c]))

(def variable-name-pattern #"[a-zA-Z][a-zA-Z0-9]*([-][a-zA-Z][a-zA-Z0-9]*)*")

(def custom-function-name-pattern #"[a-zA-Z][a-zA-Z0-9]*([_][a-zA-Z][a-zA-Z0-9]*)*")

(def table-dot-star-pattern #"[a-zA-Z][a-zA-Z0-9]*[\.][\*]")

(def entity-pattern #"([a-zA-Z][a-zA-Z0-9]*[\.])*[a-zA-Z][a-zA-Z0-9]*([_][a-zA-Z][a-zA-Z0-9]*)*")

(def order-pattern #"(asc|desc)")

(s/def ::entity (s/and symbol? (v/named-as entity-pattern)))

(s/def ::aliased-entity (s/and symbol? (v/named-as entity-pattern entity-pattern)))

(s/def ::alias ::entity)

(s/def ::variable-name (s/and keyword? (v/named-as variable-name-pattern)))

(s/def ::column-name ::entity)

(s/def ::table-dot-column-name ::entity)

(s/def ::aliased-column-name ::aliased-entity)

(s/def ::table-dot-star (s/and symbol? (v/named-as table-dot-star-pattern)))

(s/def ::number number?)

(s/def ::string string?)

(s/def ::boolean #{true false})

(s/def ::nil nil?)

(s/def ::not (s/cat :label #{'not}
                    :arg ::where))

(s/def ::nil-check (s/cat :label #{'nil? 'not-nil?}
                          :arg ::where))

(s/def ::comparable (s/or :var ::variable-name
                          :literal ::literal
                          :column ::column))

(s/def ::comparison (s/cat :label (set '(= < > <> <= >=))
                           :left ::comparable
                           :right ::comparable))

(s/def ::conjunction (s/cat :label #{'and 'or}
                            :first ::where
                            :rest (s/+ ::where)))

(s/def ::where-in (s/cat :label #{'in}
                         :column (s/or :single ::basic-column
                                       :multi (s/and vector?
                                                     (s/coll-of ::basic-column)))
                         :collection (s/or :var ::variable-name
                                           :single-dim-list (s/and vector?
                                                                   (s/coll-of (s/or :literal ::literal
                                                                                    :var ::variable-name)))
                                           :two-dim-list (s/and vector?
                                                                (s/coll-of (s/or :var ::variable-name
                                                                                 :list (s/or :literal ::literal
                                                                                             :var ::variable-name))))
                                           :query ::query)))

(s/def ::misc-expression (s/or :custom (s/cat :label #{'custom-fn}
                                              :func-name (v/named-as custom-function-name-pattern)
                                              :args (s/* (s/or :literal ::literal
                                                               :var ::variable-name
                                                               :column ::basic-column)))
                               :sysdate (s/cat :label #{'sysdate})
                               :format-date (s/cat :label #{'format-date}
                                                   :col-name ::basic-column
                                                   :format-str (v/valid-date-format))))

(s/def ::where (s/or :var ::variable-name
                     :bool ::boolean
                     :column ::basic-column
                     :expression (s/and list?
                                        (s/or :not ::not
                                              :nil ::nil-check
                                              :comp ::comparison
                                              :conj ::conjunction
                                              :in ::where-in
                                              :misc ::misc-expression))))

(s/def ::literal (s/or :number ::number
                       :string ::string
                       :boolean ::boolean
                       :nil ::nil))

(s/def ::aggregate-expression (s/cat :func-name #{'avg 'min 'max 'median 'sum}
                                     :column ::basic-column))

(s/def ::count-expression (s/cat :func-name #{'count}))

(s/def ::column-expression (s/and list?
                                  (s/or :aggr ::aggregate-expression
                                        :count ::count-expression
                                        :misc ::misc-expression)))

(s/def ::column-untyped (s/or :var-name ::variable-name
                              :expression ::column-expression))

(s/def ::aliased-expression (s/and (v/exact-count 1)
                                   (s/map-of
                                     ::alias
                                     (s/or :literal ::literal
                                           :untyped ::column-untyped
                                           ))))

(s/def ::basic-column (s/or :column-name ::column-name
                            :table-column-name ::table-dot-column-name))

(s/def ::column
  (s/or :basic-column ::basic-column
        :table-star ::table-dot-star
        :aliased ::aliased-column-name
        :literal ::literal
        :untyped ::column-untyped
        :expression ::aliased-expression))

(s/def ::select (s/or :single-column ::column
                      :multi-column (s/and vector?
                                           (v/min-count 1)
                                           (s/coll-of ::column)
                                           (v/no-repeat-columns-or-aliases))))

(s/def ::table-name ::entity)

(s/def ::aliased-table-name ::aliased-entity)

(s/def ::table (s/or :name ::table-name
                     :inner-query ::query))

(s/def ::aliased-table (s/or :name ::aliased-table-name
                             :inner-query (s/and (v/exact-count 1)
                                                 (s/map-of ::alias ::query))))

(s/def ::additional-table (s/and vector?
                                 (s/cat :join #{'INNER 'LEFT-OUTER 'RIGHT-OUTER}
                                        :table ::aliased-table
                                        :on (s/and (v/min-count 1)
                                                   (s/map-of ::table-dot-column-name
                                                             ::table-dot-column-name)))))

(s/def ::from (s/or :single (s/or :table ::table
                                  :aliased ::aliased-table)
                    :join (s/and vector?
                                 (s/cat :first ::aliased-table
                                        :additional (s/+ ::additional-table)))))

(s/def ::group-by (s/and vector?
                         (v/min-count 1)
                         (s/coll-of ::column)))

(s/def ::ordered-column (s/or :column ::basic-column
                              :ordered (v/named-as entity-pattern order-pattern)))

(s/def ::order-by (s/or :single ::ordered-column
                        :multi (s/and vector? (v/min-count 2)
                                      (s/coll-of ::ordered-column))))

(s/def ::limit (s/or :var ::variable-name
                     :literal integer?))

(s/def ::offset (s/or :var ::variable-name
                      :literal integer?))

(s/def ::distinct #{true})

(s/def :table/get (s/keys :opt-un [::select
                                   ::where
                                   ::group-by
                                   ::order-by
                                   ::limit
                                   ::offset
                                   ::distinct]))

(s/def ::simple-query (s/merge :table/get
                               (s/keys :req-un [::from])))

(s/def ::binary-query (s/and vector?
                             (s/cat :label #{'MINUS}
                                    :left ::query
                                    :right ::query)))

(s/def ::n-ary-query (s/and vector?
                            (s/cat :label #{'UNION 'UNION-ALL 'INTERSECT}
                                   :first ::query
                                   :rest (s/+ ::query))))

(s/def ::query (s/or :simple ::simple-query
                     :binary ::binary-query
                     :n-ary ::n-ary-query))

(s/def :service/inquisitor (s/and (v/min-count 1)
                           (s/map-of ::variable-name ::query)))

(s/def :table/name ::table-name)

(s/def :column/type #{})

(s/def :table/column (s/and vector? (s/cat :name ::column-name
                                           :type :column/type
                                           :size (s/? int?)
                                           :nilable (s/? #{:allow-null}))))

(s/def :table/columns (s/and vector? (s/coll-of :table/column)))

(s/def ::table-spec (s/keys :req-un [:table/name :table/columns]))

(s/def :table/insert (s/or :from ::query
                           :special (s/map-of ::column-name ::misc-expression)))

(s/def :update/columns (s/and vector? (s/coll-of ::column-name)))

(s/def :table/update (s/keys :req-un [:update/columns ::where]))

(s/def :table/delete ::where)

(s/def :table/inserter (s/merge ::table-spec (s/keys :req-un [:table/insert])))

(s/def :table/updater (s/merge ::table-spec (s/keys :req-un [:table/update])))

(s/def :table/deleter (s/merge ::table-spec (s/keys :req-un [:table/delete])))

(s/def :table/getter (s/merge ::table-spec (s/keys :req-un [:table/get])))

(s/def :map/get (s/and (v/min-count 1) (s/map-of ::variable-name :table/get)))

(s/def :map/insert (s/and (v/min-count 1) (s/map-of ::variable-name :table/insert)))

(s/def :map/update (s/and (v/min-count 1) (s/map-of ::variable-name :table/update)))

(s/def :map/delete (s/and (v/min-count 1) (s/map-of ::variable-name :table/delete)))

(s/def :table/dao (s/and (v/min-count 1)
                         (s/merge ::table-spec
                                  (s/keys :opt-un [:map/get
                                                   :map/insert
                                                   :map/update
                                                   :map/delete]))))

(s/def :table/group (s/and (v/min-count 1)
                           (s/map-of ::table-name :table/columns)))

(s/def :dao/service (s/and (v/min-count 1)
                           (s/map-of ::table-name
                                     (s/keys :req-un [:table/columns]
                                             :opt-un [:map/get
                                                      :map/insert
                                                      :map/update
                                                      :map/delete]))))

