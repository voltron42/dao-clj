(ns service.honey-test
  (:require [clojure.test :refer :all]
            [dao.service.honey :refer :all]
            [honeysql.core :as sql]))

(deftest test-q-mark
  (println (pr-str (sql/format {:select [:*] :from [:?/table-name]} {:table-name :customers})))
  )

(deftest test-override-in
  (println (pr-str (sql/format {:where [:in :a.id {:select [:id] :from [:customers] :where [:< :b 25]}]})))
  (println (pr-str (sql/format {:where [:in :a.id [1 2 3]]})))
  (println (pr-str (sql/format [:in :a.id [9 8 7 ]])))
  )

(deftest test-override-in-2
  (with-redefs [list-limit (constantly 3)]
    (println (pr-str (sql/format {:where [:in :a.id (vec (range 1 6))]})))
    (println (pr-str (sql/format {:where [:in :a.id (vec (range 1 7))]})))
    (println (pr-str (sql/format {:where [:in :a.id (vec (range 1 8))]})))
    (println (pr-str (sql/format {:where [:in :a.id (vec (range 1 9))]})))

    (println (pr-str (sql/format {:where [:not-in :a.id (vec (range 1 6))]})))
    (println (pr-str (sql/format {:where [:not-in :a.id (vec (range 1 7))]})))
    (println (pr-str (sql/format {:where [:not-in :a.id (vec (range 1 8))]})))
    (println (pr-str (sql/format {:where [:not-in :a.id (vec (range 1 9))]})))

    ))

(deftest test-validate-join

  (println (pr-str (sql/format {:join [:customer [:= :a.id :customer.id]]})))

  (println (pr-str (sql/format {:join [:customer [:= :a.id :customer.id]
                                       :employee [:and [:= :a.id :employee.id]
                                                  [:= :a.status :employee.status]]]})))

  (println (pr-str (sql/format {:join [[:customer :c] [:= :a.id :c.id]
                                       [:employee :e] [:and [:= :a.id :e.id]
                                                  [:= :a.status :e.status]]]})))

  (println (pr-str (sql/format {:left-join [:customer [:= :a.id :customer.id]]})))

  (println (pr-str (sql/format {:right-join [:customer [:= :a.id :customer.id]]})))

  )
