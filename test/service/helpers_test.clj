(ns service.helpers-test
  (:require [clojure.test :refer :all]
            [service.helpers :as h]))

(deftest test-tpl
  (let [sql-fn (h/tpl "select * from %s" :table-name)]

    (is (= (sql-fn {:table-name "a"}) ["select * from a"]))

    (is (= (sql-fn {:table-name "customers"}) ["select * from customers"]))

    (is (= (sql-fn {:table-name "form.load"}) ["select * from form.load"]))

    ))

(deftest test-tpl-w-vars
  (let [sql-fn (h/tpl "select * from %s where id = ?" :table-name :+/id)]

    (is (= (sql-fn {:table-name "a" :id 235}) ["select * from a where id = ?" 235]))

    (is (= (sql-fn {:table-name "customers" :id 15}) ["select * from customers where id = ?" 15]))

    (is (= (sql-fn {:table-name "form.load" :id "abc"}) ["select * from form.load where id = ?" "abc"]))

    ))

(deftest test-where-in-list
  (let [sql-fn (h/tpl "select * from load where %s" (h/where-in-list "status" :statuses))]

    (is (= (sql-fn {:statuses (sorted-set "A" "B")})
           ["select * from load where status in (?,?)" "A" "B"]))

    (is (= (sql-fn {:statuses (sorted-set "A" "C" "D")})
           ["select * from load where status in (?,?,?)" "A" "C" "D"]))

    ))

(deftest test-where-in-list-w-args-prior
  (let [sql-fn (h/tpl "select * from load where id = ? and %s" :+/id (h/where-in-list "status" :statuses))]

    (is (= (sql-fn {:statuses (sorted-set "A" "B") :id 534})
           ["select * from load where id = ? and status in (?,?)" 534 "A" "B"]))

    (is (= (sql-fn {:statuses (sorted-set "A" "C" "D") :id "def"})
           ["select * from load where id = ? and status in (?,?,?)" "def" "A" "C" "D"]))

    ))
