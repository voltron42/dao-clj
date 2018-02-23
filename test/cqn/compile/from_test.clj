(ns cqn.compile.from-test
  (:require [clojure.test :refer :all]
            [cqn.compile.from :as f]))

(deftest test-from
  (is (= (f/build-from-compiler 'Customer) "From Customer"))
  (is (= (f/build-from-compiler '[Customer]) "From Customer"))
  )

(deftest test-from-aliased
  (is (= (f/build-from-compiler 'Customer/c) "From Customer c"))
  )

(deftest test-from-var-table
  (let [from-compiler (f/build-from-compiler :table-name)]
    (is (= (from-compiler {:table-name 'Customers}) "From Customers"))
    (is (= (from-compiler {:table-name 'Employees}) "From Employees"))
    )
  )

(deftest test-from-join
  (is (= (f/build-from-compiler '[Customer
                                  [Inner Employees {Customer.ID Employees.ID}]])
         (str "From Customer" "\n"
              "inner-join Employees on Customer.ID = Employees.ID")))
  )

(deftest test-from-join-aliased
  (is (= (f/build-from-compiler '[Customer/c
                                  [Inner Employees/e {e.ID c.ID}]])
         (str "From Customer c" "\n"
              "inner-join Employees e on e.ID = c.ID")))
  )

(deftest test-from-join-aliased-multi-on
  (is (= (f/build-from-compiler '[Customer/c
                                  [Inner Employees/e {e.ID c.ID e.Hash c.Hash}]])
         (str "From Customer c" "\n"
              "inner-join Employees e on (e.ID = c.ID) and (e.Hash = c.Hash)")))
  )

