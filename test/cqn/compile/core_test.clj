(ns cqn.compile.core-test
  (:require [clojure.test :refer :all]
            [cqn.compile.core :as c]))

(deftest test-select
  (is (= (c/build-select-compiler nil false) "Select *"))
  (is (= (c/build-select-compiler nil true) "Select distinct *"))
  (is (= (c/build-select-compiler 'CustomerId false) "Select CustomerId"))
  (is (= (c/build-select-compiler 'CustomerId true) "Select distinct CustomerId"))
  (is (= (c/build-select-compiler '[CustomerId CustomerName] false) "Select CustomerId, CustomerName"))
  (is (= (c/build-select-compiler '[CustomerId/Id CustomerName] false) "Select CustomerId as Id, CustomerName"))
  (is (= (c/build-select-compiler '[CustomerId/Id CustomerName/Name] false) "Select CustomerId as Id, CustomerName as Name"))
  )

(deftest test-select-not-implemented
  (is (= (c/build-select-compiler '{CustomerCount (count)} false) "Select count(*) as CustomerCount"))
  (is (= (c/build-select-compiler '{AvgCustomerAge (avg CustomerAge)} false) "Select avg(CustomerAge) as AvgCustomerAge"))
  )
