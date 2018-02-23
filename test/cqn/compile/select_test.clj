(ns cqn.compile.select-test
  (:require [clojure.test :refer :all]
            [cqn.compile.select :refer :all]))

(deftest test-select
  (is (= (build-select-compiler nil false) "Select *"))
  (is (= (build-select-compiler nil true) "Select distinct *"))
  (is (= (build-select-compiler 'CustomerId false) "Select CustomerId"))
  (is (= (build-select-compiler 'CustomerId true) "Select distinct CustomerId"))
  (is (= (build-select-compiler '[CustomerId CustomerName] false) "Select CustomerId, CustomerName"))
  (is (= (build-select-compiler '[CustomerId/Id CustomerName] false) "Select CustomerId as Id, CustomerName"))
  (is (= (build-select-compiler '[CustomerId/Id CustomerName/Name] false) "Select CustomerId as Id, CustomerName as Name"))
  (is (= (build-select-compiler '{CustomerCount (count)} false) "Select count(*) as CustomerCount"))
  (is (= (build-select-compiler '{AvgCustomerAge (avg CustomerAge)} false) "Select avg(CustomerAge) as AvgCustomerAge"))
  )