(ns cqn.compile.core-test
  (:require [clojure.test :refer :all]
            [cqn.compile.core :as c]))

(deftest test-order-by
  (is (= (c/build-order-by-compiler 'CustomerName) "Order By CustomerName"))
  (is (= (c/build-order-by-compiler 'CustomerName/desc) "Order By CustomerName desc"))
  (is (= (c/build-order-by-compiler '[CustomerName/desc]) "Order By CustomerName desc"))
  (is (= (c/build-order-by-compiler '[CustomerName/desc CustomerAge]) "Order By CustomerName desc, CustomerAge"))
  )

