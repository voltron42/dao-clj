(ns cqn.compile.expression-test
  (:require [clojure.test :refer :all]
            [cqn.compile.expression :as x]))

(deftest test-count
  (is (= (x/build-expression-compiler '(count)) "count(*)"))
  )

(deftest test-func
  (is (= (x/build-expression-compiler '(avg CustomerAge)) "avg(CustomerAge)"))
  )

(deftest test-binary
  (is (= (x/build-expression-compiler '(= CustomerName "Fred")) "CustomerName = 'Fred'"))
  )

(deftest test-n-ary
  (is (= (x/build-expression-compiler '(or (= CustomerName "Fred") (= CustomerName "George") (= CustomerName "Jerry")))
         "(CustomerName = 'Fred') or (CustomerName = 'George') or (CustomerName = 'Jerry')"))
  )

(deftest test-layered-n-ary
  (is (= (x/build-expression-compiler '(or (= CustomerName "Fred") (and (> CustomerAge 15) (= CustomerName "George"))))
         "(CustomerName = 'Fred') or ((CustomerAge > 15) and (CustomerName = 'George'))"))
  )

(deftest test-binary-w-var
  (is (= ((x/build-expression-compiler '(= CustomerName :name)) {:name "Fred"}) "CustomerName = 'Fred'"))
  )

(deftest test-binary-w-var-where
  (is (= (x/build-where-expression-compiler '(= CustomerName :name)) ["CustomerName = ?" :name]))
  )