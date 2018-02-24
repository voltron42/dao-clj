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
  (is (= (x/build-where-expression-compiler
           '(= CustomerName :name))
         ["CustomerName = ?" :name]))
  )

(deftest test-in
  (is (= (x/build-expression-compiler
           '(in Country ["Germany" "France" "UK"]))
         ["Country in (?,?,?)" "Germany" "France" "UK"]))
  )

(deftest test-in-w-internal-var
  (is (= (x/build-expression-compiler
           '(in Country ["Germany" "France" :third]))
         ["Country in ('Germany','France',?)" :third]))
  )

(deftest test-in-w-var
  (is (= ((x/build-expression-compiler
            '(in Country :countries))
           {:countries ["Germany" "France" "UK"]})
         ["Country in (?,?,?)" "Germany" "France" "UK"]))
  )

(deftest test-custom-fn
  (is (= (x/build-expression-compiler '(custom-fn GET_SEQ_DC_NEXT_VAL "SEQ_RFS_LOAD_ID"))
         "GET_SEQ_DC_NEXT_VAL('SEQ_RFS_LOAD_ID')"))
  )

