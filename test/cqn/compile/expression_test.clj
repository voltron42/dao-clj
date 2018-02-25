(ns cqn.compile.expression-test
  (:require [clojure.test :refer :all]
            [cqn.compile.expression :as x]
            [cqn.compile.core :as c]))

(deftest test-count
  (is (= (x/build-expression-compiler nil '(count)) "count(*)"))
  )

(deftest test-func
  (is (= (x/build-expression-compiler nil '(avg CustomerAge)) "avg(CustomerAge)"))
  )

(deftest test-binary
  (is (= (x/build-expression-compiler nil '(= CustomerName "Fred")) "CustomerName = 'Fred'"))
  )

(deftest test-n-ary
  (is (= (x/build-expression-compiler nil '(or (= CustomerName "Fred") (= CustomerName "George") (= CustomerName "Jerry")))
         "(CustomerName = 'Fred') or (CustomerName = 'George') or (CustomerName = 'Jerry')"))
  )

(deftest test-layered-n-ary
  (is (= (x/build-expression-compiler nil '(or (= CustomerName "Fred") (and (> CustomerAge 15) (= CustomerName "George"))))
         "(CustomerName = 'Fred') or ((CustomerAge > 15) and (CustomerName = 'George'))"))
  )

(deftest test-binary-w-var
  (is (= ((x/build-expression-compiler nil '(= CustomerName :name)) {:name "Fred"}) "CustomerName = 'Fred'"))
  )

(deftest test-binary-w-var-where
  (is (= (x/build-where-expression-compiler nil
           '(= CustomerName :name))
         ["CustomerName = ?" :name]))
  )

(deftest test-in
  (is (= (x/build-expression-compiler nil
           '(in Country ["Germany" "France" "UK"]))
         ["Country in (?,?,?)" "Germany" "France" "UK"]))
  )

(deftest test-in-w-internal-var
  (is (= (x/build-expression-compiler nil
           '(in Country ["Germany" "France" :third]))
         ["Country in (?,?,?)" "Germany" "France" :third]))
  )

(deftest test-in-w-var
  (is (= ((x/build-expression-compiler nil
            '(in Country :countries))
           {:countries ["Germany" "France" "UK"]})
         ["Country in (?,?,?)" "Germany" "France" "UK"]))
  )

(deftest test-custom-fn
  (is (= (x/build-expression-compiler nil '(custom-fn GET_SEQ_DC_NEXT_VAL "SEQ_RFS_LOAD_ID"))
         "GET_SEQ_DC_NEXT_VAL('SEQ_RFS_LOAD_ID')"))
  )

(deftest test-where-like
  (is (= (x/build-expression-compiler nil
           '(like CustomerName "a%"))
         "CustomerName like 'a%'"))
  )

(deftest test-between-numbers
  (is (= (x/build-expression-compiler nil
           '(between Price 10 20))
         "Price between 10 and 20"))
  )

(deftest test-between-strings
  (is (= (x/build-expression-compiler nil
           '(between ProductName "Carnarvon Tigers" "Mozzarella di Giovanni"))
         "ProductName between 'Carnarvon Tigers' and 'Mozzarella di Giovanni'"))
  )

(deftest test-concat-columns
  (is (= (x/build-expression-compiler nil
           '(concat Address ", " PostalCode ", " City ", " Country))
         "concat(Address,', ',PostalCode,', ',City,', ',Country)"))
  )

(deftest test-where-and-query
  (is (= (x/build-expression-compiler nil
           '(nil? l.sub_type))
         "l.sub_type is null"
         )
      )
  )

(deftest test-where-in-query
  (is (= (x/build-expression-compiler
           c/build-simple-query-compiler
           '(in Country {:select Country
                         :from Suppliers}))
         (str "Country in ("
              "Select Country" "\n"
              "From Suppliers"
              ")")))
  )
