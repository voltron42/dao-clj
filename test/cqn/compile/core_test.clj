(ns cqn.compile.core-test
  (:require [clojure.test :refer :all]
            [cqn.compile.core :as c]))

(deftest test-order-by
  (is (= (c/build-order-by-compiler 'CustomerName) "Order By CustomerName"))
  (is (= (c/build-order-by-compiler 'CustomerName/desc) "Order By CustomerName desc"))
  (is (= (c/build-order-by-compiler '[CustomerName/desc]) "Order By CustomerName desc"))
  (is (= (c/build-order-by-compiler '[CustomerName/desc CustomerAge]) "Order By CustomerName desc, CustomerAge"))
  )

(deftest test-simple-queries

  (is (= (c/build-simple-query-compiler '{:from Customers})
         (str "Select *" "\n" "From Customers")))

  (is (= (c/build-simple-query-compiler '{:select {list_count (count)}
                                          :from form.formulary_load
                                          :where (= file_load_id :file-load-id)})
         [(str "Select count(*) as list_count" "\n"
               "From form.formulary_load" "\n"
               "Where file_load_id = ?")
          :file-load-id]))

  (is (= (c/build-simple-query-compiler '{:from form.rfs_load
                                          :where (or
                                                   (> modified_date :min-mod-date)
                                                   (and
                                                     (= modified_date :mod-date)
                                                     (> rfs_load_id :rfs-load-id)))
                                          :order-by [modified_date rfs_load_id]})
         [(str "Select *" "\n"
               "From form.rfs_load" "\n"
               "Where (modified_date > ?) or ((modified_date = ?) and (rfs_load_id > ?))" "\n"
               "Order By modified_date, rfs_load_id")
          :min-mod-date
          :mod-date
          :rfs-load-id]))
  )