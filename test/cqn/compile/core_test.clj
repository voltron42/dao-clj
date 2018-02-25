(ns cqn.compile.core-test
  (:require [clojure.test :refer :all]
            [cqn.compile.core :as c]
            [cqn.compile.expression :as x]))

(deftest test-order-by
  (is (= (c/build-order-by-compiler 'CustomerName) "Order By CustomerName"))
  (is (= (c/build-order-by-compiler 'CustomerName/desc) "Order By CustomerName desc"))
  (is (= (c/build-order-by-compiler '[CustomerName/desc]) "Order By CustomerName desc"))
  (is (= (c/build-order-by-compiler '[CustomerName/desc CustomerAge]) "Order By CustomerName desc, CustomerAge"))
  )

(deftest test-simple-query
  (is (= (c/build-simple-query-compiler '{:from Customers})
         (str "Select *" "\n" "From Customers")))
  )

(deftest test-simple-where
  (is (= (c/build-simple-query-compiler '{:select {list_count (count)}
                                          :from form.formulary_load
                                          :where (= file_load_id :file-load-id)})
         [(str "Select count(*) as list_count" "\n"
               "From form.formulary_load" "\n"
               "Where file_load_id = ?")
          :file-load-id]))
  )

(deftest test-nested-where
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

(deftest test-multiple-columns
  (is (= (c/build-simple-query-compiler
           '{:select [CustomerName City]
             :from Customers})
         (str "Select CustomerName, City" "\n"
              "From Customers")))
  )

(deftest test-distinct
  (is (= (c/build-simple-query-compiler
           '{:select Country
             :from Customers
             :distinct true})
         (str "Select Distinct Country" "\n"
              "From Customers")))
  )

(deftest test-where-in-var
    (is (= (c/build-simple-query-compiler
             '{:from Customers
               :where (in Country ["Germany" "France" "UK"])})
           [(str "Select *" "\n"
                 "From Customers" "\n"
                 "Where Country in (?,?,?)")
            "Germany" "France" "UK"]))
  )

(deftest test-where-in-var
  (let [query-compiler (c/build-simple-query-compiler
                         '{:from Customers
                           :where (in Country :my-list)})]
    (is (= (query-compiler {:my-list ["Germany" "France" "UK"]})
           [(str "Select *" "\n"
                 "From Customers" "\n"
                 "Where Country in (?,?,?)")
            "Germany" "France" "UK"]))
    (is (= (query-compiler {:my-list ["Germany" "France"]})
           [(str "Select *" "\n"
                 "From Customers" "\n"
                 "Where Country in (?,?)")
            "Germany" "France"]))
    (is (= (query-compiler {:my-list ["Germany" "France" "UK" "Denmark"]})
           [(str "Select *" "\n"
                 "From Customers" "\n"
                 "Where Country in (?,?,?,?)")
            "Germany" "France" "UK" "Denmark"]))
    )
  )

(deftest test-where-in-var
  (let [query-compiler (c/build-where-compiler nil
                         '(in Country :my-list))]
    (is (= (query-compiler {:my-list ["Germany" "France" "UK"]})
           ["Where Country in (?,?,?)"
            "Germany" "France" "UK"]))
    (is (= (query-compiler {:my-list ["Germany" "France"]})
           ["Where Country in (?,?)"
            "Germany" "France"]))
    (is (= (query-compiler {:my-list ["Germany" "France" "UK" "Denmark"]})
           ["Where Country in (?,?,?,?)"
            "Germany" "France" "UK" "Denmark"]))
    )
  )

(deftest test-aliased-columns
  (is (= (c/build-simple-query-compiler
           '{:select [CustomerID/ID CustomerName/Customer]
             :from Products})
         (str "Select CustomerID as ID, CustomerName as Customer" "\n"
              "From Products")))
  )

(deftest test-where-like
  (is (= (c/build-simple-query-compiler
           '{:from Customers
             :where (like CustomerName "a%")})
         (str "Select *" "\n"
              "From Customers" "\n"
              "Where CustomerName like 'a%'")))
  )

(deftest test-between-numbers
  (is (= (c/build-simple-query-compiler
           '{:from Products
             :where (between Price 10 20)})
         (str "Select *" "\n"
              "From Products" "\n"
              "Where Price between 10 and 20")))
  )

(deftest test-between-strings
  (is (= (c/build-simple-query-compiler
           '{:from Products
             :where (between ProductName "Carnarvon Tigers" "Mozzarella di Giovanni")
             :order-by ProductName})
         (str "Select *" "\n"
              "From Products" "\n"
              "Where ProductName between 'Carnarvon Tigers' and 'Mozzarella di Giovanni'" "\n"
              "Order By ProductName")))
  )

(deftest test-concat-columns
  (is (= (c/build-simple-query-compiler
           '{:select [CustomerName {Address (concat Address ", " PostalCode ", " City ", " Country)}]
             :from Products})
         (str "Select CustomerName, concat(Address,', ',PostalCode,', ',City,', ',Country) as Address" "\n"
              "From Products")))
  )

(deftest test-where-and-query
  (is (= (c/build-simple-query-compiler
           '{:select w.*
             :from [form.formulary_load/l
                    [INNER form.formulary_webdav/w {l.formulary_load_id w.formulary_load_id}]]
             :where (and
                      (= l.management_status "A")
                      (<= l.effective_date :effective-date)
                      (= w.version :version)
                      (= w.rollup_drug_db :rollup-drug-db)
                      (= l.publisher :publisher)
                      (= l.list_id :list-id)
                      (= l.type :type)
                      (nil? l.sub_type))
             :order-by l.effective_date/desc
             })
         [(str "Select w.*" "\n"
               "From form.formulary_load l" "\n"
               "inner-join form.formulary_webdav w on l.formulary_load_id = w.formulary_load_id" "\n"
               "Where (l.management_status = 'A') and (l.effective_date <= ?) and (w.version = ?) and (w.rollup_drug_db = ?) and (l.publisher = ?) and (l.list_id = ?) and (l.type = ?) and (l.sub_type is null)" "\n"
               "Order By l.effective_date desc")
          :effective-date :version :rollup-drug-db :publisher :list-id :type]))
  )

(deftest test-where-in-query
  (is (= (c/build-simple-query-compiler
           '{:from Customers
             :where (in Country {:select Country
                                 :from Suppliers})})
         (str "Select *" "\n"
              "From Customers" "\n"
              "Where Country in ("
              "Select Country" "\n"
              "From Suppliers"
              ")")))
  )

