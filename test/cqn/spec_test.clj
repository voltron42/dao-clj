(ns cqn.spec-test
  (:require [clojure.test :refer :all]
            [clojure.spec.alpha :as s]
            [cqn.spec :as c]))

(deftest test-validation
  (is (= nil (s/explain-data :cqn.spec/query '{:from quantity_limit})))
  
  (is (= nil (s/explain-data :cqn.spec/query '{:select {list_count (count)}
                                       :from form.formulary_load
                                       :where (= file_load_id :file-load-id)})))

  (is (= nil (s/explain-data :cqn.spec/query '{:from form.rfs_load
                                               :where (or
                                                        (> modified_date :min-mod-date)
                                                        (and
                                                          (= modified_date :mod-date)
                                                          (> rfs_load_id :rfs-load-id)))
                                               :order-by [modified_date rfs_load_id]})))

  (is (= nil (s/explain-data :cqn.spec/query '{:select [CustomerName City]
                                               :from Customers})))

  (is (= nil (s/explain-data :cqn.spec/query '{:select Country
                                               :from Customers
                                               :distinct true})))

  (is (= nil (s/explain-data :cqn.spec/query '{:from Customers
                                               :where (in Country ["Germany" "France" "UK"])})))

  (is (= nil (s/explain-data :cqn.spec/query '{:from Customers
                                               :where (in Country {:select Country
                                                                   :from Suppliers})})))

  (is (= nil (s/explain-data :cqn.spec/query '{:select [CustomerID/ID CustomerName/Customer]
                                               :from Products})))

  (is (= nil (s/explain-data :cqn.spec/query '{:from [UNION
                                                      {:select City
                                                       :from Customers}
                                                      {:select City
                                                       :from Suppliers}]
                                               :order-by City})))

  (is (= nil (s/explain-data :cqn.spec/query '{:from [UNION-ALL
                                                      {:select [City Country]
                                                       :from Customers
                                                       :where (= Country "Germany")}
                                                      {:select [City Country]
                                                       :from Suppliers
                                                       :where (= Country "Germany")}]
                                               :order-by City})))

  (is (= nil (s/explain-data :cqn.spec/query '{:from Customers
                                               :where (like CustomerName "a%")})))

  (is (= nil (s/explain-data :cqn.spec/query '{:from Products
                                               :where (between Price 10 20)})))

  (is (= nil (s/explain-data :cqn.spec/query '{:from Products
                                               :where (between ProductName "Carnarvon Tigers" "Mozzarella di Giovanni")
                                               :order-by ProductName})))

  (is (= nil (s/explain-data :cqn.spec/query '{:select [CustomerName {Address (concat Address ", " PostalCode ", " City ", " Country)}]
                                               :from Products})))

  (is (= nil (s/explain-data :cqn.spec/query '{:select w.*
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
                                               })))

  (is (= nil (s/explain-data :cqn.spec/query '{:select [{CustomerCount (count)} Country]
                                               :from Customers
                                               :group-by Country
                                               :order-by CustomerCount/desc})))
  )

