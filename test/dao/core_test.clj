(ns dao.core-test
  (:require [clojure.test :refer :all]
            [dao.core :refer :all])
  (:import (clojure.lang ExceptionInfo)))

(deftest test-build-query
  (is (= (build-query '{:from Customers})
         [(str "Select *" "\n" "From Customers")]))
  (let [query (build-inquiry '{:from Customers})]
    (is (= (query) [(str "Select *" "\n" "From Customers")]))
    (is (= (query {}) [(str "Select *" "\n" "From Customers")]))
    (is (= (query {:some-variable "some value"}) [(str "Select *" "\n" "From Customers")])))
  (is (= (build-query '{:select {list_count (count)}
                        :from form.formulary_load
                        :where (= file_load_id :file-load-id)}
                      {:file-load-id "12345"})
         [(str "Select count(*) as list_count" "\n"
               "From form.formulary_load" "\n"
               "Where file_load_id = ?")
          "12345"]))
  (try
    (build-query '{:select {list_count (count)}
                   :from form.formulary_load
                   :where (= file_load_id :file-load-id)})
    (catch ExceptionInfo e
      (is (= "Variables in query not accounted for." (.getMessage e)))
      (is (= #{:file-load-id} (:missing-vars (.getData e))))))
  (let [query (build-inquiry '{:select {list_count (count)}
                               :from form.formulary_load
                               :where (= file_load_id :file-load-id)})]
    (is (= (query {:file-load-id "12345"})
           [(str "Select count(*) as list_count" "\n"
                 "From form.formulary_load" "\n"
                 "Where file_load_id = ?")
            "12345"]))
    (try
      (query)
      (catch ExceptionInfo e
        (is (= "Variables in query not accounted for." (.getMessage e)))
        (is (= #{:file-load-id} (:missing-vars (.getData e))))
        )))

  (let [query (build-inquiry '{:from Customers
                               :where (and (like CustomerName :pattern)
                                           (in Country :my-list))})]
    (is (= (query {:pattern "A%" :my-list ["Germany" "France" "UK"]})
           [(str "Select *" "\n"
                 "From Customers" "\n"
                 "Where (CustomerName like ?) and (Country in (?,?,?))")
            "A%" "Germany" "France" "UK"]))
    (is (= (query {:pattern "N%" :my-list ["Germany" "France"]})
           [(str "Select *" "\n"
                 "From Customers" "\n"
                 "Where (CustomerName like ?) and (Country in (?,?))")
            "N%" "Germany" "France"]))
    (is (= (query {:pattern "S%" :my-list ["Germany" "France" "UK" "Denmark"]})
           [(str "Select *" "\n"
                 "From Customers" "\n"
                 "Where (CustomerName like ?) and (Country in (?,?,?,?))")
            "S%" "Germany" "France" "UK" "Denmark"]))
    (try
      (query)
      (catch ExceptionInfo e
        (is (= "Variables in query not accounted for." (.getMessage e)))
        (is (= #{:pattern :my-list} (:missing-vars (.getData e))))
        ))
    (try
      (query {:pattern "T%"})
      (catch ExceptionInfo e
        (is (= "Variables in query not accounted for." (.getMessage e)))
        (is (= #{:my-list} (:missing-vars (.getData e))))
        ))
    (try
      (query {:my-list ["Germany" "France" "UK" "Denmark"]})
      (catch ExceptionInfo e
        (is (= "Variables in query not accounted for." (.getMessage e)))
        (is (= #{:pattern} (:missing-vars (.getData e))))
        ))
    )
  )
