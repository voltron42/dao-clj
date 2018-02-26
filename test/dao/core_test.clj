(ns dao.core-test
  (:require [clojure.test :refer :all]
            [dao.core :refer :all])
  (:import (clojure.lang ExceptionInfo)))

(deftest test-build-query
  (is (= (build-query '{:from Customers})
         (str "Select *" "\n" "From Customers")))
  (let [query (build-inquiry '{:from Customers})]
    (is (= (query) (str "Select *" "\n" "From Customers")))
    (is (= (query {}) (str "Select *" "\n" "From Customers")))
    (is (= (query {:some-variable "some value"}) (str "Select *" "\n" "From Customers"))))
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
        (is (= #{:file-load-id} (:missing-vars (.getData e))))))))
