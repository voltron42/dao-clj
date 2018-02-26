(ns dao.core-test
  (:require [clojure.test :refer :all]
            [dao.core :refer :all])
  (:import (clojure.lang ExceptionInfo)))

(deftest test-build-query
  (try
    (let [query (build-query '{:from Customers})]
      (is (= query (str "Select *" "\n" "From Customers"))))
    (catch ExceptionInfo e
      (println (.getMessage e))
      (println (.getData e))
      ))
  )