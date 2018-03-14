(ns service.core-test
  (:require [clojure.test :refer :all]
            [clojure.java.jdbc :as jdbc]
            [service.core :refer :all])
  (:import (clojure.lang ExceptionInfo)))

(deftest test-inquisitor
  (let [db "This is the db!"
        sql "select * from customers"
        inquire (build-inquisitor db sql)
        query-args (atom nil)]
    (with-redefs [jdbc/query #(reset! query-args %&)]
      (inquire)
      (is (= @query-args [db [sql] {}])))))

(deftest test-deleter
  (let [db "This is the db!"
        where "rfs_load_id = ?"
        delete (build-deleter db :rfs_load [where :rfs_load_id] :arg-spec {:rfs_load_id int?})
        delete-args (atom nil)]
    (with-redefs [jdbc/delete! #(reset! delete-args %&)]
      (delete {:rfs_load_id 15})
      (is (= @delete-args [db :rfs_load [where 15] {}]))
      (try
        (delete {:rfs_load_id "15"})
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= (count errors) 1))
            (is (= :rfs_load_id (-> errors first :arg)))
            (is (= "15" (-> errors first :errors :clojure.spec.alpha/problems first :val))))))
      (try
        (delete)
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= errors [{:missing-args #{:rfs_load_id}}]))))))))

