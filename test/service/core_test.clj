(ns service.core-test
  (:require [clojure.test :refer :all]
            [clojure.java.jdbc :as jdbc]
            [service.core :refer :all]
            [common.validations :as v]
            [clojure.spec.alpha :as s]
            [clojure.string :as str])
  (:import (clojure.lang ExceptionInfo)))

(deftest test-inquisitor-simple
  (let [db "This is the db!"
        sql "select * from customers"
        inquire (build-inquisitor db sql)
        query-args (atom nil)]
    (with-redefs [jdbc/query #(reset! query-args %&)]
      (inquire)
      (is (= @query-args [db [sql] {}]))
      )))

(s/def :query/table-name (v/matches? #"[a-zA-Z][a-zA-Z0-9_]*([/.][a-zA-Z][a-zA-Z0-9_]*)*"))

(deftest test-inquisitor-w-function
  (let [db "This is the db!"
        sql (fn [{:keys [table-name]}] (str "select * from " table-name))
        inquire (build-inquisitor db sql :arg-spec {:table-name :query/table-name})
        query-args (atom nil)]
    (with-redefs [jdbc/query #(reset! query-args %&)]

      (inquire {:table-name "a"})
      (is (= @query-args [db ["select * from a"] {}]))

      (inquire {:table-name "customer"})
      (is (= @query-args [db ["select * from customer"] {}]))

      (inquire {:table-name "form.load"})
      (is (= @query-args [db ["select * from form.load"] {}]))

      (try
        (inquire {:table-name "_customer"})
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= (count errors) 1))
            (is (= :table-name (-> errors first :arg)))
            (is (= "_customer" (-> errors first :val)))
            (is (= "(common.validations/matches? #\"[a-zA-Z][a-zA-Z0-9_]*([/.][a-zA-Z][a-zA-Z0-9_]*)*\")" (-> errors first :cond str)))
            )))
      )))

(deftest test-inquisitor-w-args
  (let [db "This is the db!"
        sql ["select * from customers where customer_id = ?" :customer-id]
        inquire (build-inquisitor db sql :arg-spec {:customer-id int?})
        query-args (atom nil)]
    (with-redefs [jdbc/query #(reset! query-args %&)]
      (inquire {:customer-id 234})
      (is (= @query-args [db ["select * from customers where customer_id = ?" 234] {}])))))

(s/def :load/statuses (s/and set? (s/coll-of #{"A" "B" "C" "D" "F"})))

(deftest test-inquisitor-where-in-fn
  (let [db "This is the db!"
        sql-fn (fn [{:keys [statuses]}]
                 (into [(str
                          "select * from load where status in ("
                          (str/join "," (repeat (count statuses) "?")) ")")]
                       statuses))
        inquire (build-inquisitor db sql-fn :arg-spec {:statuses :load/statuses})
        query-args (atom nil)]
    (with-redefs [jdbc/query #(reset! query-args %&)]

      (inquire {:statuses (sorted-set "A" "B")})
      (is (= @query-args [db ["select * from load where status in (?,?)" "A" "B"] {}]))

      (inquire {:statuses (sorted-set "A" "C" "D")})
      (is (= @query-args [db ["select * from load where status in (?,?,?)" "A" "C" "D"] {}]))

      )))

(deftest test-deleter-generic
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
            (is (= "15" (-> errors first :val)))
            (is (not= "clojure.core/int?" (-> errors first :cond str)))
            (is (= (type int?) (-> errors first :cond type)))
            )))
      (try
        (delete)
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= errors [{:missing-args #{:rfs_load_id}}]))))))))

(s/def :rfs_load/id int?)

(deftest test-deleter-defined
  (let [db "This is the db!"
        where "rfs_load_id = ?"
        delete (build-deleter db :rfs_load [where :rfs_load_id] :arg-spec {:rfs_load_id :rfs_load/id})
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
            (is (= "15" (-> errors first :val)))
            (is (= "clojure.core/int?" (-> errors first :cond str)))
            )))
      (try
        (delete)
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= errors [{:missing-args #{:rfs_load_id}}]))))))))

(deftest test-updater-generic
  (let [db "This is the db!"
        where "file_id = ?"
        update (build-updater db :datafile {:version     #{"10" "30"}
                                            :publisher   (v/matches? #"[DST]000000000[0-9][0-9][0-9][0-9][0-9]")
                                            :action      #{"F" "U"}}
                              [where :file_id]
                              :arg-spec {:file_id int?}
                              :fixed-values {:load_status "L"})
        update-args (atom nil)]
    (with-redefs [jdbc/update! #(reset! update-args %&)]
      (update {:version "10" :action "F" :publisher "D00000000023456" :file_id 54321})
      (is (= @update-args [db :datafile {:version "10" :action "F" :publisher "D00000000023456" :load_status "L"} [where 54321] {}]))
      (try
        (update {:version "10" :action "F" :publisher "D000000023456" :file_id 54321})
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= (count errors) 1))
            (is (= :publisher (-> errors first :arg)))
            (is (= "D000000023456" (-> errors first :val)))
            (is (not= "(common.validations/matches? #\"[DST]000000000[0-9][0-9][0-9][0-9][0-9]\")" (-> errors first :cond str)))
            (is (= (type (v/matches? #"[DST]000000000[0-9][0-9][0-9][0-9][0-9]")) (-> errors first :cond type)))
            )))
      (try
        (update {:version "10" :action "A" :publisher "D00000000023456" :file_id 54321})
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= (count errors) 1))
            (is (= :action (-> errors first :arg)))
            (is (= "A" (-> errors first :val)))
            (is (= #{"F" "U"} (-> errors first :cond)))
            )))
      )))

(s/def :datafile/publisher (v/matches? #"[DST]000000000[0-9][0-9][0-9][0-9][0-9]"))

(deftest test-updater-defined
  (let [db "This is the db!"
        where "file_id = ?"
        update (build-updater db :datafile {:version     #{"10" "30"}
                                            :publisher   :datafile/publisher
                                            :action      #{"F" "U"}}
                              [where :file_id]
                              :arg-spec {:file_id int?}
                              :fixed-values {:load_status "L"})
        update-args (atom nil)]
    (with-redefs [jdbc/update! #(reset! update-args %&)]
      (update {:version "10" :action "F" :publisher "D00000000023456" :file_id 54321})
      (is (= @update-args [db :datafile {:version "10" :action "F" :publisher "D00000000023456" :load_status "L"} [where 54321] {}]))
      (try
        (update {:version "10" :action "F" :publisher "D000000023456" :file_id 54321})
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= (count errors) 1))
            (is (= :publisher (-> errors first :arg)))
            (is (= "D000000023456" (-> errors first :val)))
            (is (= "(common.validations/matches? #\"[DST]000000000[0-9][0-9][0-9][0-9][0-9]\")" (-> errors first :cond str)))

            )))
      (try
        (update {:version "10" :action "A" :publisher "D00000000023456" :file_id 54321})
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= (count errors) 1))
            (is (= :action (-> errors first :arg)))
            (is (= "A" (-> errors first :val)))
            (is (= #{"F" "U"} (-> errors first :cond)))
            )))
      )))

