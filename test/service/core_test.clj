(ns service.core-test
  (:require [clojure.test :refer :all]
            [clojure.java.jdbc :as jdbc]
            [service.core :refer :all]
            [common.validations :as v]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [honeysql.core :as honey]
            [service.helpers :as h]
            [clj-time.core :as t]
            [util.exceptions :as x])
  (:import (clojure.lang ExceptionInfo)
           (java.time DateTimeException)
           (java.util Formatter$DateTime)
           (org.joda.time DateTime)
           (java.sql SQLException)))

(deftest test-wrap-try
  (try
    (x/try-catch (throw (IllegalArgumentException. "This is an exception")))
    (catch ExceptionInfo i
      (is (= "java.lang.IllegalArgumentException: This is an exception" (.getMessage i)))
      (let [{:keys [type message stack-trace] :as data} (.getData i)]
        (is (= #{:type :message :stack-trace} (set (keys data))))
        (is (= "class java.lang.IllegalArgumentException" type))
        (is (= "This is an exception" message))))
    (catch Throwable t
      (is false "should throw ExceptionInfo"))))

(deftest test-inquisitor-simple
  (let [db "This is the db!"
        trx "This is a transaction!"
        sql "select * from customers"
        inquire (build-inquisitor db sql)
        expected-results [5 4 3 2 1]
        query-args (atom nil)]
    (with-redefs [jdbc/query #(do
                                (reset! query-args %&)
                                expected-results)]
      (let [results (inquire)]
        (is (= results expected-results))
        (is (= @query-args [db [sql] {}])))

      (let [results (inquire :trx trx)]
        (is (= results expected-results))
        (is (= @query-args [trx [sql] {}]))))))

(deftest test-inquisitor-simple-w-opts
  (let [db "This is the db!"
        trx "This is a transaction!"
        sql "select * from customers"
        row-fn #(select-keys % [:id :first-name :last-name])
        result-set-fn (partial group-by :last-name)
        opts {:row-fn row-fn
              :result-set-fn result-set-fn}
        inquire (build-inquisitor db sql
                  :row-fn row-fn
                  :result-set-fn result-set-fn)
        expected-results [5 4 3 2 1]
        query-args (atom nil)]
    (with-redefs [jdbc/query #(do
                                (reset! query-args %&)
                                expected-results)]
      (let [results (inquire)]
        (is (= results expected-results))
        (is (= @query-args [db [sql] opts])))

      (let [results (inquire :trx trx)]
        (is (= results expected-results))
        (is (= @query-args [trx [sql] opts]))))))

(deftest test-inquisitor-simple-w-exception
  (let [db "This is the db!"
        sql "select * from customers"
        error-message "this is a sql exception."
        inquire (build-inquisitor db sql)
        expected-results [5 4 3 2 1]
        query-args (atom nil)]
    (with-redefs [jdbc/query #(do
                                (reset! query-args %&)
                                (throw (Exception. error-message)))]
      (try
        (inquire)
        (catch Exception e
          (is (= @query-args [db [sql] {}]))
          (is (instance? ExceptionInfo e))
          (let [{:keys [message type stack-trace] :as data} (.getData e)]
            (is (= #{:message :type :stack-trace} (set (keys data))))
            (is (= message error-message))
            (is (= type "class java.lang.Exception"))))))))

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
            (is (= :table-name (-> errors first :label)))
            (is (= "_customer" (-> errors first :val)))
            (is (= "(common.validations/matches? #\"[a-zA-Z][a-zA-Z0-9_]*([/.][a-zA-Z][a-zA-Z0-9_]*)*\")" (-> errors first :cond str)))))))))

(deftest test-inquisitor-w-honeysql
  (let [db "This is the db!"
        sql (h/tpl "select * from %s" :table-name)
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
            (is (= :table-name (-> errors first :label)))
            (is (= "_customer" (-> errors first :val)))
            (is (= "(common.validations/matches? #\"[a-zA-Z][a-zA-Z0-9_]*([/.][a-zA-Z][a-zA-Z0-9_]*)*\")" (-> errors first :cond str)))))))))

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
        sql-fn (h/tpl "select * from load where %s" (h/where-in-list "status" :statuses))
        inquire (build-inquisitor db sql-fn :arg-spec {:statuses :load/statuses})
        query-args (atom nil)]
    (with-redefs [jdbc/query #(reset! query-args %&)]

      (inquire {:statuses (sorted-set "A" "B")})
      (is (= @query-args [db ["select * from load where status in (?,?)" "A" "B"] {}]))

      (inquire {:statuses (sorted-set "A" "C" "D")})
      (is (= @query-args [db ["select * from load where status in (?,?,?)" "A" "C" "D"] {}])))))

(deftest test-deleter-generic
  (let [db "This is the db!"
        trx "This is a transaction!"
        where "rfs_load_id = ?"
        delete (build-deleter db :rfs_load [where :rfs_load_id] :arg-spec {:rfs_load_id int?})
        delete-args (atom nil)]
    (with-redefs [jdbc/delete! #(reset! delete-args %&)]
      (delete {:rfs_load_id 15})
      (is (= @delete-args [db :rfs_load [where 15] {}]))
      (delete {:rfs_load_id 15} :trx trx)
      (is (= @delete-args [trx :rfs_load [where 15] {}]))
      (try
        (delete {:rfs_load_id "15"})
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= (count errors) 1))
            (is (= :rfs_load_id (-> errors first :label)))
            (is (= "15" (-> errors first :val)))
            (is (not= "clojure.core/int?" (-> errors first :cond str)))
            (is (= (type int?) (-> errors first :cond type))))))
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
            (is (= :rfs_load_id (-> errors first :label)))
            (is (= "15" (-> errors first :val)))
            (is (= "clojure.core/int?" (-> errors first :cond str))))))
      (try
        (delete)
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= errors [{:missing-args #{:rfs_load_id}}]))))))))

(deftest test-updater-generic
  (let [db "This is the db!"
        trx "This is a transaction!"
        where "file_id = ?"
        update (build-updater db :datafile
                              {:version     #{"10" "30"}
                               :publisher   (v/matches? #"[DST]000000000[0-9][0-9][0-9][0-9][0-9]")
                               :action      #{"F" "U"}}
                              [where :file_id]
                              :arg-spec {:file_id int?}
                              :fixed-values {:load_status "L"})
        update-args (atom nil)]
    (with-redefs [jdbc/update! #(reset! update-args %&)]
      (update {:version "10"
               :action "F"
               :publisher "D00000000023456"
               :file_id 54321})
      (is (= [db :datafile {:version "10"
                            :action "F"
                            :publisher "D00000000023456"
                            :load_status "L"}
              [where 54321] {}]
             @update-args))
      (update {:version "10"
               :action "F"
               :publisher "D00000000023456"
               :file_id 54321}
              :trx trx)
      (is (= [trx :datafile {:version "10"
                            :action "F"
                            :publisher "D00000000023456"
                            :load_status "L"}
              [where 54321] {}]
             @update-args))
      (try
        (update {:version "10" :action "F" :publisher "D000000023456" :file_id 54321})
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= (count errors) 1))
            (is (= :publisher (-> errors first :label)))
            (is (= "D000000023456" (-> errors first :val)))
            (is (not= "(common.validations/matches? #\"[DST]000000000[0-9][0-9][0-9][0-9][0-9]\")" (-> errors first :cond str)))
            (is (= (type (v/matches? #"[DST]000000000[0-9][0-9][0-9][0-9][0-9]")) (-> errors first :cond type))))))

      (try
        (update {:version "10" :action "A" :publisher "D00000000023456" :file_id 54321})
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= (count errors) 1))
            (is (= :action (-> errors first :label)))
            (is (= "A" (-> errors first :val)))
            (is (= #{"F" "U"} (-> errors first :cond)))))))))

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
      (is (= [db :datafile {:version "10" :action "F" :publisher "D00000000023456" :load_status "L"} [where 54321] {}] @update-args))

      (try
        (update {:version "10" :action "F" :publisher "D000000023456" :file_id 54321})
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= (count errors) 1))
            (is (= :publisher (-> errors first :label)))
            (is (= "D000000023456" (-> errors first :val)))
            (is (= "(common.validations/matches? #\"[DST]000000000[0-9][0-9][0-9][0-9][0-9]\")" (-> errors first :cond str))))))

      (try
        (update {:version "10" :action "A" :publisher "D00000000023456" :file_id 54321})
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= (count errors) 1))
            (is (= :action (-> errors first :label)))
            (is (= "A" (-> errors first :val)))
            (is (= #{"F" "U"} (-> errors first :cond)))))))))

(s/def :customer/name (v/matches? #"[A-Z][a-z]*"))

(deftest test-executor
  (let [db "This is the db!"
        trx "This is a transaction!"
        sql ["insert into customer (id,first_name,last_name) values (customer_id_seq(),?,?)"
             :first-name :last-name]
        execute (build-executor db sql
                                :arg-spec {:first-name :customer/name
                                           :last-name :customer/name})
        execute-args (atom nil)]
    (with-redefs [jdbc/execute! #(reset! execute-args %&)]
      (execute {:first-name "Steve" :last-name "Dave"})
      (is (= @execute-args [db ["insert into customer (id,first_name,last_name) values (customer_id_seq(),?,?)" "Steve" "Dave"] {}]))

      (execute {:first-name "Steve" :last-name "Dave"} :trx trx)
      (is (= @execute-args [trx ["insert into customer (id,first_name,last_name) values (customer_id_seq(),?,?)" "Steve" "Dave"] {}])))))

(s/def :customer/date-of-birth (partial instance? DateTime))

(deftest test-inserter
  (let [db "This is the db!"
        insert (build-inserter db :customer
                               {:first-name    :customer/name
                                :last-name     :customer/name
                                :date-of-birth :customer/date-of-birth})
        insert-args (atom nil)
        insert-multi-args (atom nil)]
    (with-redefs [jdbc/insert! #(reset! insert-args %&)
                  jdbc/insert-multi! #(reset! insert-multi-args %&)]
      (insert {:first-name "Steve"
               :last-name "Dave"
               :date-of-birth (t/date-time 1998 2 5)})
      (let [[insert-db table {:keys [first-name last-name date-of-birth]} opts] @insert-args]
        (is (= insert-db db))
        (is (= table :customer))
        (is (= first-name "Steve"))
        (is (= last-name "Dave"))
        (is (and (not (nil? date-of-birth)) (t/equal? date-of-birth (t/date-time 1998 2 5))))
        (is (= opts {})))

      (insert [{:first-name "Steve"
                :last-name "Dave"
                :date-of-birth (t/date-time 1998 2 5)}
               {:first-name "George"
                :last-name "Kaplan"
                :date-of-birth (t/date-time 1987 5 17)}])
      (let [[insert-db table [{:keys [first-name last-name date-of-birth]} {first :first-name last :last-name dob :date-of-birth}] opts] @insert-multi-args]
        (is (= insert-db db))
        (is (= table :customer))
        (is (= first-name "Steve"))
        (is (= last-name "Dave"))
        (is (and (not (nil? date-of-birth)) (t/equal? date-of-birth (t/date-time 1998 2 5))))
        (is (= first "George"))
        (is (= last "Kaplan"))
        (is (and (not (nil? dob)) (t/equal? dob (t/date-time 1987 5 17))))
        (is (= opts {})))

      (try
        (insert {:first-name "Steve"
                 :last-name "Dave"
                 :date-of-birth "1987-07-12"})
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= (count errors) 1))
            (let [[{:keys [label val cond]}] errors]
              (is (= label :date-of-birth))
              (is (= val "1987-07-12"))
              (is (= (str cond) "(clojure.core/partial clojure.core/instance? org.joda.time.DateTime)")))))))))

(deftest test-inserter-special
  (let [db "This is the db!"
        insert (build-inserter db :customer
                               {:first-name    :customer/name
                                :last-name     :customer/name
                                :date-of-birth :customer/date-of-birth}
                               :special-cols
                               {:id "customer_id_seq()"})
        insert-args (atom nil)
        insert-multi-args (atom nil)
        execute-args (atom nil)
        dob-1 (t/date-time 1998 2 5)
        dob-2 (t/date-time 1987 5 17)
        row-stmt "into customer (id,date-of-birth,first-name,last-name) values (customer_id_seq(),?,?,?)"
        ]
    (with-redefs [jdbc/insert! #(reset! insert-args %&)
                  jdbc/insert-multi! #(reset! insert-multi-args %&)
                  jdbc/execute! #(reset! execute-args %&)
                  ]
      (insert {:first-name "Steve"
               :last-name "Dave"
               :date-of-birth dob-1})
      (let [[insert-db [insert & args] opts] @execute-args
            [prefix & rows] (str/split insert #" \n ")
            [suffix rows] (mapv #(% rows) [last drop-last])
            ]
        (is (= insert-db db))
        (is (= prefix "insert all"))
        (is (= suffix "select * from dual"))
        (is (= (count rows) 1))
        (is (every? (partial = row-stmt) rows))

        (is (= args [dob-1
                     "Steve"
                     "Dave"]))

        (is (= opts {})))

      (insert [{:first-name "Steve"
                :last-name "Dave"
                :date-of-birth dob-1}
               {:first-name "George"
                :last-name "Kaplan"
                :date-of-birth dob-2}])
      (let [[insert-db [insert & args] opts] @execute-args
            [prefix & rows] (str/split insert #" \n ")
            [suffix rows] (mapv #(% rows) [last drop-last])
            ]
        (is (= insert-db db))
        (is (= prefix "insert all"))
        (is (= suffix "select * from dual"))
        (is (= (count rows) 2))
        (is (every? (partial = row-stmt) rows))

        (is (= args [dob-1
                     "Steve"
                     "Dave"
                     dob-2
                     "George"
                     "Kaplan"]))

        (is (= opts {})))

      (try
        (insert {:first-name "Steve"
                 :last-name "Dave"
                 :date-of-birth "1987-07-12"})
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= (count errors) 1))
            (let [[{:keys [label val cond]}] errors]
              (is (= label :date-of-birth))
              (is (= val "1987-07-12"))
              (is (= (str cond) "(clojure.core/partial clojure.core/instance? org.joda.time.DateTime)")))))))))

(deftest test-dao-service
  (let [db "This is the db!"
        db-call (build-dao-service
                  db {:customers/get-all [:read "select * from customers"]

                      :get-from [:read (h/tpl "select * from %s" :table-name)
                                 :arg-spec {:table-name :query/table-name}]

                      :get-from-by-id [:read (h/tpl "select * from %s where %s = ?" :table-name :id-name :?/id)
                                       :arg-spec {:table-name :query/table-name
                                                  :id-name :query/table-name
                                                  :id int?}]

                      :customers/get-by-id [:read ["select * from customers where customer_id = ?" :customer-id]
                                           :arg-spec {:customer-id int?}]

                      :employees/get-by-id [:read (partial honey/format {:select [:*]
                                                                        :from [:employee]
                                                                        :where [:= :employee.id :?employee-id]})
                                           :arg-spec {:employee-id int?}]

                      :load/get-by-statuses [:read (h/tpl "select * from load where %s" (h/where-in-list "status" :statuses))
                                             :arg-spec {:statuses :load/statuses}]

                      :load/get-by-actions [:read (partial honey/format {:select [:*]
                                                                         :from [:load]
                                                                         :where [:in :action :?actions]})
                                            :arg-spec {:actions :load/statuses}]

                      :load/get-by-statuses-and-action [:read (h/tpl "select * from load where action = ? and %s" :?/action (h/where-in-list "status" :statuses))
                                                        :arg-spec {:statuses :load/statuses
                                                                   :action #{"C" "R" "U" "D"}}]

                      :rfs-load/delete-by-id [:delete :rfs_load ["rfs_load_id = ?" :rfs_load_id]
                                              :arg-spec {:rfs_load_id :rfs_load/id}]

                      :datafile/update-from-header [:update :datafile {:version     #{"10" "30"}
                                                                       :publisher   :datafile/publisher
                                                                       :action      #{"F" "U"}}
                                                    ["file_id = ?" :file_id]
                                                    :arg-spec {:file_id int?}
                                                    :fixed-values {:load_status "L"}]

                      :customer/create-with-seq-id [:execute ["insert into customer (id,first_name,last_name) values (customer_id_seq(),?,?)"
                                                              :first-name :last-name]
                                                    :arg-spec {:first-name :customer/name
                                                               :last-name :customer/name}]

                      :customer/create-special [:create :customer
                                                {:id int?
                                                 :first-name :customer/name
                                                 :last-name :customer/name}
                                                :special-cols
                                                {:id "customer_id_seq()"}]

                      :customer/create [:create :customer
                                        {:first-name    :customer/name
                                         :last-name     :customer/name
                                         :date-of-birth :customer/date-of-birth}]})
        jdbc-args (atom nil)]
    (with-redefs [jdbc/query #(reset! jdbc-args (into [:query] %&))
                  jdbc/delete! #(reset! jdbc-args (into [:delete!] %&))
                  jdbc/insert! #(reset! jdbc-args (into [:insert!] %&))
                  jdbc/update! #(reset! jdbc-args (into [:update!] %&))
                  jdbc/insert-multi! #(reset! jdbc-args (into [:insert-multi!] %&))
                  jdbc/execute! #(reset! jdbc-args (into [:execute!] %&))]
      (db-call :customers/get-all)
      (is (= [:query db ["select * from customers"] {}] @jdbc-args))

      (db-call :get-from {:table-name "a"})
      (is (= [:query db ["select * from a"] {}] @jdbc-args))

      (db-call :get-from {:table-name "customer"})
      (is (= [:query db ["select * from customer"] {}] @jdbc-args))

      (db-call :get-from {:table-name "form.load"})
      (is (= [:query db ["select * from form.load"] {}] @jdbc-args))

      (try
        (db-call :get-from {:table-name "_customer"})
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= (count errors) 1))
            (is (= :table-name (-> errors first :label)))
            (is (= "_customer" (-> errors first :val)))
            (is (= "(common.validations/matches? #\"[a-zA-Z][a-zA-Z0-9_]*([/.][a-zA-Z][a-zA-Z0-9_]*)*\")" (-> errors first :cond str))))))

      (db-call :customers/get-by-id {:customer-id 234})
      (is (= [:query db ["select * from customers where customer_id = ?" 234] {}] @jdbc-args))

      (db-call :employees/get-by-id {:employee-id 234})
      (is (= [:query db ["SELECT * FROM employee WHERE employee.id = ?" 234] {}] @jdbc-args))

      (db-call :load/get-by-statuses {:statuses (sorted-set "A" "B")})
      (is (= [:query db ["select * from load where status in (?,?)" "A" "B"] {}] @jdbc-args))

      (db-call :load/get-by-statuses {:statuses (sorted-set "A" "C" "D")})
      (is (= [:query db ["select * from load where status in (?,?,?)" "A" "C" "D"] {}] @jdbc-args))

      (db-call :load/get-by-actions {:actions (sorted-set "A" "B")})
      (is (= [:query db ["SELECT * FROM load WHERE action IN (?, ?)" "A" "B"] {}] @jdbc-args))

      (db-call :load/get-by-actions {:actions (sorted-set "A" "C" "D")})
      (is (= [:query db ["SELECT * FROM load WHERE action IN (?, ?, ?)" "A" "C" "D"] {}] @jdbc-args))

      (db-call :delete-rfs-load-by-id {:rfs_load_id 15})
      (is (= [:delete! db :rfs_load ["rfs_load_id = ?" 15] {}] @jdbc-args))

      (try
        (db-call :delete-rfs-load-by-id  {:rfs_load_id "15"})
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= (count errors) 1))
            (is (= :rfs_load_id (-> errors first :label)))
            (is (= "15" (-> errors first :val)))
            (is (= "clojure.core/int?" (-> errors first :cond str))))))

      (try
        (db-call :delete-rfs-load-by-id)
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= errors [{:missing-args #{:rfs_load_id}}])))))

      (db-call :update-datafile-from-header {:version "10" :action "F" :publisher "D00000000023456" :file_id 54321})
      (is (= [:update! db :datafile {:version "10" :action "F" :publisher "D00000000023456" :load_status "L"} ["file_id = ?" 54321] {}] @jdbc-args))

      (try
        (db-call :update-datafile-from-header {:version "10" :action "F" :publisher "D000000023456" :file_id 54321})
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= (count errors) 1))
            (is (= :publisher (-> errors first :label)))
            (is (= "D000000023456" (-> errors first :val)))
            (is (= "(common.validations/matches? #\"[DST]000000000[0-9][0-9][0-9][0-9][0-9]\")" (-> errors first :cond str))))))

      (try
        (db-call :update-datafile-from-header {:version "10" :action "A" :publisher "D00000000023456" :file_id 54321})
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= (count errors) 1))
            (is (= :action (-> errors first :label)))
            (is (= "A" (-> errors first :val)))
            (is (= #{"F" "U"} (-> errors first :cond))))))

      (db-call :create-customer-with-seq-id {:first-name "Steve" :last-name "Dave"})
      (is (= [:execute! db ["insert into customer (id,first_name,last_name) values (customer_id_seq(),?,?)" "Steve" "Dave"] {}] @jdbc-args))


      (db-call :create-customer {:first-name "Steve"
                                 :last-name "Dave"
                                 :date-of-birth (t/date-time 1998 2 5)})
      (let [[label insert-db table {:keys [first-name last-name date-of-birth]} opts] @jdbc-args]
        (is (= label :insert!))
        (is (= insert-db db))
        (is (= table :customer))
        (is (= first-name "Steve"))
        (is (= last-name "Dave"))
        (is (and (not (nil? date-of-birth)) (t/equal? date-of-birth (t/date-time 1998 2 5))))
        (is (= opts {})))

      (db-call :create-customer
               [{:first-name "Steve"
                :last-name "Dave"
                :date-of-birth (t/date-time 1998 2 5)}
               {:first-name "George"
                :last-name "Kaplan"
                :date-of-birth (t/date-time 1987 5 17)}])
      (let [[label insert-db table [{:keys [first-name last-name date-of-birth]} {first :first-name last :last-name dob :date-of-birth}] opts] @jdbc-args]
        (is (= label :insert-multi!))
        (is (= insert-db db))
        (is (= table :customer))
        (is (= first-name "Steve"))
        (is (= last-name "Dave"))
        (is (and (not (nil? date-of-birth)) (t/equal? date-of-birth (t/date-time 1998 2 5))))
        (is (= first "George"))
        (is (= last "Kaplan"))
        (is (and (not (nil? dob)) (t/equal? dob (t/date-time 1987 5 17))))
        (is (= opts {})))

      (try
        (db-call :create-customer
                 {:first-name "Steve"
                  :last-name "Dave"
                  :date-of-birth "1987-07-12"})
        (is false "should throw exception")
        (catch ExceptionInfo e
          (let [{:keys [errors]} (.getData e)]
            (is (= (count errors) 1))
            (let [[{:keys [label val cond]}] errors]
              (is (= label :date-of-birth))
              (is (= val "1987-07-12"))
              (is (= (str cond) "(clojure.core/partial clojure.core/instance? org.joda.time.DateTime)")))))))))
