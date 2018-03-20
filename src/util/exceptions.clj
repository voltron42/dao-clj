(ns util.exceptions
  (:import (java.sql SQLException)
           (clojure.lang ExceptionInfo)))

(defn- stack-trace-elem [^StackTraceElement elem]
  {:class (.getClassName elem)
   :method (.getMethodName elem)
   :file (.getFileName elem)
   :line (.getLineNumber elem)})

(defn wrap-stack-trace [^Throwable t]
  (mapv stack-trace-elem (.getStackTrace t)))

(defn- filter-non-nil [data]
  (reduce-kv #(if (nil? %3) %1 (assoc %1 %2 %3)) {} data))

(defmulti wrap-exception-by-type type)

(defn wrap-exception [^Throwable throwable]
  (when-not (nil? throwable)
    (filter-non-nil (wrap-exception-by-type throwable))))

(defmethod wrap-exception-by-type :default [^Throwable throwable]
  {:type (str (type throwable))
   :cause (wrap-exception (.getCause throwable))
   :message (.getMessage throwable)
   :stack-trace (wrap-stack-trace throwable)})

(defmethod wrap-exception-by-type SQLException [^SQLException exception]
  {:message (.getMessage exception)
   :error-code (.getErrorCode exception)
   :state (.getSQLState exception)
   :cause (wrap-exception (.getCause exception))
   :next-exception (wrap-exception (.getNextException exception))
   :stack-trace (wrap-stack-trace exception)})

(defmacro try-catch
  [& body]
  `(try
    ~@body
    (catch Throwable t#
      (throw (ExceptionInfo. (str t#) (wrap-exception t#))))))
