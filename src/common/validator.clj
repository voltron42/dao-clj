(ns common.validator
  (:require [clojure.spec.alpha :as s])
  (:import (clojure.lang ExceptionInfo)))

(defn validator [spec-name error-message]
  (fn [spec]
    (when-not (s/valid? spec-name spec)
      (throw
        (ExceptionInfo.
          error-message
          (s/explain-data spec-name spec))))))
