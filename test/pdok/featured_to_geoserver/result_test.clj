(ns pdok.featured-to-geoserver.result-test
  (:require [clojure.test :refer :all] 
            [pdok.featured-to-geoserver.result :refer :all]))

(deftest error-result-test
  (is (= (->Result nil "problem") (error-result "problem")))
  (is (= (assoc (->Result nil "problem") :line 42 :col 47) (error-result "problem" :line 42 :col 47))))

(deftest unit-result-test
  (is (= (->Result "value" nil) (unit-result "value")))
  (is (= (assoc (->Result "value" nil) :comment "default value") (unit-result "value" :comment "default value")))
  (is (thrown? AssertionError (unit-result "value" :key-without-value)))
  (is (thrown? AssertionError (unit-result "value" :key "value" :value "forbidden")))
  (is (thrown? AssertionError (unit-result "value" :key "value" :error "forbidden"))))

(deftest error-result?-test
  (is (error-result? (error-result "problem")))
  (is (not (error-result? (unit-result "value")))))

(deftest unit-result?-test
  (is (unit-result? (unit-result "ok")))
  (is (not (unit-result? (error-result "problem")))))

(deftest bind-result-test
  (is (error-result? (bind-result (fn [_] (error-result "error")) (unit-result 42))))
  (is (= 47 (:line (bind-result (fn [_] (error-result "error" :line 47)) (unit-result 42))))))

(deftest filter-result-test
  (is (=
        (error-result "illegal value" :error-value 42)
        (filter-result odd? (unit-result 42))))
  (is (=
        (error-result "custom message" :error-value 42)
        (filter-result odd? "custom message" (unit-result 42))))
  (is (unit-result? (filter-result odd? (unit-result 47)))))

(deftest map-result-test
  (is (= (unit-result 42) (map-result inc (unit-result 41))))
  (is (error-result? (map-result inc (error-result "problem")))))

(deftest result<-test
  (is (= 
        (unit-result (+ 42 47))
        (result<- [x (unit-result 42)
                   y (unit-result 47)]
                  (+ x y)))))
