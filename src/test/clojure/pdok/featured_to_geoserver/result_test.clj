(ns pdok.featured-to-geoserver.result-test
  (:require [clojure.test :refer :all]
            [pdok.featured-to-geoserver.result :refer :all]))

(deftest test-error-result
  (is (= (->Result nil :problem) (error-result :problem)))
  (is (= (assoc (->Result nil :problem) :line 42 :col 47) (error-result :problem :line 42 :col 47))))

(deftest test-unit-result
  (is (= (->Result "value" nil) (unit-result "value")))
  (is (= (assoc (->Result "value" nil) :comment "default value") (unit-result "value" :comment "default value")))
  (is (thrown? IllegalArgumentException (unit-result "value" :key-without-value)))
  (is (thrown? IllegalArgumentException (unit-result "value" :key "value" :value "forbidden")))
  (is (thrown? IllegalArgumentException (unit-result "value" :key "value" :error "forbidden"))))

(deftest test-error-result?
  (is (error-result? (error-result :problem)))
  (is (not (error-result? (unit-result "value")))))

(deftest test-unit-result?
  (is (unit-result? (unit-result "ok")))
  (is (not (unit-result? (error-result :problem)))))

(deftest test-bind-result
  (is (error-result? (bind-result (fn [_] (error-result :error)) (unit-result 42))))
  (is (= 47 (:line (bind-result (fn [_] (error-result :error :line 47)) (unit-result 42))))))

(deftest test-filter-result
  (is (=
        (error-result :illegal-value)
        (filter-result odd? (unit-result 42))))
  (is (=
        (error-result :illegal-value :line 47)
        (filter-result odd? (unit-result 42 :line 47))))
  (is (=
        (error-result :custom-error)
        (filter-result odd? :custom-error (unit-result 42))))
  (is (unit-result? (filter-result odd? (unit-result 47)))))

(deftest test-map-result
  (is (= (unit-result 42) (map-result inc (unit-result 41))))
  (is (= (unit-result 42 :line 42) (map-result inc (unit-result 41 :line 42))))
  (is (error-result? (map-result inc (error-result :problem)))))

(deftest test-result<-
  (is (=
        (unit-result (+ 42 47))
        (result<- [x (unit-result 42)
                   y (unit-result 47)]
                  (+ x y)))))

(deftest test-merge-result
  (is
    (=
      (unit-result "new-value" :line 42 :col 47)
      (merge-result (unit-result "orig-value" :line 42) (unit-result "new-value" :col 47)))))

(deftest test-unwrap-result
  (is
    (=
      ["value" nil]
      (unwrap-result (unit-result "value" :line 42 :col 47))))
  (is
    (=
      [nil {:error :problem :error-details {:line 42 :col 47}}]
      (unwrap-result
        (filter-result
          (constantly false)
          :problem
          (unit-result "value" :line 42 :col 47))))))
