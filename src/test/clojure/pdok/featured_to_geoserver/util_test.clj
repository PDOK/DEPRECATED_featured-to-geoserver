(ns pdok.featured-to-geoserver.util-test
  (:require [clojure.test :refer :all]
            [pdok.featured-to-geoserver.util :refer :all]))

(deftest test-dissoc-in
  (is (= {} (dissoc-in {:a {:b []}} [:a :b])))
  (is (= {:a {:c []}} (dissoc-in {:a {:b [] :c []}} [:a :b]))))
