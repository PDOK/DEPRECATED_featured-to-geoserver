(ns pdok.featured-to-geoserver.changelog-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [pdok.featured-to-geoserver.result :refer :all]
            [pdok.featured-to-geoserver.changelog :as changelog]))

(deftest test-index-of-seq
  (is (empty? (changelog/index-of-seq "" ",")) "Should be empty for an empty input string")
  (is (empty? (changelog/index-of-seq "no-separator" ",")) "Should be empty when separator is not present in string")
  (is (= (list 0) (changelog/index-of-seq "," ",")) "A string with just the separator should result in (list 0)")
  (is (= (list 0 6 7) (changelog/index-of-seq ",first,,second" ",")) "Empty colums should be handled correctly")
  (is (= (list 5) (changelog/index-of-seq "first,second" ",")) "Should contain a single index for a single occurrence of the separator")
  (is (= (list 5 12 18) (changelog/index-of-seq "first,second,third,fourth" ",")) "Should contain multiple indexes for multiple occurrences of the separator")
  (is (= (list 5 13) (changelog/index-of-seq "first,,second,,third" ",,")) "Multicharacter separator should also work as expected"))

(deftest test-str-split
  (is
    (=
      (list
        (unit-result "a" :col 1)
        (unit-result "b" :col 3)
        (unit-result "c" :col 5)
      (changelog/str-split "a,b,c", ","))))
  (is
    (=
      (list
        (unit-result "a" :col 1)
        (unit-result "b,c" :col 3)
      (changelog/str-split "a,b,c", "," 2)))))

(deftest test-read-changelog
  (is 
    (=
      (error-result :unsupported-version :line 1) 
      (changelog/read-changelog (list "v2")))
    "Should not accept an unsupported changelog version")
  (is
    (=
      (error-result :field-empty :field :schema-name :line 2 :col 0)
      (changelog/read-changelog (list "v1" "")))
    "Should not accept missing schema-name and object-type")
  (is 
    (=
      (error-result :field-missing :field :object-type :line 2)
      (changelog/read-changelog (list "v1" "schema-name")))
  "Should not accept missing feature-type")
  (is 
    (=
      (error-result :field-empty :field :object-type :line 2 :col 12)
      (changelog/read-changelog (list "v1" "schema-name,")))
    "Should not accept empty feature-type")
  (is 
    (=
      (error-result :field-empty :field :schema-name :line 2 :col 0)
      (changelog/read-changelog (list "v1" ",object-type"))) 
    "Should not accept empty dataset")
  (is 
    (= 
      (error-result :field-empty :field :schema-name :line 2 :col 0) 
      (changelog/read-changelog (list "v1" ",")))
    "Should not accept empty dataset and feature-type")
  (is
    (=
      (unit-result {:schema-name :schema-name :object-type :object-type :actions (list)})
      (changelog/read-changelog (list "v1" "schema-name,object-type")))
    "Should result in an empty changelog")
  (is
    (=
      (unit-result {:schema-name :schema-name
                    :object-type :object-type
                    :actions (list
                               (unit-result
                                 {:action :new
                                  :object-id "b5ab7b8a-7474-49b7-87ea-44bd2fea13e8"
                                  :version-id "115ba9a3-275f-4022-944a-dcacdc71ff6a"
                                  :object-data {:i 42}}
                                 :line 3)
                               (unit-result
                                 {:action :change
                                  :object-id "b5ab7b8a-7474-49b7-87ea-44bd2fea13e8"
                                  :prev-version-id "115ba9a3-275f-4022-944a-dcacdc71ff6a"
                                  :version-id "a24f32bf-412d-4733-99aa-1ca5f6086ac3"
                                  :object-data {:i 47}}
                                 :line 4)
                               (unit-result
                                 {:action :new
                                  :object-id "7a9e7edc-a49b-438d-a11d-23c8072d5dd4"
                                  :version-id "d7bf02ce-010c-46c8-bd63-d81ad415efe1"
                                  :object-data {:i 47}}
                                 :line 5)
                               (unit-result
                                 {:action :close
                                  :object-id "7a9e7edc-a49b-438d-a11d-23c8072d5dd4"
                                  :prev-version-id "d7bf02ce-010c-46c8-bd63-d81ad415efe1"
                                  :version-id "4167bce6-12a0-4d9c-bd22-afd68f113683"
                                  :object-data {:i 47}}
                                 :line 6)
                               (unit-result
                                 {:action :delete
                                  :object-id "b5ab7b8a-7474-49b7-87ea-44bd2fea13e8"
                                  :version-id "a24f32bf-412d-4733-99aa-1ca5f6086ac3"}
                                 :line 7))})
      (changelog/read-changelog
        (list
          "v1"
          "schema-name,object-type"
          "new,b5ab7b8a-7474-49b7-87ea-44bd2fea13e8,115ba9a3-275f-4022-944a-dcacdc71ff6a,[\"^ \",\"~:i\",42]"
          "change,b5ab7b8a-7474-49b7-87ea-44bd2fea13e8,115ba9a3-275f-4022-944a-dcacdc71ff6a,a24f32bf-412d-4733-99aa-1ca5f6086ac3,[\"^ \",\"~:i\",47]"
          "new,7a9e7edc-a49b-438d-a11d-23c8072d5dd4,d7bf02ce-010c-46c8-bd63-d81ad415efe1,[\"^ \",\"~:i\",47]"
          "close,7a9e7edc-a49b-438d-a11d-23c8072d5dd4,d7bf02ce-010c-46c8-bd63-d81ad415efe1,4167bce6-12a0-4d9c-bd22-afd68f113683,[\"^ \",\"~:i\",47]"
          "delete,b5ab7b8a-7474-49b7-87ea-44bd2fea13e8,a24f32bf-412d-4733-99aa-1ca5f6086ac3")))
    "Should result in changelog with some actions"))
