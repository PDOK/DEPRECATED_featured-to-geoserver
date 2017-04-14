(ns pdok.featured-to-geoserver.changelog-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [pdok.transit :as transit]
            [pdok.featured-to-geoserver.util :refer :all]
            [pdok.featured-to-geoserver.result :refer :all]
            [pdok.featured-to-geoserver.changelog :as changelog]))

(deftest test-read-changelog
  (is
    (=
      (error-result :unsupported-version :line 1)
      (changelog/read-changelog (list "pdok-featured-changelog-v3")))
    "Should not accept an unsupported changelog version")
  (is
    (=
      (unit-result {:meta-info {:header-field "value"} :actions (list)})
      (changelog/read-changelog (list "pdok-featured-changelog-v2" (transit/to-json {:header-field "value"}))))
    "Should result in an empty changelog")
  (is
    (=
      (unit-result {:meta-info {:header-field "value"}
                    :actions (list
                               (error-result 
                                 :unknown-action 
                                 :action :illegal 
                                 :line 3))})
      (changelog/read-changelog
        (cons
          "pdok-featured-changelog-v2"
          (map
            transit/to-json
            (list
              {:header-field "value"}
              {:action "illegal"}))))))
  (is
    (=
      (unit-result {:meta-info {:header-field "value"}
                    :actions (list
                               (error-result 
                                 :fields-missing 
                                 :action :delete 
                                 :fields [:collection :previous-version] 
                                 :line 3))})
      (changelog/read-changelog
        (cons
          "pdok-featured-changelog-v2"
          (map
            transit/to-json
            (list
              {:header-field "value"}
              {:action "delete"}))))))
  (is
    (=
      (unit-result {:meta-info {:header-field "value"}
                    :actions (list
                               (unit-result
                                 {:action :new
                                  :collection :collection
                                  :id "b5ab7b8a-7474-49b7-87ea-44bd2fea13e8"
                                  :version (uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a")
                                  :attributes {:i 42}}
                                 :line 3)
                               (unit-result
                                 {:action :change
                                  :collection :collection
                                  :id "b5ab7b8a-7474-49b7-87ea-44bd2fea13e8"
                                  :previous-version (uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a")
                                  :version (uuid "a24f32bf-412d-4733-99aa-1ca5f6086ac3")
                                  :attributes {:i 47}}
                                 :line 4)
                               (unit-result
                                 {:action :new
                                  :collection :collection
                                  :id "7a9e7edc-a49b-438d-a11d-23c8072d5dd4"
                                  :version (uuid "d7bf02ce-010c-46c8-bd63-d81ad415efe1")
                                  :attributes {:i 47}}
                                 :line 5)
                               (unit-result
                                 {:action :close
                                  :collection :collection
                                  :id "7a9e7edc-a49b-438d-a11d-23c8072d5dd4"
                                  :previous-version (uuid "d7bf02ce-010c-46c8-bd63-d81ad415efe1")
                                  :version (uuid "4167bce6-12a0-4d9c-bd22-afd68f113683")
                                  :attributes {:i 47}}
                                 :line 6)
                               (unit-result
                                 {:action :delete
                                  :collection :collection
                                  :id "b5ab7b8a-7474-49b7-87ea-44bd2fea13e8"
                                  :previous-version (uuid "a24f32bf-412d-4733-99aa-1ca5f6086ac3")}
                                 :line 7))})
      (changelog/read-changelog
        (cons
          "pdok-featured-changelog-v2"
          (map
            transit/to-json
            (list
              {:header-field "value"}
              {:action "new"
               :collection "collection"
               :id "b5ab7b8a-7474-49b7-87ea-44bd2fea13e8"
               :version (uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a")
               :attributes {:i 42}}
              {:action "change"
               :collection "collection"
               :id "b5ab7b8a-7474-49b7-87ea-44bd2fea13e8"
               :previous-version (uuid "115ba9a3-275f-4022-944a-dcacdc71ff6a")
               :version (uuid "a24f32bf-412d-4733-99aa-1ca5f6086ac3")
               :attributes {:i 47}}
              {:action "new"
               :collection "collection"
               :id "7a9e7edc-a49b-438d-a11d-23c8072d5dd4"
               :version (uuid "d7bf02ce-010c-46c8-bd63-d81ad415efe1")
               :attributes {:i 47}}
              {:action "close"
               :collection "collection"
               :id "7a9e7edc-a49b-438d-a11d-23c8072d5dd4"
               :previous-version (uuid "d7bf02ce-010c-46c8-bd63-d81ad415efe1")
               :version (uuid "4167bce6-12a0-4d9c-bd22-afd68f113683")
               :attributes {:i 47}}
              {:action "delete"
               :collection "collection"
               :id "b5ab7b8a-7474-49b7-87ea-44bd2fea13e8"
               :previous-version (uuid "a24f32bf-412d-4733-99aa-1ca5f6086ac3")})))))
    "Should result in changelog with some actions"))

(run-tests)