(ns pdok.featured-to-geoserver.config
  (:require [environ.core :refer :all]))

(defn- as-int [value]
  (when value
    (Integer/parseInt value)))

(defn n-workers []
  (or (as-int (env :n-workers)) 5))

(defn queue-length []
  (or (as-int (env :queue-length)) 20))

(defn- not-nil-env [key]
  (let [value (env key)]
    (when (not value)
      (throw (IllegalArgumentException. (str "Environment variable missing: " key))))
    value))

(defn db []
  {:url (not-nil-env :database-url)
   :user (not-nil-env :database-user)
   :password (not-nil-env :database-password)})