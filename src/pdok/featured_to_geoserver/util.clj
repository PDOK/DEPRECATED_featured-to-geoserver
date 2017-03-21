(ns pdok.featured-to-geoserver.util)

(defn uuid [s]
  (java.util.UUID/fromString s))