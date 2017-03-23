(ns pdok.featured-to-geoserver.util)

(defn dissoc-in [m [k & ks]]
  (if ks
    (let [v (dissoc-in (k m) ks)]
      (if (empty? v)
        (dissoc m k)
        (assoc m k v)))
    (dissoc m k)))

(defn uuid [s]
  (java.util.UUID/fromString s))
