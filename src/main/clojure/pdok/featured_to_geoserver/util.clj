(ns pdok.featured-to-geoserver.util)

(defn dissoc-in [m [k & ks]]
  (if ks
    (let [v (dissoc-in (k m) ks)]
      (if (empty? v)
        (dissoc m k)
        (assoc m k v)))
    (dissoc m k)))

(defn exception-to-string [e]
  (if (instance? Iterable e)
    (clojure.string/join " Next: " (map str (seq e)))
    (str e)))

(defn uuid [s]
  (java.util.UUID/fromString s))

(gen-class :name pdok.featured_to_geoserver.version)

(defn implementation-version
  []
  (or
    (try
      (-> (Class/forName "pdok.featured_to_geoserver.version")
        (.getPackage)
        (.getImplementationVersion))
      (catch Throwable t nil))
    "unknown"))
