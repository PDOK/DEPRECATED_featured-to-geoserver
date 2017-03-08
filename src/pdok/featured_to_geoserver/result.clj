(ns pdok.featured-to-geoserver.result)

(defrecord Result[value error])

(defn- result [x params]
  (assert
    (even? (count params)) 
    "expect an even amount of key-value pairs")
  (let [keywords (keep-indexed #(if (even? %1) %2) params)] 
    (assert 
      (empty? (remove keyword? keywords))
      "expect only keywords as key in key-value pairs")
    (assert
      (empty? (filter #(or (= % :value) (= % :error)) keywords))
      "non-allowed keyword(s) in params; :value and/or :error encountered"))
  (if params
    (apply (partial assoc x) params)
    x))

(defn unit-result [x & params]
  (assert x "expect a value, not nil")
  (result (->Result x nil) params))

(defn error-result [msg & params]
  (result (->Result nil msg) params))

(defn unit-result? [x]
  (let [{value :value} x]
    (some? value)))

(defn error-result? [x]
  (let [{error :error} x]
    (some? error)))

(defn bind-result [f r]
  (let [{value :value error :error} r]
    (if (some? value)
      (f value)
      r)))

(defn merge-result [a b]
  (merge b (-> a
             (into {})
             (dissoc :error :value))))

(defn filter-result
  ([f r] (filter-result f :illegal-value r))
  ([f error r]
    (bind-result
      #(if (f %)
         r
         (merge-result
           r
           (error-result error)))
      r)))

(defn map-result [f r]
  (bind-result #(merge-result r (unit-result (f %))) r))

(defmacro ^{:private true} assert-arg [x msg]
  `(when-not ~x (throw (IllegalArgumentException. (str (first ~'&form) " requires " ~msg " in " *ns* ":" (:line (meta ~'&form)))))))

(defmacro result<- [bindings & body]
  (assert-arg (vector? bindings) "a vector for its bindings")
  (assert-arg (even? (count bindings)) "even forms in binding vector")
  `(let [generator# ~(second bindings)]
     (bind-result
       (fn [~(first bindings)]
         ~(if (= 2 (count bindings))
            `(unit-result ~@body)
            `(result<- [~@(drop 2 bindings)] ~@body)))
       generator#)))