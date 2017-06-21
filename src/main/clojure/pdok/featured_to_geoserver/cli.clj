(ns pdok.featured-to-geoserver.cli
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.tools.cli :as cli]
            [pdok.featured-to-geoserver.util :refer :all]
            [pdok.featured-to-geoserver.core :as core])
  (:gen-class))

(def cli-options
  [["-f" "--format  FORMAT" "File format (zip or plain)"
    :default "zip"
    :validate [#(some (partial = %) ["zip" "plain"]) "File format must be zip or plain"]]
   [nil "--db-host HOST" "Database host"
    :default "localhost"]
   [nil "--db-port PORT" "Database port"
    :default 5432
    :parse-fn #(Integer/parseInt %)
    :validate [#(> % 0) "Port number should be positive"]]
   [nil "--db-user USER" "Database user"
    :default "postgres"]
   [nil "--db-password PASSWORD" "Database password"
    :default "postgres"]
   [nil "--db-name DATABASE" "Database name"
    :default "pdok"]
   [nil "--array-mapping COLLECTION/FIELD" "Specify array mapping for field"
    :parse-fn #(str/split % #"/")
    :validate [#(= 2 (count %)) "Must be COLLECTION/FIELD"]
    :assoc-fn (fn [m k [collection field]] (update-in m [k (keyword collection) :array] #(conj (or % []) field)))
    :default {}]
   [nil "--exclude-filter FIELD=EXCLUDE-VALUE" "Exclude changelog entries"
    :parse-fn #(str/split % #"=")
    :validate [#(= 2 (count %)) "Must be FIELD=EXCLUDE-VALUE"]
    :assoc-fn (fn [m k [field exclude-value]] (update-in m [k (keyword field)] #(conj (or % []) exclude-value)))
    :default {}]])

(defn log-usage [summary]
  (log/info "")
  (log/info "Usage: lein run [options] dataset files")
  (log/info "")
  (log/info "Options")
  (doseq [option (str/split summary #"\n")]
    (log/info option))
  (log/info ""))

(defn -main [& args]
  (log/info "This is the featured-to-geoserver CLI version" (implementation-version))
  (let [{[dataset & files] :arguments summary :summary options :options errors :errors} (cli/parse-opts args cli-options)
        {format :format
         host :db-host
         port :db-port
         user :db-user
         password :db-password
         database :db-name
         array-mapping :array-mapping
         exclude-filter :exclude-filter} options]
    (cond
      errors (do (log-usage summary) (doseq [error errors] (log/error error)))
      (empty? files) (log-usage summary)
      :else (let [db {:url (str "//" host ":" port "/" database)
                      :user user
                      :password password}
                  process-channel (async/chan 1)
                  terminate-channel (async/chan 1)]
              (core/create-workers 1 db process-channel terminate-channel)
              (doseq [file files]
                (log/info "Processing file:" file)
                (async/>!! process-channel {:file file 
                                            :dataset dataset 
                                            :format format
                                            :mapping array-mapping
                                            :exclude-filter exclude-filter}))
              (async/close! process-channel)
              (log/info (str "Worker " (async/<!! terminate-channel) " terminated"))
              (log/info "Application terminated")))))
