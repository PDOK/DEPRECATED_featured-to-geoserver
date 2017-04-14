(def version (slurp "VERSION"))
(def artifact-name (str "featured-to-geoserver-" version))
(def uberjar-name (str artifact-name "-standalone.jar"))
(def webjar-name (str artifact-name "-web.jar"))
(def uberwar-name (str artifact-name ".war"))
(def git-ref (clojure.string/replace (:out (clojure.java.shell/sh "git" "rev-parse" "HEAD"))#"\n" "" ))

(defproject featured-to-geoserver version
  :min-lein-version "2.5.4"
  :uberjar-name ~uberjar-name
  :manifest {"Implementation-Version" ~(str version "(" git-ref ")")}
  :description "Open-source GeoServer loading software for Featured"
  :url "https://github.com/PDOK/featured-to-geoserver"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.3.426"]
                 [nl.pdok/featured-shared "1.1.2"]
                 [cheshire/cheshire "5.7.0"]
                 [ring/ring-defaults "0.2.3"]
                 [ring/ring-json "0.4.0"]
                 [environ/environ "1.1.0"]
                 [prismatic/schema "1.1.4"]
                 [http-kit/http-kit "2.2.0"]
                 [org.postgresql/postgresql "9.4.1212.jre7"]
                 [compojure/compojure "1.5.2"]
                 [clj-time/clj-time "0.13.0"]
                 [org.clojure/tools.cli "0.3.5"]]
  :plugins [[lein-ring/lein-ring "0.11.0"]
            [lein-cloverage "1.0.9"]]
  :ring {:port 7000
         :init pdok.featured-to-geoserver.api/init!
         :destroy pdok.featured-to-geoserver.api/destroy!
         :handler pdok.featured-to-geoserver.api/app}
  :main pdok.featured-to-geoserver.cli
  :source-paths ["src/main/clojure"]
  :resource-paths ["src/main/resources"]
  :test-paths ["src/test/clojure"]
  :profiles {:uberjar {:aot :all}
             :cli {:uberjar-name ~uberjar-name
                   :aliases {"build" ["do" "uberjar"]}}
             :web-jar {:uberjar-name ~webjar-name
                       :aliases {"build" ["do" ["ring" "uberjar"]]}}
             :test {:resource-paths ["test/resources"]}
             :dev {:resource-paths ["test/resources"]}})
