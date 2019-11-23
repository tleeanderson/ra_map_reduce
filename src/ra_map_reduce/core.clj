(ns ra-map-reduce.core
  (:gen-class)
  (:require [ra-map-reduce.model :as model])
  (:require [ra-map-reduce.query :as query]))

(def game-data (model/data-records model/schema-path "game"))
(def team-data (model/data-records model/schema-path "team"))
(def stadium-data (model/data-records model/schema-path "stadium"))
(def conference-data (model/data-records model/schema-path "conference"))

;;ground queries
(defn add-team-offsets []
  "Add the offsets for the team relation."
  (query/agg-group nil {:offset (fn [vs]
                                  (reduce + vs))} team-data))

(defn home-team-points-attendance []
  "Sum home points and attendance for each home team."
  (query/agg-group #{:home_team} {:home_points (fn [vs]
                                                        (reduce + vs))
                                         :attendance (fn [vs]
                                                        (reduce + vs))} game-data))

(defn select-big10-conf []
  "Select teams which are in the big10 conference. GBR."
  (query/select team-data (fn [r]
                            (= (r :conference) "BIG 10"))))

(defn nj-team-game []
  "Performs natural join between team and game."
  (query/join :table (fn [r]
                 (= (r :name) (r :home_team))) team-data game-data))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
