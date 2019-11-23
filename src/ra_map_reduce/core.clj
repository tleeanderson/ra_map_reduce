(ns ra-map-reduce.core
  (:gen-class)
  (:require [ra-map-reduce.model :as model])
  (:require [ra-map-reduce.query :as query]))

(defn game-data []
  (model/data-records model/schema-path "game"))
(defn team-data []
  (model/data-records model/schema-path "team"))
(defn stadium-data []
  (model/data-records model/schema-path "stadium"))
(defn conference-data []
  (model/data-records model/schema-path "conference"))

(defn add-team-offsets [td]
  (query/agg-group nil {:offset (fn [vs]
                                  (reduce + vs))} td))

(defn home-team-points-attendance [gd]
  (query/agg-group #{:home_team} {:home_points (fn [vs]
                                                 (reduce + vs))
                                  :attendance (fn [vs]
                                                (reduce + vs))} gd))

(defn select-big10-conf [td]
  (query/select td (fn [r]
                     (= (r :conference) "BIG 10"))))

(defn nj-team-game [td gd]
  (query/join :table (fn [r]
                       (= (r :name) (r :home_team))) td gd))

(defn -main
  "Run queries and output results to file."
  [& args]
  (let [game (game-data)
        team (team-data)
        stadium (stadium-data)
        conference (conference-data)
        off (add-team-offsets team)] 
    (println "Use default grouping and add the offset column for team:" off)))
