(ns ra-map-reduce.core
  (:gen-class)
  (:require [ra-map-reduce.model :as model])
  (:require [ra-map-reduce.query :as query]))

(defn game-data
  "Read in records from game table."
  []
  (model/data-records model/schema-path "game"))

(defn team-data
  "Read in records from team table."
  []
  (model/data-records model/schema-path "team"))

(defn stadium-data
  "Read in records from stadium table."
  []
  (model/data-records model/schema-path "stadium"))

(defn conference-data
  "Read in records from conference table."
  []
  (model/data-records model/schema-path "conference"))

(defn add-team-offsets
  "Takes team data and adds offsets for all records in table."
  [td]
  (query/agg-group nil {:offset (fn [vs]
                                  (reduce + vs))} td identity))

(defn home-team-points-attendance
  "Sums the home points and attendance for each home team group in
   game table."
  [gd]
  (query/agg-group #{:home_team} {:home_points (fn [vs]
                                                 (reduce + vs))
                                  :attendance (fn [vs]
                                                (reduce + vs))} gd identity))

(defn select-big10-conf
  "Filters the team table by conference and returns only teams in the
   big10. GBR."
  [td]
  (query/project (query/select td (fn [r]
                     (= (r :conference) "BIG 10")) model/passed-records) #{:name :conference :stadium}
                 [:city :state] identity))

(defn nj-team-game
  "Natural join between team and game tables."
  [td gd]
  (query/project (query/join :table (fn [r]
                       (= (r :name) (r :home_team))) td gd model/passed-records)
                 #{:home_team :away_team :home_points :away_points} [:date :stadium] identity))

(defn home-team-wins
  "Computes statistics for every game where the home team won."
  [td gd]
  (let [nj (query/project (query/join :table (fn [r]
                       (= (r :name) (r :home_team))) td gd model/passed-records)
                 #{:home_team :away_team :home_points :away_points :stadium} [:date :city] identity)]
    (query/project
     (query/select (model/grab-records nj) (fn [r]
                       (> (r :home_points) (r :away_points))) model/passed-records)
     #{:home_points :away_points :home_team :away_team :stadium} [:home_team :away_team] identity)))

(defn stadium-stats
  "Computes stats for every game per stadium group."
  [td gd]
  (let [nj (query/project (query/join :table (fn [r]
                       (= (r :name) (r :home_team))) td gd model/passed-records) #{:home_points
                                                                                   :away_points
                                                                                   :stadium
                                                                                   :attendance}
                          [nil] model/grab-records)]
    (query/agg-group #{:stadium} {:home_points (fn [vs]
                                                 (apply max vs))
                                  :away_points (fn [vs]
                                                 (reduce min vs))
                                  :attendance (fn [vs]
                                                 (float (/ (reduce + vs) (count vs))))} nj identity)))

(defn example-out
  "Creates example output for each query."
  [ex-num descr query]
  (str "Example " ex-num ": " descr "\n\nEquivalent SQL:\n" query "\n\nMapReduce: "))

(defn print-coll
  "Print out collection."
  [coll]
  (doseq [item coll]
    (println "\t" item)))

(defn separate
  "Returns a separator string."
  []
  (println (str "\n" (apply str (repeat 100 "-")) "\n")))

(def stadium-stats-fq [stadium-stats
                       "\tSELECT AVG(ATTENDANCE), MAX(HOME_POINTS), MIN(AWAY_POINTS)\n\tFROM TEAM\n\tJOIN GAME ON TEAM.NAME = GAME.HOME_TEAM\n\tGROUP BY STADIUM"])

(def home-team-wins-fq [home-team-wins
                        "\tSELECT HOME_POINTS,AWAY_POINTS,HOME_TEAM,AWAY_TEAM,STADIUM\n\tFROM TEAM\n\tJOIN GAME ON TEAM.NAME = GAME.HOME_TEAM\n\tWHERE HOME_POINTS > AWAY_POINTS"])

(def add-team-offsets-fq [add-team-offsets
                          "\tSELECT SUM(OFFSET)\n\tFROM TEAM\n\tGROUP BY OFFSET"])

(def home-team-points-att-fq [home-team-points-attendance
                              "\tSELECT SUM(HOME_POINTS), SUM(ATTENDANCE)\n\tFROM GAME\n\tGROUP BY HOME_TEAM"])

(def select-big10-conf-fq [select-big10-conf
                           "\tSELECT NAME,CONFERENCE,STADIUM\n\tFROM TEAM\n\tWHERE CONFERENCE = 'BIG 10'"])

(def nj-team-game-fq [nj-team-game
                      "\tSELECT HOME_TEAM,AWAY_TEAM,HOME_POINTS,AWAY_POINTS\n\tFROM TEAM\n\tJOIN GAME ON TEAM.NAME = GAME.HOME_TEAM"])

(defn -main
  "Run queries and output results to stdout."
  [& args]
  (let [game (game-data)
        team (team-data)
        stadium (stadium-data)
        conference (conference-data)
        [off off-query] [((first add-team-offsets-fq) team) (second add-team-offsets-fq)]
        [htp htp-query] [((first home-team-points-att-fq) game) (second home-team-points-att-fq)]
        [b10 b10-query] [((first select-big10-conf-fq) team) (second select-big10-conf-fq)]
        [nj-tg nj-tg-query] [((first nj-team-game-fq) team game) (second nj-team-game-fq)]
        [htw htw-query] [((first home-team-wins-fq) team game) (second home-team-wins-fq)]
        [ss ss-query] [((first stadium-stats-fq) team game) (second stadium-stats-fq)]]
    (separate)
    (println (example-out "0" "This will contain an english description of the query." "\tQUERY IN SQL"))
    (println "\tMap reduce output will appear here.\n\t[[map reduce join attributes] [records]]")
    (separate)
    (println (example-out "1" "Use grouping on team on the map side and add the offset column for each team in team."
                         off-query))
    (print-coll off)
    (separate)
    (println (example-out "2" "For each group of home teams, sum the home team points and attendance for every game."
                          htp-query))
    (print-coll htp)
    (separate)
    (println (example-out "3" "Select the name, conference, and stadium for all teams in the BIG 10 conference.\n\tUse map grouping on city and state."
                          b10-query))
    (print-coll b10)
    (separate)
    (println (example-out "4" "Compute the natural join between team and game tables. Use map grouping on date and stadium." nj-tg-query))
    (print-coll nj-tg)
    (separate)
    (println (example-out "5" "Return the home points, away points, home team, away team, and stadium for every game where the home team won.\n\tUse map grouping on home team and away team." htw-query))
    (print-coll htw)
    (separate)
    (println (example-out "6" "For every game, compute the average attendance, maximum home points, and minimum away points for each stadium.\n\tUse map grouping on stadium." ss-query))
    (print-coll ss)
    (separate)))
