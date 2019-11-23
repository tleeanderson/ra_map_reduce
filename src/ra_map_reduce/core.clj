(ns ra-map-reduce.core
  (:gen-class)
  (:require [ra-map-reduce.model :as model])
  (:require [ra-map-reduce.query :as query]))

(defn game-data []
  "Read in records from game table."
  (model/data-records model/schema-path "game"))
(defn team-data []
  "Read in records from team table."
  (model/data-records model/schema-path "team"))
(defn stadium-data []
  "Read in records from stadium table."
  (model/data-records model/schema-path "stadium"))
(defn conference-data []
  "Read in records from conference table."
  (model/data-records model/schema-path "conference"))

(defn add-team-offsets [td]
  "Takes team data and adds offsets for all records in table."
  (query/agg-group nil {:offset (fn [vs]
                                  (reduce + vs))} td model/raw-mr))

(def add-team-offsets-fq [add-team-offsets
                          "\tSELECT SUM(OFFSET)\n\tFROM TEAM\n\tGROUP BY OFFSET"])

(defn home-team-points-attendance [gd]
  "Sums the home points and attendance for each home team group in
   game table."
  (query/agg-group #{:home_team} {:home_points (fn [vs]
                                                 (reduce + vs))
                                  :attendance (fn [vs]
                                                (reduce + vs))} gd model/raw-mr))
(def home-team-points-att-fq [home-team-points-attendance
                              "\tSELECT SUM(HOME_POINTS), SUM(ATTENDANCE)\n\tFROM GAME\n\tGROUP BY HOME_TEAM"])

(defn select-big10-conf [td]
  "Filters the team table by conference and returns only teams in the
   big10. GBR."
  (query/project (query/select td (fn [r]
                     (= (r :conference) "BIG 10")) model/passed-records) #{:name :conference :stadium}
                 #{:city :state} model/raw-mr))

(def select-big10-conf-fq [select-big10-conf
                           "\tSELECT NAME,CONFERENCE,STADIUM\n\tFROM TEAM\n\tWHERE CONFERENCE = 'BIG 10'"])

(defn nj-team-game [td gd]
  "Natural join between team and game tables."
  (query/project (query/join :table (fn [r]
                       (= (r :name) (r :home_team))) td gd model/passed-records)
                 #{:home_team :away_team :home_points :away_points} #{:date :stadium} model/raw-mr))

(def nj-team-game-fq [nj-team-game
                      "\tSELECT HOME_TEAM,AWAY_TEAM,HOME_POINTS,AWAY_POINTS\n\tFROM TEAM\n\tJOIN GAME ON TEAM.NAME = GAME.HOME_TEAM"])

(defn example-out [ex-num descr query]
  (str "Example " ex-num ": " descr "\n\nEquivalent SQL:\n" query "\n\nMapReduce: "))

(defn print-coll [coll]
  (doseq [item coll]
    (println "\t" item)))

(defn separate []
  (println (str "\n" (apply str (repeat 100 "-")) "\n")))

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
        [nj-tg nj-tg-query] [((first nj-team-game-fq) team game) (second nj-team-game-fq)]]
    (println (example-out "1" "Use default grouping and add the offset column for each team in team."
                         off-query))
    (print-coll off)
    (separate)
    (println (example-out "2" "Sum the home team points and attendance for each home game."
                          htp-query))
    (print-coll htp)
    (separate)
    (println (example-out "3" "Filters the team table by conference and returns only teams in the BIG 10."
                          b10-query))
    (print-coll b10)
    (separate)
    (println (example-out "4" "Natural join between team and game tables." nj-tg-query))
    (print-coll nj-tg)
    (separate)))
