(ns ra-map-reduce.query
  (:gen-class)
  (:refer ra-map-reduce.ra-operator)
  (:refer ra-map-reduce.model))

(defn select-query [data filt-cond-func]
  "Takes data and a function to perform filtering and returns
   a relation filtered according to the filter condition function."
  (let [{mp :map rd :reduce} (select (fn [r]
                                       (filt-cond-func r)))]
    (map-reduce team-data mp rd)))

(defn project-query [data attr-set]
  "Uses generic project to select name, city, stadium from each record."
  (let [{mp :map rd :reduce} (project attr-set)]
    (map-reduce data mp rd)))

(defn join [sep-key join-cond-func]
  "Takes a key to separate the relations in the reduce stage of cartesian
   product and a function to perform the join condition."
  (let [{cp-mp :map cp-rd :reduce} (cartesian-product nil sep-key)
        {s-mp :map s-rd :reduce} (select (fn [r]
                                             (join-cond-func r)))]
    (map-reduce (grab-records
     (map-reduce [team-data game-data] cp-mp cp-rd)) s-mp s-rd)))

(defn agg-group-query [group-keys key-func-map data]
  "Takes grouping keys, function map by key, and a relation and returns
   aggregation with grouping on relation according to grouping keys and
   function map."
  (let [{mp :map rd :reduce} (agg-group group-keys key-func-map)]
    (map-reduce data mp rd)))

;;ground queries
(defn add-team-offsets []
  "Add the offsets for the team relation."
  (agg-group-query nil {:offset (fn [vs]
                                  (reduce + vs))} team-data))

(defn home-team-points-attendance []
  "Sum home points and attendance for each home team."
  (agg-group-query #{:home_team} {:home_points (fn [vs]
                                                        (reduce + vs))
                                         :attendance (fn [vs]
                                                        (reduce + vs))} game-data))

(defn select-big10-conf []
  "Select teams which are in the big10 conference. GBR."
  (select-query team-data (fn [r]
                            (= (r :conference) "BIG 10"))))

(defn nj-team-game []
  "Performs natural join between team and game."
  (join :table (fn [r]
                 (= (r :name) (r :home_team)))))

