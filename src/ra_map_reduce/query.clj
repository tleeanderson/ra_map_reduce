(ns ra-map-reduce.query
  (:gen-class)
  (:refer ra-map-reduce.ra-operator)
  (:refer ra-map-reduce.model))

(defn add-offsets-query []
  "Uses generic agg-group. Default grouping on table name and sum the offsets."
  (let [{mp :map rd :reduce} (agg-group nil {:offset (fn [vs]
                                                       (reduce + vs))})]
    (map-reduce team-data mp rd)))

(defn home-team-stats []
  "Uses generic agg-group. Sum home points and attendance for each home team."
  (let [{mp :map rd :reduce} (agg-group #{:home_team}
                                        {:home_points (fn [vs]
                                                        (reduce + vs))
                                         :attendance (fn [vs]
                                                        (reduce + vs))})]
    (map-reduce game-data mp rd)))

(defn select-conference-query []
  "Uses generic select function to filter teams by conference."
  (let [{mp :map rd :reduce} (select (fn [r]
                                       (= (r :conference) "BIG 10")))]
    (map-reduce team-data mp rd)))

(defn project-query []
  "Uses generic project to select name, city, stadium from each record."
  (let [{mp :map rd :reduce} (project #{:name :city :stadium})]
    (map-reduce stadium-data mp rd)))

(defn join-query []
  (let [{mp :map rd :reduce} (cartesian-product #{:name})]
    (map-reduce [team-data team-data] mp rd)))

(defn several-query []
  (let [{cp-mp :map cp-rd :reduce} (cartesian-product #{:name})
        {s-mp :map s-rd :reduce} (select (fn [r]
                                       (contains? #{"ACC" "Nebraska"} (r :name))))]
    (map-reduce (grab-records
                 (map-reduce [team-data conference-data] cp-mp cp-rd)) s-mp s-rd)))
