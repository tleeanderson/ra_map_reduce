(ns ra-map-reduce.query
  (:gen-class)
  (:refer ra-map-reduce.ra-operator))

(defn add-offsets-query [data]
  "Uses generic agg-group. Default grouping on table name and sum the offsets."
  (let [{mp :map rd :reduce} (agg-group nil {:offset (fn [vs]
                                                       (reduce + vs))})]
    (map-reduce data mp rd)))

(defn home-team-stats [data]
  "Uses generic agg-group. Sum home points and attendance for each home team."
  (let [{mp :map rd :reduce} (agg-group #{:home-team}
                                        {:home_points (fn [vs]
                                                        (reduce + vs))
                                         :attendance (fn [vs]
                                                        (reduce + vs))})]
    (map-reduce data mp rd)))

(defn select-conference-query [data]
  "Uses generic select function to filter teams by conference."
  (let [{mp :map rd :reduce} (select (fn [r]
                                       (= (r :conference) "BIG 10")))]
    (map-reduce data mp rd)))

(defn project-query [data]
  "Uses generic project to select name, city, stadium from each record."
  (let [{mp :map rd :reduce} (project #{:name :city :stadium})]
    (map-reduce data mp rd)))
