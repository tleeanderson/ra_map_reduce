(ns ra-map-reduce.ra-operator
  (:gen-class))

(defn reduce-identity [key vals]
  "Default reduce function. Takes key and vals and returns them."
  [key vals])

(defn select [cond-func]
  "Semantic equivalent to RA select. Takes function and evaluates it with given
   record. passed and failed are used as group keys."
  {:map (fn [record]
          (if (cond-func record)
            ["passed" record]
            ["failed" (record :offset)]))
   :reduce reduce-identity})

(defn project [attr-keys]
  "Takes a set of attribute keys and selects a subset of the record
   map by those attribute keys. Groups by table by default."
  {:map (fn [record]
          [(record :table)
           (select-keys record attr-keys)])
   :reduce reduce-identity})

(defn agg-group [group-keys agg-func-map]
  {:map (fn [record]
          [(if (some? group-keys)
             (mapv record (sort group-keys))
             (record :table)) record])
   :reduce (fn [key vals]
             [key (map (fn [[k af]]
                         {k (af (map k vals))}) agg-func-map)])})
