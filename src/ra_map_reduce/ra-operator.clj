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
  "Takes a set of group keys and aggregation function map and
   performs aggregation and grouping. If no group keys are specified,
   then grouping is done on table. Aggregation function map must be keyed
   by attribute name and values should be functions that expect a list of values."
  {:map (fn [record]
          [(if (some? group-keys)
             (mapv record (sort group-keys))
             (record :table)) record])
   :reduce (fn [key vals]
             [key (map (fn [[k af]]
                         {k (af (map k vals))}) agg-func-map)])})

(defn cartesian-product [group-keys]
  "Takes a set of group-keys and creates a vector group key with
   values from the record. Uses default_key if no key is specified."
  {:map (fn [record]
          [(if (some? group-keys)
             (mapv record (sort group-keys))
             "default_key") record])
   :reduce reduce-identity})
