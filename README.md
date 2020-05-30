# Implementation of RA operators via Map and Reduce Functions

Shows off the implementation of the RA operators via map and reduce functions which are
submitted to a simulated MapReduce system. Please read **paper.pdf** for an in depth analysis
of the system herein.

## Build
You must have Leiningen installed to build this application. Please visit this link for installation
instructions: https://leiningen.org/

From the project root, execute:<br/>
     `lein uberjar`

## Usage

To execute the prebuilt jar, execute:<br/>
   `java -jar ra_map_reduce-0.1.0-SNAPSHOT-standalone.jar`

If you built the artifact jar, execute:<br/>
    `java -jar target/uberjar/ra_map_reduce-0.1.0-SNAPSHOT-standalone.jar`

## Options

The jar accepts no options.
