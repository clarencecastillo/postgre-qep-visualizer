# Postgre QEP Visualizer

## About

The QEP Visualiser Tool is a web application built using Flask and Angular. This project aims to explore the development of a new QEP visualiser that is able to compensate for the shortfalls of current QEP visualisation tools offered. This will aid us in understanding how to go about fine tuning the database to improve performance.

## Usage

For ease of use, the application is deployed at Heroku and can be accessed via [here](https://postgre-qep-visualizer.herokuapp.com/). Note that it is deployed on a free plan and will require a few seconds to *warm up*.

To visualize a query execution plan:

1. Paste the output of `EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) <query>` to the first text field. This field will indicate if the given JSON string contains any syntax errors.

2. Paste the corresponding SQL query to the second text field. This field does not perform any syntax checking and therefore assumes the given query is correct.

A sample query is provided and can be loaded by clicking on the *Load Sample* button at the bottom. Note that the application uses the browser's _localstorage_ to store the most recent QEP Plan and SQL Query.

If deployed locally, the application can be accessed via your browser at http://localhost:5000. It is recommended to use the app using Google Chrome at a higher resolution screen (or zoom out a little if it's to stuffy).

## Dependencies

This application was built with Python v3.7.0 and uses the dependencies specified in `requirements.txt`. Use the command `pip install -r requirements.txt` to install the latest version of the listed packages.

The web application was built with Node.js v8.11.3 and uses the dependencies specified in `package.json`. Use the command `npm install` to install the locked version of the listed packages.

## Deployment

To run the application, execute the file `server.py`. Ensure that the build of the web application exists (check that `static` folder is present) and that port 5000 is unused.

## Building

To build the web application, run `npm build`.
