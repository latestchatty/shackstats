#!/bin/bash
set -e
set -x
tsc
cp -f node_modules/jquery/dist/jquery.min.js web/ext/
cp -f node_modules/papaparse/papaparse.min.js web/ext/
cp -f node_modules/chart.js/dist/Chart.min.js web/ext/
cp -f node_modules/moment/min/moment.min.js web/ext/
cp -f node_modules/bluebird/js/browser/bluebird.min.js web/ext/
cp -f node_modules/lodash/lodash.min.js web/ext/
cp -f node_modules/datatables.net/js/jquery.dataTables.js web/ext/
cp -f node_modules/datatables.net-dt/css/jquery.dataTables.css web/ext/
