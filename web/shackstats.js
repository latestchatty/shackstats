// ShackStats
// Copyright (C) 2016 Brian Luft
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
// Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
// OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"use strict";

var dataRoot = "https://shackstats.com/data/";

// returns non-empty string or null.
function getUrlParameter(name) {
    name = name.replace(/[\[]/, '\\[').replace(/[\]]/, '\\]');
    var regex = new RegExp('[#&]' + name + '=([^&#]*)');
    var results = regex.exec(window.location.hash);
    var resultOrEmpty = (results === null ? "" : decodeURIComponent(results[1].replace(/\+/g, ' '))).trim();
    return resultOrEmpty === "" ? null : resultOrEmpty;
}

function parseDataset(x) {
    switch (x) {
        case "topPosters":
        case "totalPosts":
        case "activeUsers":
        case "newUsers":
        case "newUsersWithTenPosts":
            return x;
        default:
            return null;
    }
}

function parseGroup(x) {
    switch (x) {
        case "day":
        case "week":
        case "month":
        case "year":
            return x;
        default:
            return null;
    }
}

function parseDate(x) {
    if (x === null) {
        return null;
    } else if (x.toString().match(/^[0-9][0-9][0-9][0-9]$/)) {
        return moment(x + "-01-01").toDate();
    } else {
        var m = moment(x);
        return m.isValid() ? m.toDate() : null;
    }
}

function parseCategory(x) {
    switch (x) {
        case "ontopic":
        case "nws":
        case "stupid":
        case "political":
        case "tangent":
        case "informative":
            return x;
        default:
            return null;
    }
}

function parseDisplay(x) {
    switch (x) {
        case "scatter":
        case "line":
        case "table":
            return x;
        default:
            return null;
    }
}

function parseNewUserFilter(x) {
    switch (x) {
        case "10plus":
        case "all":
            return x;
        default:
            return null;
    }
}

function downloadCsv(filename) { // Promise<any[]>
    var extraClass = filename !== "users.csv" ? " dataFileDownload" : "";
    return new Promise(function(resolve, reject) {
        Papa.parse(dataRoot + filename, {
            download: true,
            header: true,
            error: function(err) {
                $("div#footer").append($("<span class=\"download" + extraClass + "\">" +
                    "<i class=\"fa fa-warning\" aria-hidden=\"true\"></i>" + filename + "</span>"));
                reject("The requested data file was not found.");
            },
            complete: function(results) {
                $("div#footer").append($("<span class=\"download" + extraClass + "\"><a href=\"" + dataRoot + filename + "\">" +
                    "<i class=\"fa fa-file-text-o\" aria-hidden=\"true\"></i>" + filename + "</a></span>"));
                resolve(results.data);
            }
        });
    });
}

function valueColumnForCategory(category) {
    return (category || "total") + "_post_count";
}

function resolveP(value) {
    return new Promise(function(resolve, reject) {
        resolve(value);
    });
}

function rejectP(message) {
    return new Promise(function(resolve, reject) {
        reject(message);
    });
}

function filterPointsByDateRange(options, points) {
    return _.filter(points, function(point) {
        if (options.startDate !== null && point.x < options.startDate) {
            return false;
        } else if (options.endDate !== null && point.x > options.endDate) {
            return false;
        } else {
            return true;
        }
    });
}

function getDelayedCallWrapper(func, seconds) {
    var token = 0;
    return function() {
        token++;
        var thisToken = token;
        setTimeout(function() {
            if (thisToken === token) {
                func();
            }
        }, seconds * 1000);
    };
}

var groupAdjectives = {
    day: "Daily",
    week: "Weekly",
    month: "Monthly",
    year: "Yearly"
};

var userIdDict = {}; // lowercase username -> user id
var usernameDict = {}; // user id -> username
var usersLoaded = false;

var datasets = [
    {
        type: "scoreboard",
        name: "topPosters",
        title: "Top Posters",
        showOptions: [
            "div#periodOption",
            "div#periodDateOption",  
            "div#authorDivider",
            "div#categoryOption",
        ],
        optionKeys: [
            "dataset", "periodType", "periodDate", "category", "display"
        ],
        defaultDisplay: "table",
        getCsvFilename: function() {
            var periodStartDate = moment(options.periodDate).startOf(options.periodType).format("YYYYMMDD");
            var filename = "post_counts_by_user_for_" + options.periodType + "_" + periodStartDate + ".csv";  
            return resolveP(filename);
        },
        generateChartInfo: function(csvData) {
            var col = valueColumnForCategory(options.category);
            var points = _.map(csvData, function(row) {
                return { x: usernameDict[row.user_id], y: parseInt(row[col]) }
            });
            var filteredPoints = _.filter(points, function(pt) { return pt.y > 0; });
            var orderedPoints = _.orderBy(filteredPoints, ["y"], ["desc"]);
            var dateFormat =
                options.periodType === "month" ? "MM/YYYY" :
                options.periodType === "year" ? "YYYY" :
                "MM/DD/YYYY";
            var periodStartDate = moment(options.periodDate).startOf(options.periodType).format(dateFormat);
            var titleCategoryPart = options.category === null ? "" : "\"" + options.category + "\" ";
            return resolveP({
                chartTitle: "Top " + titleCategoryPart + "posters for " + options.periodType + " of " + periodStartDate,
                xAxisLabel: "User",
                yAxisLabel: "Number of posts",
                values: orderedPoints
            });
        }
    },
    {
        type: "changeOverTime",
        name: "totalPosts",
        title: "Posts",
        unit: "Post count",
        showOptions: [
            "div#groupOption", 
            "div#startDateOption",
            "div#endDateOption",
            "div#authorDivider",
            "div#authorOption",
            "div#categoryOption",
        ],
        optionKeys: [
            "dataset", "groupBy", "startDate", "endDate", "author", "category", "display"
        ],
        defaultDisplay: "line",
        getCsvFilename: function() {
            if (options.author !== null) {
                var lcUsername = options.author.toString().toLowerCase();
                if (userIdDict.hasOwnProperty(lcUsername)) {
                    var userId = userIdDict[lcUsername];
                    return resolveP("daily_post_counts_for_user_" + userId + ".csv");
                } else {
                    return rejectP("The user \"" + options.author + "\" was not found.");
                }
            } else {
                return resolveP("daily_post_counts.csv");
            }
        },
        generateChartInfo: function(csvData) {
            var col = valueColumnForCategory(options.category);
            var groups = _.groupBy(csvData, function(row) {
                return moment(row.date).startOf(options.groupBy).format("YYYY-MM-DD");
            });
            var dates = _.keys(groups);
            var points = _.map(dates, function(date) {
                var counts = _.map(groups[date], function(x) { return parseInt(x[col]); });
                var totalCount = _.reduce(counts, function(a, b) { return a + b; }, 0);
                return { x: moment(date).toDate(), y: totalCount };
            });
            var title = groupAdjectives[options.groupBy] + " posts"
                + (options.author !== null ? " by \"" + usernameDict[userIdDict[options.author.toLowerCase()]] + "\"" : "")
                + (options.category !== null ? " flagged \"" + options.category + "\"" : "");
            return resolveP({
                chartTitle: title,
                xAxisLabel: "Date",
                yAxisLabel: "Number of Posts",
                values: filterPointsByDateRange(options, points)
            });
        }
    },
    {
        type: "changeOverTime",
        name: "activeUsers",
        title: "Active Users",
        unit: "User count",
        showOptions: [
            "div#groupOption", 
            "div#startDateOption",
            "div#endDateOption",
        ],
        optionKeys: [
            "dataset", "groupBy", "startDate", "endDate", "display"
        ],
        defaultDisplay: "line",
        getCsvFilename: function() {
            switch (options.groupBy) {
                case "day": return resolveP("daily_poster_counts.csv");
                case "week": return resolveP("weekly_poster_counts.csv");
                case "month": return resolveP("monthly_poster_counts.csv");
                case "year": return resolveP("yearly_poster_counts.csv");
                default: return rejectP("Unrecognized grouping.");
            }
        },
        generateChartInfo: function(csvData) {
            var points = _.map(csvData, function(row) {
                return { x: moment(row.date).toDate(), y: parseInt(row.poster_count) };
            });
            return resolveP({
                chartTitle: groupAdjectives[options.groupBy] + " active users",
                xAxisLabel: "Date",
                yAxisLabel: "Number of active users",
                values: filterPointsByDateRange(options, points)
            });
        }
    },
    {
        type: "changeOverTime",
        name: "newUsers",
        title: "New Users",
        unit: "User count",
        showOptions: [
            "div#groupOption", 
            "div#startDateOption",
            "div#endDateOption",
            "div#authorDivider",
            "div#newUserFilterOption"
        ],
        optionKeys: [
            "dataset", "groupBy", "startDate", "endDate", "newUserFilter", "display"
        ],
        defaultDisplay: "line",
        getCsvFilename: function() {
            var tenPlus = options.newUserFilter === "10plus" ? "10plus_" : ""; 
            switch (options.groupBy) {
                case "day": return resolveP("daily_new_" + tenPlus + "poster_counts.csv");
                case "week": return resolveP("weekly_new_" + tenPlus + "poster_counts.csv");
                case "month": return resolveP("monthly_new_" + tenPlus + "poster_counts.csv");
                case "year": return resolveP("yearly_new_" + tenPlus + "poster_counts.csv");
                default: return rejectP("Unrecognized grouping.");
            }
        },
        generateChartInfo: function(csvData) {
            var points = _.map(csvData, function(row) {
                return { x: moment(row.date).toDate(), y: parseInt(row.new_poster_count) };
            });
            var tenPlus = options.newUserFilter === "10plus" ? " with 10+ posts" : "";
            return resolveP({
                chartTitle: groupAdjectives[options.groupBy] + " new users" + tenPlus,
                xAxisLabel: "Date",
                yAxisLabel: "Number of new users" + tenPlus,
                values: filterPointsByDateRange(options, points)
            });
        }
    }
];

var options = {};

function setOptions(args) {
    options.dataset = parseDataset(args.dataset) || "topPosters";
    var dataset = datasets.filter(function(x) { return x.name == options.dataset; })[0];
    options.groupBy = parseGroup(args.groupBy) || "month";
    options.periodType = parseGroup(args.periodType) || "day";
    options.periodDate = parseDate(args.periodDate) || moment().add(-1, "day").toDate();
    options.startDate = parseDate(args.startDate);
    options.endDate = parseDate(args.endDate);
    options.newUserFilter = parseNewUserFilter(args.newUserFilter) || "10plus";
    options.author = args.author;
    options.category = parseCategory(args.category);
    options.display = parseDisplay(args.display);
    if (dataset.type === "scoreboard") {
        options.display = "table";
    }
}

var lastSeenHash = window.location.hash;

function readOptionsFromForm() {
    function processInput(x) {
        if (x === null) {
            return null;
        }
        x = x.toString().trim();
        return x === "" ? null : x.toString().trim();
    }

    var inputValues = {
        dataset: options.dataset,
        groupBy: processInput($("select#groupCmb").val()),
        periodType: processInput($("select#periodTypeCmb").val()),
        periodDate: processInput($("input#periodDateTxt").val()),
        startDate: processInput($("input#startDateTxt").val()),
        endDate: processInput($("input#endDateTxt").val()),
        newUserFilter: processInput($("select#newUserFilterCmb").val()),
        author: processInput($("input#authorTxt").val()),
        category: processInput($("select#categoryCmb").val()),
        display: processInput($("select#displayCmb").val())
    };
    var dataset = datasets.filter(function(x) { return x.name == options.dataset; })[0];
    var keys = _.filter(dataset.optionKeys, function(x) {
        var value = inputValues[x];
        return value !== null && value.toString() !== "";
    });
    var pairs = _.map(keys, function(x) { return x + "=" + encodeURIComponent(inputValues[x]); });
    lastSeenHash = "#" + pairs.join("&");
    location.replace("#" + pairs.join("&"));
    setOptions(inputValues);
}

function readOptionsFromHash() {
    setOptions({
        dataset: getUrlParameter("dataset"),
        groupBy: getUrlParameter("groupBy"),
        periodType: getUrlParameter("periodType"),
        periodDate: getUrlParameter("periodDate"),
        startDate: getUrlParameter("startDate"),
        endDate: getUrlParameter("endDate"),
        newUserFilter: getUrlParameter("newUserFilter"),
        author: getUrlParameter("author"),
        category: getUrlParameter("category"),
        display: getUrlParameter("display")
    });
    setFormFromOptions();
}

function doLoad() {
    var dataset = datasets.filter(function(x) { return x.name == options.dataset; })[0];
    options.display = options.display || dataset.defaultDisplay;
    $("a.datasetLink").removeClass("selectedDataset");
    $("a#datasetLink" + options.dataset).addClass("selectedDataset");
    $("div.option").css("display", "none");
    $("div.option.alwaysShown").css("display", "table-cell");
    $(dataset.showOptions.join()).css("display", "table-cell");
    if (dataset.type === "scoreboard") {
        $("option#scatterChoice, option#lineChoice").attr("disabled", "disabled");
        options.display = "table";
    } else {
        $("option#scatterChoice, option#lineChoice").removeAttr("disabled");
    }

    $("div#loading").text("Loading...").css("display", "block");
    $("div#datatableContainer").html("").css("display", "none");
    $("div#chartContainer").css("display", "none");
    $("span.dataFileDownload").remove();        
    resolveP()
        .then(function() {
            if (usersLoaded) {
                return resolveP(null);
            } else {
                return downloadCsv("users.csv");
            }
        })
        .then(function(usersCsvData) {
            if (!usersLoaded) {
                usersCsvData.forEach(function(x) {
                    userIdDict[x.username.toString().toLowerCase()] = x.user_id;
                    usernameDict[x.user_id] = x.username; 
                });
                usersLoaded = true;
            }
            return dataset.getCsvFilename();
        })
        .then(function(csvFilename) {
            return downloadCsv(csvFilename);
        })
        .then(function(csvData) {
            return dataset.generateChartInfo(csvData);
        })
        .then(function(chartInfo) {
            switch (options.display || dataset.defaultDisplay) {
                case "table":
                    createDataTable(dataset, chartInfo, options);
                    break;
                case "line":
                case "scatter":
                    createGraphCanvas(dataset, chartInfo, options); 
                    break;
            }
        })
        .catch(function(err) {
            $("div#loading").text(err.toString());
        });
}

function setFormFromOptions() {
    var dataset = datasets.filter(function(x) { return x.name == options.dataset; })[0];
    $("select#groupCmb").val(options.groupBy);
    $("select#periodTypeCmb").val(options.periodType);
    $("input#periodDateTxt").val(moment(options.periodDate).format("MM/DD/YYYY"));
    $("input#startDateTxt").val(options.startDate === null ? "" : moment(options.startDate).format("MM/DD/YYYY"));
    $("input#endDateTxt").val(options.endDate === null ? "" : moment(options.endDate).format("MM/DD/YYYY"));
    $("input#authorTxt").val(options.author || "");
    $("select#categoryCmb").val(options.category || "");
    $("select#displayCmb").val(options.display || dataset.defaultDisplay);
    $("select#newUserFilterCmb").val(options.newUserFilter);
}

$(document).ready(function() {
    $.fn.dataTable.ext.errMode = "none";

    readOptionsFromHash();

    $("span#datasetLinks").append(
        datasets
        .map(function(x) {
            var icon = x.type == "scoreboard" ? "fa-list" : "fa-area-chart";
            return "<a href=\"#dataset=" + x.name + "\" class=\"datasetLink\" id=\"datasetLink" + x.name + "\">" +
                "<span style=\"margin-right: 5px;\">" +
                "<i class=\"fa " + icon + "\" aria-hidden=\"true\"></i></span>" + x.title + "</a>";
        })
        .join("")
    );

    var dataset = datasets.filter(function(x) { return x.name == options.dataset; })[0];
    setFormFromOptions();

    $("select#groupCmb, select#periodTypeCmb, select#newUserFilterCmb, select#categoryCmb, select#displayCmb")
        .change(function() {
            readOptionsFromForm();
            doLoad();
        });

    ["input#periodDateTxt", "input#startDateTxt", "input#endDateTxt", "input#authorTxt"].forEach(function(sel) {
        var elem = $(sel);
        var oldValue = elem.val();
        elem.on("change keyup paste",
            getDelayedCallWrapper(function() {
                if (elem.val() != oldValue) {
                    oldValue = elem.val(); 
                    readOptionsFromForm();
                    doLoad();
                }
            }, 0.5));
    });

    window.onhashchange = function() {
        if (window.location.hash != lastSeenHash) {
            lastSeenHash = window.location.hash;
            readOptionsFromHash();
            doLoad();
        }
    };

    $("div#optionsContainer").css("visibility", "visible");

    doLoad();
});

function createDataTable(dataset, chartInfo, options) {
    var data, order;
    
    if (dataset.type === "scoreboard") {
        data = _.map(chartInfo.values, function(pt) { return [pt.x, pt.y]; });
        order = [[1, "desc"]];
    } else {
        data = _.map(chartInfo.values, function(pt) { return [moment(pt.x).format("YYYY-MM-DD"), pt.y]; });
        order = [[0, "asc"]];
    }

    $("div#datatableContainer")
        .html("<div id=\"datatableTitle\"></div><table id=\"datatable\"></table>")
        .css("display", "block");
    $("div#datatableTitle").text(chartInfo.chartTitle);

    var table = $("table#datatable");
    table.dataTable({
        data: data,
        columns: [
            { title: chartInfo.xAxisLabel },
            { title: chartInfo.yAxisLabel },
        ],
        lengthChange: false,
        searching: false,
        pagingType: "full_numbers",
        pageLength: 25,
        order: order
    });

    $("div#loading").css("display", "none");
}

var chartInstance = null;
function createGraphCanvas(dataset, chartInfo, options) {
    Chart.defaults.global.defaultFontFamily = "'Arial', 'Helvetica', sans-serif";
    Chart.defaults.global.defaultFontSize = 14;

    if (chartInstance !== null) {
        chartInstance.destroy();
    }

    var canvas = $("<canvas id=\"graph\"></canvas>");
    $("div#chartContainer").html("").append(canvas);

    chartInstance = new Chart(canvas, {
        type: "line",
        data: {
            datasets: [{
                label: dataset.unit,
                data: chartInfo.values
            }] 
        },
        options: {
            showLines: options.display == "line",
            responsive: true,
            maintainAspectRatio: false,
            title: {
                display: true,
                text: chartInfo.chartTitle,
            },
            legend: {
                display: false,
            },
            animation: {
                duration: 0
            },
            hover: {
                animationDuration: 0
            },
            scales: {
                xAxes: [{
                    type: "time",
                    position: "bottom",
                    scaleLabel: {
                        display: true,
                        labelString: chartInfo.xAxisLabel
                    }
                }],
                yAxes: [{
                    type: "linear",
                    position: "left",
                    ticks: {
                        beginAtZero: true
                    },
                    scaleLabel: {
                        display: true,
                        labelString: chartInfo.yAxisLabel
                    }
                }]
            }
        }
    });

    $("table#datatable").css("display", "none");
    $("div#loading").css("display", "none");
    $("div#chartContainer, canvas#graph").css("display", "block");
}
