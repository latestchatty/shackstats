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
    var regex = new RegExp('[\\?&]' + name + '=([^&#]*)');
    var results = regex.exec(location.search);
    var resultOrEmpty = results === null ? '' : decodeURIComponent(results[1].replace(/\+/g, ' '));
    return resultOrEmpty === "" ? null : resultOrEmpty;
}

function parseDataset(x) {
    switch (x) {
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
    return new Promise(function(resolve, reject) {
        $("div#footer").append($("<span class=\"download\"><a href=\"" + dataRoot + filename + "\">" +
            "<i class=\"fa fa-file-text-o\" aria-hidden=\"true\"></i>" + filename + "</a></span>"));

        $.ajax({
            url: dataRoot + filename,
            success: function(data) {
                Papa.parse(data, {
                    header: true,
                    error: function(err) {
                        reject(err.toString());
                    },
                    complete: function(results) {
                        resolve(results.data);
                    }
                });
            },
            error: function(jqXHR, textStatus, errorThrown) {
                reject(textStatus.toString());
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

$(document).ready(function() {
    var options = {
        dataset: parseDataset(getUrlParameter("dataset")) || "totalPosts",
        groupBy: parseGroup(getUrlParameter("group")) || "month",
        startDate: parseDate(getUrlParameter("startDate")),
        endDate: parseDate(getUrlParameter("endDate")),
        newUserFilter: parseNewUserFilter(getUrlParameter("newUserFilter")) || "10plus",
        author: getUrlParameter("author"),
        category: parseCategory(getUrlParameter("category")),
        display: parseDisplay(getUrlParameter("display"))
    };

    var groupAdjectives = {
        day: "Daily",
        week: "Weekly",
        month: "Monthly",
        year: "Yearly"
    };

    var userIdDict = {}; // lowercase username -> user id
    var usernameDict = {}; // user id -> username 

    // need to ultimately produce this result from generateChart():
    //  {
    //      chartTitle: string,
    //      xAxisLabel: string,
    //      yAxisLabel: string,
    //      values: { x: Date, y: number }[]
    //  }

    var datasets = [
        {
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
            defaultDisplay: options.groupBy === "day" ? "scatter" : "line",
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
            name: "activeUsers",
            title: "Active Users",
            unit: "User count",
            showOptions: [
                "div#groupOption", 
                "div#startDateOption",
                "div#endDateOption",
            ],
            defaultDisplay: options.groupBy === "day" ? "scatter" : "line",
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
                    yAxisLabel: "Number of Active Users",
                    values: filterPointsByDateRange(options, points)
                });
            }
        },
        {
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
            defaultDisplay: options.groupBy === "day" ? "scatter" : "line",
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

    $("span#datasetLinks").append(
        datasets
        .map(function(x) { return "<a href=\"?dataset=" + x.name + "\"><span style=\"color: black; margin-right: 5px;\"><i class=\"fa fa-area-chart\" aria-hidden=\"true\"></i></span>" + x.title + "</a>"; })
        .join(" &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;")
    );

    var dataset = datasets.filter(function(x) { return x.name == options.dataset; })[0];
    options.display = options.display || dataset.defaultDisplay;
    $(dataset.showOptions.join()).css("display", "table-cell");

    $("div#datasetTitle").text(dataset.title);
    $("input#datasetField").val(options.dataset);
    $("select#groupCmb").val(options.groupBy);
    $("input#startDateTxt").val(options.startDate === null ? "" : moment(options.startDate).format("MM/DD/YYYY"));
    $("input#endDateTxt").val(options.endDate === null ? "" : moment(options.endDate).format("MM/DD/YYYY"));
    $("input#authorTxt").val(options.author || "");
    $("select#categoryCmb").val(options.category || "");
    $("select#displayCmb").val(options.display || dataset.defaultDisplay);
    $("select#newUserFilterCmb").val(options.newUserFilter);
    $("div#optionsContainer").css("visibility", "visible");

    resolveP()
        .then(function() {
            return downloadCsv("users.csv");
        })
        .then(function(usersCsvData) {
            usersCsvData.forEach(function(x) {
                userIdDict[x.username.toString().toLowerCase()] = x.user_id;
                usernameDict[x.user_id] = x.username; 
            });
            return dataset.getCsvFilename();
        })
        .then(function(csvFilename) {
            $("code#csvFilename").text(csvFilename);
            $("a#csvLink").attr("href", dataRoot + csvFilename).css("visibility", "visible");
            return downloadCsv(csvFilename);
        })
        .then(function(csvData) {
            return dataset.generateChartInfo(csvData);
        })
        .then(function(chartInfo) {
            switch (options.display) {
                case "table":
                    createDataTable(dataset, chartInfo, options);
                    break;
                case "line":
                case "scatter":
                    createGraphCanvas(dataset, chartInfo, options); 
                    break;
                case "csv":
                    break;
            }
        })
        .catch(function(err) {
            $("div#loading").text("Error loading data: " + err.toString());
        });
});

function createDataTable(dataset, chartInfo, options) {
    var data = _.map(chartInfo.values, function(pt) { return [moment(pt.x).format("YYYY-MM-DD"), pt.y]; });
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
        order: [[0, "asc"]]
    });

    $("div#datatableTitle").text(chartInfo.chartTitle).css("display", "block");
    $("div#loading").css("display", "none");
    $("table#datatable").css("display", "block");
}

function createGraphCanvas(dataset, chartInfo, options) {
    Chart.defaults.global.defaultFontFamily = "'Arial', 'Helvetica', sans-serif";
    Chart.defaults.global.defaultFontSize = 14;
    new Chart($("canvas#graph"), {
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
    $("canvas#graph").css("display", "block");
}
