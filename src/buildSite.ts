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
import * as _ from "lodash";
import * as aws from "aws-sdk";
import * as babyparse from "babyparse";
import * as crypto from "crypto";
import * as fs from "fs";
import * as glob from "glob";
import * as moment from "moment-timezone";
import * as path from "path";
import * as pg from "pg";
import * as process from "process";
import { Dictionary } from "./Dictionary";

// set to 0 in production, increase to make things faster in development
const MIN_POST_ID: number = 0; //35500000;

// set to true in production, false to skip uploading to S3
const DO_UPLOAD: boolean = true;

// --------------------------------------------------------------------------------------------------------------------

function getEnv(key: string): string {
    if (process.env.hasOwnProperty(key)) {
        return process.env[key];
    } else {
        console.error(`The environment variable "${key}" must be provided.`);
        process.exit(1);
    }
}

interface Config {
    webDir: string;
    dataDir: string;
    pg: pg.Client;
}

async function go(): Promise<void> {
    // these environment variables are used implicitly by the AWS SDK
    getEnv("AWS_ACCESS_KEY_ID");
    getEnv("AWS_SECRET_ACCESS_KEY");

    const webDir = getEnv("SHACKSTATS_WEB_DIR");
    const dataDir =  path.join(webDir, "data");
    fs.accessSync(dataDir);

    const config: Config = {
        webDir: webDir,
        dataDir: dataDir,
        pg: new pg.Client({
            user: getEnv("SHACKSTATS_PG_USERNAME"),
            database: getEnv("SHACKSTATS_PG_DATABASE"),
            password: getEnv("SHACKSTATS_PG_PASSWORD"),
            port: parseInt(getEnv("SHACKSTATS_PG_PORT")),
            host: getEnv("SHACKSTATS_PG_HOST")
        })
    };

    await pgConnect(config.pg);
    const userIdMap = await buildUsersFile(config);
    await buildUserPostCountsFiles(config, userIdMap);
    await buildPostCountsFile(config);
    await buildPeriodUserPostCountsFiles(config, userIdMap);
    await buildPosterCountFiles(config);
    await buildNewPosterCountsFile(config, 0, "new_poster_counts");
    await buildNewPosterCountsFile(config, 10, "new_10plus_poster_counts");
    await buildFilesFile(config);

    if (DO_UPLOAD) {
        await uploadFiles(config);
    }
}

async function pgConnect(client: pg.Client): Promise<void> {
    return new Promise<void>((resolve, reject) => {
        client.connect((err: Error) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

async function pgQuery(client: pg.Client, sql: string, values?: any[]): Promise<any[]> {
    console.info("=====");
    console.info(sql);
    if (typeof values !== "undefined") {
        values.forEach((x, i) => {
            console.info(`$${i+1} = ${x}`);
        });
    }
    const startMsec = new Date().getTime();
    const result = await client.query(sql, values);
    const durationMsec = new Date().getTime() - startMsec;
    console.info(`${result.rowCount} row(s) returned in ${durationMsec} msec.`);
    return result.rows;
}

async function pgQuerySingle(client: pg.Client, sql: string, values?: any[]): Promise<any> {
    const rows = await pgQuery(client, sql, values);
    if (rows.length != 1) {
        throw new Error(`Expected one row, instead received ${rows.length} rows.`);
    }
    return rows[0];
}

async function readFile(filePath: string): Promise<Buffer> {
    return new Promise<Buffer>((resolve, reject) => {
        fs.readFile(filePath, (err, data) => {
            if (err) {
                reject(err);
            } else {
                resolve(data);
            }
        });
    });
}

async function writeFile(filePath: string, contents: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
        fs.writeFile(filePath, contents, err => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

function quoteCsvValue(value: any) {
    if (Number.isInteger(value)) {
        return value.toString();
    } else {
        return `"${value.toString().replace('"', '""')}"`;
    }
}

async function writeCsvFile<T>(config: Config, filename: string, rows: T[], cols: string[],
selectors: ((x: T) => any)[]) {
    const filePath = path.join(config.dataDir, filename);
    const lines = rows.map(row => selectors.map(selector => quoteCsvValue(selector(row))).join());
    const headerLine = cols.map(x => `"${x.replace('"', '""')}"`).join();
    const text = headerLine + "\n" + lines.join("\n");
    await writeFile(filePath, text);
}

async function writeJsonFile(config: Config, filename: string, data: any): Promise<void> {
    const filePath = path.join(config.dataDir, filename);
    const contents = JSON.stringify(data);
    await writeFile(filePath, contents);
} 

// --------------------------------------------------------------------------------------------------------------------

async function buildFilesFile(config: Config): Promise<void> {
    const filePaths = await new Promise<string[]>((resolve, reject) => {
        glob(path.join(config.dataDir, "*.csv"), {}, function (err, files) {
            if (err) {
                reject(err);
            } else {
                resolve(files);
            }
        });
    });

    const rows: { filename: string, sha256: string, size: number }[] = [];
    for (var i = 0; i < filePaths.length; i++) {
        const filePath = filePaths[i];
        const filename = path.basename(filePath);
        if (filename != "files.csv" && filename != "file_hashes.csv") {
            const buffer = await readFile(filePath);
            const sha256 = crypto.createHash("sha256").update(buffer).digest("hex");
            rows.push({ filename: filename, sha256: sha256, size: buffer.length });
        }
    }

    await writeCsvFile(config, "files.csv", rows, ["filename"], [x => x.filename]);

    await writeCsvFile(config, "file_hashes.csv", rows, ["filename", "sha256", "size"], [
        x => x.filename, x => x.sha256, x => x.size
    ]);
}

// "date" is formatted using TO_CHAR(..., 'YYYY-MM-DD HH24:MI:SS')
function postDateToUtc(date: string): string {
    return moment.tz(date, "America/Chicago").tz("UTC").format();
}

// users.csv, users_info.csv
async function buildUsersFile(config: Config): Promise<Dictionary<string, string>> {
    function isAlpha(str: string): boolean {
        return str.length === 1 && str.match(/[A-Za-z]/i) !== null;
    }

    function getPrefix(username: string): string {
        const prefixLength = 10;
        var prefix = "";
        username.split("").forEach(ch => {
            if (prefix.length < 10 && isAlpha(ch)) {
                prefix += ch.toLowerCase();
            }
        });
        if (prefix === "") {
            prefix = "a";
        }
        return prefix;
    }

    const rs = await pgQuery(config.pg,
        `SELECT MIN(author) AS username, author_c, MIN(id) AS first_post_id,
            TO_CHAR(MIN(date), 'YYYY-MM-DD HH24:MI:SS') AS first_post_date, COUNT(*) AS post_count
        FROM post
        WHERE id > $1
        GROUP BY author_c
        ORDER BY MIN(id), author_c`,
        [MIN_POST_ID]);
    const seen = new Dictionary<string, string>(); // id -> username
    const userIdMap = new Dictionary<string, string>(); // author_c -> id
    rs.forEach(x => {
        const prefix = getPrefix(x.username);
        var suffix = 2;
        var userId = prefix; // try without suffix first
        while (seen.containsKey(userId)) {
            userId = prefix + suffix;
            suffix++;
        }
        seen.add(userId, x.username);
        userIdMap.add(x.author_c, userId);
        x.user_id = userId;
    });
    await writeCsvFile(config, "users.csv", rs, ["user_id", "username"], [
        x => x.user_id,
        x => x.username,
    ]);
    await writeCsvFile(config, "users_info.csv", rs, ["user_id", "username", "first_post_id", "first_post_date", "post_count"], [
        x => x.user_id,
        x => x.username,
        x => parseInt(x.first_post_id),
        x => postDateToUtc(x.first_post_date),
        x => parseInt(x.post_count)
    ]);
    return userIdMap;
}

const PERIODS = [["day", "daily"], ["week", "weekly"], ["month", "monthly"], ["year", "yearly"]];

// daily_post_counts.csv
async function buildPostCountsFile(config: Config): Promise<void> {
    const rs = await pgQuery(config.pg,
        `SELECT
            TO_CHAR(DATE_TRUNC('day', p.date), 'YYYY-MM-DD') AS date,
            p.category, COUNT(*) AS post_count
        FROM post p
        WHERE p.id > $1
        GROUP BY DATE_TRUNC('day', p.date), p.category
        ORDER BY DATE_TRUNC('day', p.date), p.category`,
        [MIN_POST_ID]);

    const dateGroups = _.groupBy(rs, x => x.date);
    const rows = _.keys(dateGroups).map(key => {
        const groupRows = dateGroups[key];
        const dict = new Dictionary<number, number>(); // category -> count
        groupRows.forEach(row => {
            dict.set(parseInt(row.category), parseInt(row.post_count));
        });
        return {
            date: groupRows[0].date,
            total_post_count: dict.values().reduce((a,b) => a + b),
            ontopic_post_count: dict.lookup(1, 0),
            nws_post_count: dict.lookup(2, 0),
            stupid_post_count: dict.lookup(3, 0),
            political_post_count: dict.lookup(4, 0),
            tangent_post_count: dict.lookup(5, 0),
            informative_post_count: dict.lookup(6, 0),
        };
    });

    // fill in missing days
    const rowsByDate = Dictionary.fromArray(rows, x => x.date, x => x);
    const minDate = rows.map(x => x.date).reduce((a,b) => a < b ? a : b);
    const maxDate = rows.map(x => x.date).reduce((a,b) => a < b ? b : a);
    const expandedRows = getAllDays(minDate, maxDate).map(date => {
        if (rowsByDate.containsKey(date)) {
            return rowsByDate.get(date);
        } else {
            return {
                date: date,
                total_post_count: 0,
                ontopic_post_count: 0,
                nws_post_count: 0,
                stupid_post_count: 0,
                political_post_count: 0,
                tangent_post_count: 0,
                informative_post_count: 0,
            };
        }
    });

    await writeCsvFile(config, `daily_post_counts.csv`, expandedRows,
        ["date", "total_post_count", "ontopic_post_count", "nws_post_count", "stupid_post_count",
            "political_post_count", "tangent_post_count", "informative_post_count"],
        [
            x => x.date,
            x => x.total_post_count,
            x => x.ontopic_post_count,
            x => x.nws_post_count,
            x => x.stupid_post_count,
            x => x.political_post_count,
            x => x.tangent_post_count,
            x => x.informative_post_count, 
        ]);
}

// daily_post_counts_for_user_(user id).csv
async function buildUserPostCountsFiles(config: Config, userIdMap: Dictionary<string, string>): Promise<void> {
    const rs = await pgQuery(config.pg,
        `SELECT
            author_c, TO_CHAR(DATE_TRUNC('day', p.date), 'YYYY-MM-DD') AS date,
            p.category, COUNT(*) AS post_count
        FROM post p
        WHERE p.id > $1
        GROUP BY author_c, DATE_TRUNC('day', p.date), p.category
        ORDER BY author_c, DATE_TRUNC('day', p.date), p.category`,
        [MIN_POST_ID]);
    
    const userDateGroups = _.groupBy(rs, x => `${x.date}_${x.author_c}`);
    const rows = _.keys(userDateGroups).map(key => {
        const groupRows = userDateGroups[key];
        const dict = new Dictionary<number, number>(); // category -> count
        groupRows.forEach(row => {
            dict.set(parseInt(row.category), parseInt(row.post_count));
        });
        return {
            date: groupRows[0].date,
            user_id: userIdMap.get(groupRows[0].author_c),
            total_post_count: dict.values().reduce((a,b) => a + b),
            ontopic_post_count: dict.lookup(1, 0),
            nws_post_count: dict.lookup(2, 0),
            stupid_post_count: dict.lookup(3, 0),
            political_post_count: dict.lookup(4, 0),
            tangent_post_count: dict.lookup(5, 0),
            informative_post_count: dict.lookup(6, 0),
        };
    });

    const rowsByUser = _.groupBy(rows, x => x.user_id);
    const userIds = _.keys(rowsByUser);
    for (var j = 0; j < userIds.length; j++) {
        const userId = userIds[j];
        const userRows = rowsByUser[userId];

        // fill in missing days
        const rowsByDate = Dictionary.fromArray(userRows, x => x.date, x => x);
        const minDate = userRows.map(x => x.date).reduce((a,b) => a < b ? a : b);
        const maxDate = userRows.map(x => x.date).reduce((a,b) => a < b ? b : a);
        const expandedRows = getAllDays(minDate, maxDate).map(date => {
            if (rowsByDate.containsKey(date)) {
                return rowsByDate.get(date);
            } else {
                return {
                    date: date,
                    user_id: userId,
                    total_post_count: 0,
                    ontopic_post_count: 0,
                    nws_post_count: 0,
                    stupid_post_count: 0,
                    political_post_count: 0,
                    tangent_post_count: 0,
                    informative_post_count: 0,
                };
            }
        });

        await writeCsvFile(config, `daily_post_counts_for_user_${userId}.csv`, expandedRows,
            ["date", "user_id", "total_post_count", "ontopic_post_count", "nws_post_count",
                "stupid_post_count", "political_post_count", "tangent_post_count", "informative_post_count"],
            [
                x => x.date,
                x => userId,
                x => x.total_post_count,
                x => x.ontopic_post_count,
                x => x.nws_post_count,
                x => x.stupid_post_count,
                x => x.political_post_count,
                x => x.tangent_post_count,
                x => x.informative_post_count, 
            ]);
    }
}

function getAllDays(startDate: string, endDate: string): string[] { // "YYYY-MM-DD""
    const firstDay = moment(startDate);
    const lastDay = moment(endDate);
    const numDays = lastDay.diff(firstDay, "days") + 1;
    const list: string[] = [];
    for (var date = firstDay.clone(), i = 0; i < numDays; date.add(1, "day"), i++) {
        list.push(date.format("YYYY-MM-DD"));
    }
    return list;
}

function getAllPeriods(): { noun: string, filenameSuffix: string, date: string }[] {
    const result: {noun: string, filenameSuffix: string, date: string}[] = [];
    const firstDay = moment("1999-06-01"); 
    const lastDay = moment(new Date());
    const numDays = lastDay.diff(firstDay, "days") + 1;

    for (var date = firstDay.clone(), i = 0; i < numDays; date.add(1, "day"), i++) {
        result.push({
            noun: "day",
            filenameSuffix: `day_${date.format("YYYYMMDD")}`,
            date: date.format("YYYY-MM-DD")
        });

        if (date.day() == 0) { // sunday (first of the week)
            result.push({
                noun: "week",
                filenameSuffix: `week_${date.format("YYYYMMDD")}`,
                date: date.format("YYYY-MM-DD")
            });
        }

        if (date.date() == 1) { // first of the month
            result.push({
                noun: "month",
                filenameSuffix: `month_${date.format("YYYYMMDD")}`,
                date: date.format("YYYY-MM-DD")
            });
        }

        if (date.dayOfYear() == 1) { // first of the year
            result.push({
                noun: "year",
                filenameSuffix: `year_${date.format("YYYYMMDD")}`,
                date: date.format("YYYY-MM-DD")
            });
        }
    }

    return result;
}

// post_counts_by_user_for_(day|week|month|year)_(YYYYMMDD).csv
async function buildPeriodUserPostCountsFiles(config: Config, userIdMap: Dictionary<string, string>): Promise<void> {
    const rs = await pgQuery(config.pg,
        `SELECT author_c, TO_CHAR(DATE_TRUNC('day', p.date), 'YYYY-MM-DD') AS date, p.category, COUNT(*) AS post_count
        FROM post p
        WHERE p.id > $1
        GROUP BY DATE_TRUNC('day', p.date), author_c, p.category
        ORDER BY DATE_TRUNC('day', p.date), author_c, p.category`,
        [MIN_POST_ID]);

    const filenames = new Dictionary<string, boolean>();
    for (var i = 0; i < PERIODS.length; i++) {
        const periodNoun = <"day"|"week"|"month"|"year">PERIODS[i][0];
        const periodAdjective = PERIODS[i][1]; 
        
        const userDateGroups = _.groupBy(rs, x => `${x.date}_${x.author_c}`);
        const rows = _.keys(userDateGroups).map(key => {
            const groupRows = userDateGroups[key];
            const dict = new Dictionary<number, number>(); // category -> count
            groupRows.forEach(row => {
                dict.set(parseInt(row.category), parseInt(row.post_count));
            });
            return {
                date: groupRows[0].date,
                user_id: userIdMap.get(groupRows[0].author_c),
                total_post_count: dict.values().reduce((a,b) => a + b),
                ontopic_post_count: dict.lookup(1, 0),
                nws_post_count: dict.lookup(2, 0),
                stupid_post_count: dict.lookup(3, 0),
                political_post_count: dict.lookup(4, 0),
                tangent_post_count: dict.lookup(5, 0),
                informative_post_count: dict.lookup(6, 0),
            };
        });
        
        const rowsByDate = _.groupBy(rows, x => x.date);
        const dates = _.keys(rowsByDate);
        for (var j = 0; j < dates.length; j++) {
            const date = dates[j];
            const m = moment(date, "YYYY-MM-DD");
            const dateRows = rowsByDate[date];
            const filename = `post_counts_by_user_for_${periodNoun}_${m.format("YYYYMMDD")}.csv`;
            filenames.add(filename, true);
            await writeCsvFile(config, filename, dateRows,
                ["period", "date", "user_id", "total_post_count", "ontopic_post_count", "nws_post_count",
                    "stupid_post_count", "political_post_count", "tangent_post_count", "informative_post_count"],
                [
                    x => periodNoun,
                    x => x.date,
                    x => x.user_id,
                    x => x.total_post_count,
                    x => x.ontopic_post_count,
                    x => x.nws_post_count,
                    x => x.stupid_post_count,
                    x => x.political_post_count,
                    x => x.tangent_post_count,
                    x => x.informative_post_count, 
                ]);
        }
    }

    // for any days that we missed because there were no posts, create a blank file
    const allPeriods = getAllPeriods();
    for (var i = 0; i < allPeriods.length; i++) {
        const x = allPeriods[i];
        const filename = `post_counts_by_user_for_${x.filenameSuffix}.csv`;
        if (!filenames.containsKey(filename)) {
            await writeCsvFile(config, filename, [],
                ["period", "date", "user_id", "total_post_count", "ontopic_post_count", "nws_post_count",
                    "stupid_post_count", "political_post_count", "tangent_post_count", "informative_post_count"],
                []);
        }
    }
}

// (daily|weekly|monthly|yearly)_poster_counts.csv
async function buildPosterCountFiles(config: Config): Promise<void> {
    for (var i = 0; i < PERIODS.length; i++) {
        const periodNoun = PERIODS[i][0];
        const periodAdjective = PERIODS[i][1];

        // DATE_TRUNC('week', ...) uses Monday as the beginning of the week
        const dateTruncExpr = 
            `(CASE WHEN '${periodNoun}' = 'week' THEN
                (DATE_TRUNC('${periodNoun}', p.date + '1 day'::interval) - '1 day'::interval)
            ELSE
                DATE_TRUNC('${periodNoun}', p.date) END)`;
            
        const rs = await pgQuery(config.pg,
            `SELECT TO_CHAR(${dateTruncExpr}, 'YYYY-MM-DD') AS date, COUNT(DISTINCT author_c) AS poster_count
            FROM post p
            WHERE p.id > $1
            GROUP BY ${dateTruncExpr}`,
            [MIN_POST_ID]);

        // fill in missing dates
        const rowsByDate = Dictionary.fromArray(rs, x => x.date, x => x);
        const expandedRows = getAllPeriods().filter(x => x.date == periodNoun).map(x => x.date).map(date => {
            if (rowsByDate.containsKey(date)) {
                return rowsByDate.get(date);
            } else {
                return {
                    period: periodNoun,
                    date: date,
                    poster_count: 0
                };
            }
        });

        await writeCsvFile(config, `${periodAdjective}_poster_counts.csv`, expandedRows,
            ["period", "date", "poster_count"],
            [
                x => periodNoun,
                x => x.date,
                x => parseInt(x.poster_count)
            ]);
    }
}

async function readUsersInfoFile(config: Config): Promise<{ firstPostDate: moment.Moment, postCount: number }[]> {
    return babyparse.parse(
        (await readFile(path.join(config.dataDir, "users_info.csv"))).toString(),
        { header: true }
    ).data.map(x => ({
        firstPostDate: moment.tz(x.first_post_date, "America/Chicago"),
        postCount: parseInt(x.post_count)
    }));
}

// (daily|weekly|monthly|yearly)_new_poster_counts.csv
// (daily|weekly|monthly|yearly)_new_10plus_poster_counts.csv
async function buildNewPosterCountsFile(config: Config, minPosts: number, filenameSuffix: string): Promise<void> {
    const users = (await readUsersInfoFile(config)).filter(x => x.postCount >= minPosts);
    const allPeriods = getAllPeriods();

    for (var i = 0; i < PERIODS.length; i++) {
        const periodNoun = <"day"|"week"|"month"|"year">PERIODS[i][0];
        const periodAdjective = PERIODS[i][1];
        
        const groups = _.groupBy(users, x => x.firstPostDate.clone().startOf(periodNoun).format("YYYY-MM-DD"));
        const rows = allPeriods.filter(x => x.noun == periodNoun).map(x => x.date).map(date => ({
            date: date, new_poster_count: groups.hasOwnProperty(date) ? groups[date].length : 0 
        }));

        await writeCsvFile(config, `${periodAdjective}_${filenameSuffix}.csv`, rows,
            ["period", "date", "new_poster_count"],
            [ x => periodNoun, x => x.date, x => x.new_poster_count ]);
    }
}

// --------------------------------------------------------------------------------------------------------------------

async function uploadFiles(config: Config): Promise<void> {
    aws.config.region = "us-east-1";
    const s3 = new aws.S3({ params: { Bucket: "shackstats.com" }});
    const oldFileHashesCsv = await new Promise<any>((resolve, reject) => {
        s3.getObject({ Bucket: "shackstats.com", Key: "data/file_hashes.csv" }, (err, data) => {
            if (err) {
                reject(err);
            } else {
                resolve(data.Body.toString());
            }
        });
    });
    const newFileHashesCsv = (await readFile(path.join(config.dataDir, "file_hashes.csv"))).toString();

    const oldFileHashes = babyparse.parse(oldFileHashesCsv, { header: true }).data;
    const newFileHashes = babyparse.parse(newFileHashesCsv, { header: true }).data;

    const oldFileHashDict = Dictionary.fromArray(oldFileHashes, x => <string>x.filename, x => <string>x.sha256);
    const newFilesToUpload = Dictionary.fromArray(newFileHashes, x => <string>x.filename, x => true);
    newFileHashes.forEach(x => {
        const filename: string = x.filename;
        const newHash: string = x.sha256;
        const oldHash = oldFileHashDict.lookup(filename, "");
        if (oldHash === newHash) {
            newFilesToUpload.remove(filename);
        }
    });
    const filenames = newFilesToUpload.keys();
    for (var i = 0; i < filenames.length; i++) {
        const filename = filenames[i];
        console.log(`Uploading "${filename}"`);
        const contents = await readFile(path.join(config.dataDir, filename));
        await new Promise<void>((resolve, reject) => {
            s3.upload({
                Body: contents,
                Bucket: "shackstats.com",
                Key: `data/${filename}`,
                ACL: "public-read"
            }, {}, (err, data) => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }
}

// --------------------------------------------------------------------------------------------------------------------

go()
    .then(() => {
        console.info("=====");
        console.info("Site build successful.");
        process.exit(0);
    })
    .catch(reason => {
        console.error(reason);
        process.exit(1);
    });
