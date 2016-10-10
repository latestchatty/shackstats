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
import * as _ from 'lodash';
import * as crypto from "crypto";
import * as fs from "fs";
import * as glob from "glob";
import * as moment from "moment-timezone";
import * as path from "path";
import * as pg from "pg";
import * as process from "process";
import { Dictionary } from "./Dictionary";

// set to 0 in production, increase to make things faster in development
const MIN_POST_ID: number = 35510000;

// =====

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
    await buildDataFiles(config);
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
                console.info(`Wrote file: ${filePath}`);
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

// =====

interface DailyPostCount {
    day: string; // YYYY-MM-DD
    count: number;
}

async function buildDataFiles(config: Config): Promise<void> {
    const firstDay = moment((await pgQuerySingle(config.pg,
        "SELECT TO_CHAR(MIN(date), 'YYYY-MM-DD') AS day FROM post WHERE date > '1995-01-01'")).day, "YYYY-MM-DD", true);
    const lastDay = moment((await pgQuerySingle(config.pg,
        "SELECT TO_CHAR(MAX(date), 'YYYY-MM-DD') AS day FROM post")).day, "YYYY-MM-DD", true).add(1, "day");
    
    await buildUsersFile(config);
    await buildPeriodicPostCountsFile(config);
    await buildFilesFile(config);
}

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

// users.csv
async function buildUsersFile(config: Config): Promise<void> {
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
        return prefix;
    }

    const rs = await pgQuery(config.pg,
        `SELECT MIN(author) AS username, MIN(id) AS first_post_id,
            TO_CHAR(MIN(date), 'YYYY-MM-DD HH24:MI:SS') AS first_post_date, COUNT(*) AS post_count
        FROM post
        WHERE id > $1
        GROUP BY author_c
        ORDER BY MIN(id), author_c`,
        [MIN_POST_ID]);
    const seen = new Dictionary<string, string>(); // id -> username
    rs.forEach(x => {
        const prefix = getPrefix(x.username);
        var suffix = 2;
        var userId = prefix; // try without suffix first
        while (seen.containsKey(userId)) {
            userId = prefix + suffix;
            suffix++;
        }
        seen.add(userId, x.username);
        x.user_id = userId;
    });
    await writeCsvFile(config, "users.csv", rs, ["user_id", "username", "first_post_id", "first_post_date", "post_count"], [
        x => x.user_id,
        x => x.username,
        x => parseInt(x.first_post_id),
        x => postDateToUtc(x.first_post_date),
        x => parseInt(x.post_count)
    ]);
}

// (daily|weekly|monthly|yearly)_post_counts.csv
async function buildPeriodicPostCountsFile(config: Config): Promise<void> {
    const periods = [["day", "daily"], ["week", "weekly"], ["month", "monthly"], ["year", "yearly"]];
    for (var i = 0; i < periods.length; i++) { 
        const periodNoun = periods[i][0];
        const periodAdjective = periods[i][1];
        const rs = await pgQuery(config.pg,
            `SELECT
                TO_CHAR(DATE_TRUNC('${periodNoun}', p.date), 'YYYY-MM-DD HH24:MI:SS') AS date,
                p.category, COUNT(*) AS post_count
            FROM post p
            WHERE p.id > $1
            GROUP BY DATE_TRUNC('${periodNoun}', p.date), p.category
            ORDER BY DATE_TRUNC('${periodNoun}', p.date), p.category`,
            [MIN_POST_ID]);

        const rows: any[] = [];
        var currentRow: any = { date: "" };
        rs.forEach(x => {
            if (x.date !== currentRow.date) {
                if (currentRow.date != "") {
                    rows.push(currentRow);
                }
                currentRow = {
                    date: x.date,
                    total_post_count: 0,
                    ontopic_post_count: 0,
                    nws_post_count: 0,
                    stupid_post_count: 0,
                    political_post_count: 0,
                    tangent_post_count: 0,
                    informative_post_count: 0,
                };
            }
            switch (parseInt(x.category)) {
                case 1:
                    currentRow.total_post_count += (currentRow.ontopic_post_count = x.post_count);
                    break;
                case 2:
                    currentRow.total_post_count += (currentRow.nws_post_count = x.post_count);
                    break;
                case 3:
                    currentRow.total_post_count += (currentRow.stupid_post_count = x.post_count);
                    break;
                case 4:
                    currentRow.total_post_count += (currentRow.political_post_count = x.post_count);
                    break;
                case 5:
                    currentRow.total_post_count += (currentRow.tangent_post_count = x.post_count);
                    break;
                case 6:
                    currentRow.total_post_count += (currentRow.informative_post_count = x.post_count);
                    break;
                default:
                    throw new Error(`Unrecognized category: ${x.category}`);
            }
        });
        if (currentRow.date != "") {
            rows.push(currentRow);
        }

        await writeCsvFile(config, `${periodAdjective}_post_counts.csv`, rows,
            ["period", "date", "total_post_count", "ontopic_post_count", "nws_post_count", "stupid_post_count",
                "political_post_count", "tangent_post_count", "informative_post_count"],
            [
                x => periodNoun,
                x => postDateToUtc(x.date),
                x => parseInt(x.total_post_count),
                x => parseInt(x.ontopic_post_count),
                x => parseInt(x.nws_post_count),
                x => parseInt(x.stupid_post_count),
                x => parseInt(x.political_post_count),
                x => parseInt(x.tangent_post_count),
                x => parseInt(x.informative_post_count), 
            ]);
    }
}

// =====

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
