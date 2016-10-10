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
import * as fs from "fs";
import * as moment from "moment";
import * as path from "path";
import * as pg from "pg";
import * as process from "process";
import { Dictionary } from "./Dictionary";

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

async function writeCsvFile<T>(config: Config, filename: string, rows: T[], cols: string[],
selectors: ((x: T) => any)[]) {
    const filePath = path.join(config.dataDir, filename);
    const lines = rows.map(row => 
        selectors.map(selector =>
            `"${selector(row).toString().replace('"', '""')}"`).join());
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
        "SELECT TO_CHAR(MAX(date), 'YYYY-MM-DD') AS day FROM post")).day, "YYYY-MM-DD", true);
    
    await buildDailyPostCountFile(config, firstDay, lastDay);
}

// DailyPostCountFile - daily-post-counts.json
async function buildDailyPostCountFile(config: Config, firstDay: moment.Moment, lastDay: moment.Moment): Promise<void> {
    const postCountsByDayRows = await pgQuery(config.pg,
        `SELECT COUNT(*) AS count, TO_CHAR(DATE_TRUNC('day', date), 'YYYY-MM-DD') AS day
        FROM post
        GROUP BY DATE_TRUNC('day', date)
        ORDER BY DATE_TRUNC('day', date)`);
    const postCountsByDayDict = new Dictionary<string, number>();
    postCountsByDayRows.forEach((x, i) => {
        postCountsByDayDict.add(x.day, x.count);
    });
    const postCountsByDay: DailyPostCount[] = [];
    for (var day = firstDay; day <= lastDay; day = day.add(1, "day")) {
        const dayString = day.format("YYYY-MM-DD");
        const dayCount = postCountsByDayDict.lookup(dayString, 0);
        const point: DailyPostCount = { day: dayString, count: dayCount };
        postCountsByDay.push(point);
    }
    await writeCsvFile(config, "daily-post-counts.csv", postCountsByDay,
        ["day", "count"],
        [x => x.day, x => x.count]);
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
