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
import * as aws from "aws-sdk";
import * as fs from "fs";
import * as glob from "glob";
import * as path from "path";

function getEnv(key: string): string {
    if (process.env.hasOwnProperty(key)) {
        return process.env[key];
    } else {
        console.error(`The environment variable "${key}" must be provided.`);
        process.exit(1);
    }
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

async function go(): Promise<void> {
    getEnv("AWS_ACCESS_KEY_ID");
    getEnv("AWS_SECRET_ACCESS_KEY");
    const webDir = getEnv("SHACKSTATS_WEB_DIR");

    aws.config.region = "us-east-1";
    const s3 = new aws.S3({ params: { Bucket: "shackstats.com" }});

    async function uploadWebFile(relativePath: string): Promise<void> {
        console.log(`Uploading "${relativePath}"`);
        const contents = await readFile(path.join(webDir, relativePath));
        await new Promise<void>((resolve, reject) => {
            s3.upload({
                Body: contents,
                Bucket: "shackstats.com",
                Key: relativePath,
                ACL: "public-read",
                ContentType:
                    relativePath.endsWith(".html") ? "text/html" :
                    relativePath.endsWith(".csv") ? "text/plain" :
                    relativePath.endsWith(".css") ? "text/css" :
                    relativePath.endsWith(".js") ? "application/javascript" :
                    relativePath.endsWith(".eot") ? "application/eot" :
                    relativePath.endsWith(".ico") ? "image/x-icon" :
                    relativePath.endsWith(".ttf") ? "application/opentype" :
                    relativePath.endsWith(".otf") ? "application/otf" :
                    relativePath.endsWith(".svg") ? "image/svg+xml" :
                    relativePath.endsWith(".woff") ? "application/woff" :
                    relativePath.endsWith(".woff2") ? "application/woff" :
                    "application/octet-stream",
                ContentDisposition: "inline"
            }, {}, (err, data) => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }

    await uploadWebFile("ext/font-awesome/css/font-awesome.min.css");
    await uploadWebFile("ext/font-awesome/fonts/FontAwesome.otf");
    await uploadWebFile("ext/font-awesome/fonts/fontawesome-webfont.eot");
    await uploadWebFile("ext/font-awesome/fonts/fontawesome-webfont.svg");
    await uploadWebFile("ext/font-awesome/fonts/fontawesome-webfont.ttf");
    await uploadWebFile("ext/font-awesome/fonts/fontawesome-webfont.woff");
    await uploadWebFile("ext/font-awesome/fonts/fontawesome-webfont.woff2");
    await uploadWebFile("ext/bluebird.min.js");
    await uploadWebFile("ext/Chart.min.js");
    await uploadWebFile("ext/jquery.dataTables.css");
    await uploadWebFile("ext/jquery.dataTables.js");
    await uploadWebFile("ext/jquery.min.js");
    await uploadWebFile("ext/lodash.min.js");
    await uploadWebFile("ext/moment.min.js");
    await uploadWebFile("ext/papaparse.min.js");
    await uploadWebFile("favicon.ico");
    await uploadWebFile("robots.txt");
    await uploadWebFile("shackstats.css");
    await uploadWebFile("shackstats.js");
    await uploadWebFile("error.html");
    await uploadWebFile("index.html");
}

go()
    .then(() => {
        console.info("=====");
        console.info("Site upload successful.");
        process.exit(0);
    })
    .catch(reason => {
        console.error(reason);
        process.exit(1);
    });
