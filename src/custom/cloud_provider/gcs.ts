import { CompressionMethod } from "@actions/cache/lib/internal/constants";
import { DownloadOptions } from "@actions/cache/lib/options";
import * as core from "@actions/core";
import { Storage } from "@google-cloud/storage";
import * as crypto from "crypto";
import { createReadStream, statSync } from "fs";

import { downloadCacheHttpClientConcurrent } from "../downloadUtils";

interface ArtifactCacheEntry {
    cacheKey?: string;
    scope?: string;
    cacheVersion?: string;
    creationTime?: string;
    archiveLocation?: string;
}

// Set up Google Cloud Storage client
const storage = new Storage();
const bucketName = process.env.BUCKET_NAME || "";
const bucket = storage.bucket(bucketName);

const versionSalt = "1.0";
const uploadQueueSize = Number(process.env.UPLOAD_QUEUE_SIZE || "4");
const uploadPartSize =
    Number(process.env.UPLOAD_PART_SIZE || "32") * 1024 * 1024;
const downloadQueueSize = Number(process.env.DOWNLOAD_QUEUE_SIZE || "8");
const downloadPartSize =
    Number(process.env.DOWNLOAD_PART_SIZE || "16") * 1024 * 1024;

export function getCacheVersion(
    paths: string[],
    compressionMethod?: CompressionMethod,
    enableCrossOsArchive = false
): string {
    const components = paths.slice();

    if (compressionMethod) {
        components.push(compressionMethod);
    }

    if (process.platform === "win32" && !enableCrossOsArchive) {
        components.push("windows-only");
    }

    components.push(versionSalt);

    return crypto
        .createHash("sha256")
        .update(components.join("|"))
        .digest("hex");
}

function getGcsPrefix(
    paths: string[],
    { compressionMethod, enableCrossOsArchive }
) {
    const repository = process.env.GITHUB_REPOSITORY;
    const version = getCacheVersion(
        paths,
        compressionMethod,
        enableCrossOsArchive
    );
    return ["cache", repository, version].join("/");
}

export async function getCacheEntry(
    keys,
    paths,
    { compressionMethod, enableCrossOsArchive }
) {
    const cacheEntry: ArtifactCacheEntry = {};

    for (const restoreKey of keys) {
        const gcsPrefix = getGcsPrefix(paths, {
            compressionMethod,
            enableCrossOsArchive
        });
        const [files] = await bucket.getFiles({
            prefix: `${gcsPrefix}/${restoreKey}`
        });

        if (files.length > 0) {
            const sortedFiles = files.sort((a, b) => {
                return Number(b.metadata.updated) - Number(a.metadata.updated);
            });
            const gcsPath = sortedFiles[0].name;
            cacheEntry.cacheKey = gcsPath.replace(`${gcsPrefix}/`, "");
            cacheEntry.archiveLocation = `gs://${bucketName}/${gcsPath}`;
            return cacheEntry;
        }
    }

    return cacheEntry;
}

export async function downloadCache(
    archiveLocation: string,
    archivePath: string,
    options?: DownloadOptions
): Promise<void> {
    if (!bucketName) {
        throw new Error("Environment variable BUCKET_NAME not set");
    }

    const archiveUrl = new URL(archiveLocation);
    const objectKey = archiveUrl.pathname.slice(1);
    const file = bucket.file(objectKey);

    const [url] = await file.getSignedUrl({
        action: "read",
        expires: Date.now() + 3600 * 1000
    });

    await downloadCacheHttpClientConcurrent(url, archivePath, {
        ...options,
        downloadConcurrency: downloadQueueSize,
        concurrentBlobDownloads: true,
        partSize: downloadPartSize
    });
}

export async function saveCache(
    key: string,
    paths: string[],
    archivePath: string,
    {
        compressionMethod,
        enableCrossOsArchive,
        cacheSize: archiveFileSize
    }: {
        compressionMethod: string;
        enableCrossOsArchive: boolean;
        cacheSize: number;
    }
): Promise<void> {
    if (!bucketName) {
        throw new Error("Environment variable BUCKET_NAME not set");
    }
    const LOG_INTERVAL_BYTES = 50 * 1024 * 1024; // 30 MB

    let nextLogThreshold = LOG_INTERVAL_BYTES;

    // Construct your GCS key / prefix.
    // You can rename this to `getGcsPrefix` if you use a custom helper.
    const gcsPrefix = getGcsPrefix(paths, {
        compressionMethod,
        enableCrossOsArchive
    });
    const gcsKey = `${gcsPrefix}/${key}`;

    // Get the cache size for logging
    const cacheSize = archiveFileSize
        ? archiveFileSize
        : statSync(archivePath).size; // or use your utility function
    core.info(
        `Cache Size: ~${Math.round(
            cacheSize / (1024 * 1024)
        )} MB (${cacheSize} B)`
    );

    core.debug(
        `Uploading cache from ${archivePath} to gs://${bucketName}/${gcsKey}`
    );

    // Initialize GCS client and references
    const storage = new Storage();
    const bucket = storage.bucket(bucketName);
    const file = bucket.file(gcsKey);

    // Create the read stream from our archive file
    const readStream = createReadStream(archivePath);

    // (Optional) Track read progress for logs
    let bytesUploaded = 0;
    readStream.on("data", chunk => {
        bytesUploaded += chunk.length;

        if (bytesUploaded >= nextLogThreshold) {
            const uploadedMB = (bytesUploaded / (1024 * 1024)).toFixed(2);
            const totalMB = (cacheSize / (1024 * 1024)).toFixed(2);

            core.info(`Uploaded ${uploadedMB} MB of ${totalMB} MB ...`);
            nextLogThreshold += LOG_INTERVAL_BYTES;
        }
    });

    // Pipe it to GCS via createWriteStream (resumable by default)
    await new Promise<void>((resolve, reject) => {
        readStream
            .pipe(
                file.createWriteStream({
                    resumable: true,
                    chunkSize: uploadPartSize
                })
            )
            .on("error", err => {
                reject(err);
            })
            .on("finish", () => {
                core.info("Cache saved successfully.");
                resolve();
            });
    });
}
