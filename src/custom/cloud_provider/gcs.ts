import * as utils from "@actions/cache/lib/internal/cacheUtils";
import { CompressionMethod } from "@actions/cache/lib/internal/constants";
import { DownloadOptions } from "@actions/cache/lib/options";
import * as core from "@actions/core";
import { Storage } from "@google-cloud/storage";
import * as crypto from "crypto";
import { createReadStream } from "fs";

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
    core.info(`Inside Download cache function ${bucketName}`);
    core.info(archiveLocation);
    if (!bucketName) {
        throw new Error("Environment variable BUCKET_NAME not set");
    }

    const archiveUrl = new URL(archiveLocation);
    const objectKey = archiveUrl.pathname.slice(1);
    const file = bucket.file(objectKey);
    core.info(objectKey);

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
    { compressionMethod, enableCrossOsArchive, cacheSize: archiveFileSize }
): Promise<void> {
    if (!bucketName) {
        throw new Error("Environment variable BUCKET_NAME not set");
    }

    const gcsPrefix = getGcsPrefix(paths, {
        compressionMethod,
        enableCrossOsArchive
    });
    const gcsKey = `${gcsPrefix}/${key}`;
    const file = bucket.file(gcsKey);

    const cacheSize = utils.getArchiveFileSizeInBytes(archivePath);
    core.info(
        `Cache Size: ~${Math.round(
            cacheSize / (1024 * 1024)
        )} MB (${cacheSize} B)`
    );

    const writeStream = file.createWriteStream();
    const readStream = createReadStream(archivePath);

    readStream.pipe(writeStream);

    writeStream.on("finish", () => {
        core.info(`Cache saved successfully.`);
    });

    writeStream.on("error", error => {
        throw new Error(`Error saving cache to GCS: ${error}`);
    });
}
