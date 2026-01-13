import { CompressionMethod } from "@actions/cache/lib/internal/constants";
import { DownloadOptions } from "@actions/cache/lib/options";
import * as core from "@actions/core";
import { Storage } from "@google-cloud/storage";
import * as crypto from "crypto";
import { createReadStream, statSync, promises as fsPromises } from "fs";

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
    Number(process.env.UPLOAD_PART_SIZE || "64") * 1024 * 1024;
const downloadQueueSize = Number(process.env.DOWNLOAD_QUEUE_SIZE || "8");
const downloadPartSize =
    Number(process.env.DOWNLOAD_PART_SIZE || "32") * 1024 * 1024;

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

interface UploadPart {
    partNumber: number;
    offset: number;
    size: number;
    partKey: string;
}

async function uploadPartWithRetry(
    storage: Storage,
    bucketName: string,
    archivePath: string,
    part: UploadPart,
    retries = 3
): Promise<void> {
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            const fileHandle = await fsPromises.open(archivePath, "r");
            try {
                const buffer = Buffer.alloc(part.size);
                await fileHandle.read(buffer, 0, part.size, part.offset);

                const bucket = storage.bucket(bucketName);
                const file = bucket.file(part.partKey);

                await new Promise<void>((resolve, reject) => {
                    const stream = file.createWriteStream({
                        resumable: true
                    });

                    stream.on("error", reject);
                    stream.on("finish", () => resolve());

                    stream.end(buffer);
                });

                core.debug(
                    `Successfully uploaded part ${part.partNumber} (${(part.size / (1024 * 1024)).toFixed(2)} MB)`
                );
                return;
            } finally {
                await fileHandle.close();
            }
        } catch (error) {
            lastError = error as Error;
            if (attempt < retries) {
                core.warning(
                    `Failed to upload part ${part.partNumber} (attempt ${attempt}/${retries}): ${lastError.message}`
                );
                await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
            }
        }
    }

    throw new Error(
        `Failed to upload part ${part.partNumber} after ${retries} attempts: ${lastError?.message}`
    );
}

async function uploadInParallel(
    storage: Storage,
    bucketName: string,
    archivePath: string,
    gcsKey: string,
    fileSize: number
): Promise<void> {
    const partSize = uploadPartSize;
    const concurrency = uploadQueueSize;

    // For small files, use simple upload
    if (fileSize <= partSize) {
        core.debug("File size is small, using simple upload");
        const bucket = storage.bucket(bucketName);
        const file = bucket.file(gcsKey);
        const readStream = createReadStream(archivePath);

        await new Promise<void>((resolve, reject) => {
            readStream
                .pipe(
                    file.createWriteStream({
                        resumable: true
                    })
                )
                .on("error", reject)
                .on("finish", resolve);
        });
        return;
    }

    core.info(
        `Starting parallel upload with ${concurrency} concurrent parts of ${(partSize / (1024 * 1024)).toFixed(0)} MB each`
    );

    const parts: UploadPart[] = [];
    let partNumber = 0;

    for (let offset = 0; offset < fileSize; offset += partSize) {
        const size = Math.min(partSize, fileSize - offset);
        parts.push({
            partNumber: partNumber++,
            offset,
            size,
            partKey: `${gcsKey}.part${partNumber}`
        });
    }

    core.info(`Uploading ${parts.length} parts in parallel...`);

    // Upload parts with controlled concurrency
    const activeUploads: Promise<void>[] = [];
    let completed = 0;
    const LOG_INTERVAL = 10;

    for (const part of parts) {
        const uploadPromise = uploadPartWithRetry(
            storage,
            bucketName,
            archivePath,
            part
        ).then(() => {
            completed++;
            if (completed % LOG_INTERVAL === 0 || completed === parts.length) {
                const progress = ((completed / parts.length) * 100).toFixed(1);
                core.info(
                    `Upload progress: ${completed}/${parts.length} parts (${progress}%)`
                );
            }
        });

        activeUploads.push(uploadPromise);

        if (activeUploads.length >= concurrency) {
            await Promise.race(activeUploads);
            // Remove completed promises
            const newActiveUploads = activeUploads.filter(p => {
                let completed = false;
                p.then(() => {
                    completed = true;
                }).catch(() => {
                    completed = true;
                });
                return !completed;
            });
            activeUploads.splice(0, activeUploads.length, ...newActiveUploads);
        }
    }

    // Wait for all remaining uploads to complete
    await Promise.all(activeUploads);

    core.info("All parts uploaded, composing final object...");

    // Compose all parts into the final object
    const bucket = storage.bucket(bucketName);
    const partFiles = parts.map(part => bucket.file(part.partKey));
    const allTempFiles: string[] = partFiles.map(f => f.name);

    // GCS compose has a limit of 32 source objects
    // If we have more, we need to compose in batches hierarchically
    const MAX_COMPOSE_COUNT = 32;
    let sourceFiles = partFiles.map(f => f.name);
    let generation = 0;

    while (sourceFiles.length > MAX_COMPOSE_COUNT) {
        core.info(
            `Composing ${sourceFiles.length} parts in batches (generation ${generation})...`
        );

        const composedFiles: string[] = [];
        const batchSize = MAX_COMPOSE_COUNT - 1; // Use 31 to be safe

        for (let i = 0; i < sourceFiles.length; i += batchSize) {
            const batch = sourceFiles.slice(i, i + batchSize);
            const composedKey = `${gcsKey}.composed${generation}_${Math.floor(i / batchSize)}`;
            composedFiles.push(composedKey);
            allTempFiles.push(composedKey);

            const composedFile = bucket.file(composedKey);
            await composedFile.save("", { resumable: false });
            await bucket.combine(batch, composedKey);

            core.debug(
                `Composed batch ${Math.floor(i / batchSize) + 1}: ${batch.length} parts into ${composedKey}`
            );
        }

        sourceFiles = composedFiles;
        generation++;
    }

    core.info(
        `Creating final object from ${sourceFiles.length} intermediate file(s)...`
    );

    // Final compose
    const finalFile = bucket.file(gcsKey);
    await finalFile.save("", { resumable: false });
    await bucket.combine(sourceFiles, gcsKey);

    core.info("Composed final object, cleaning up temporary files...");

    // Delete all temporary files (parts and intermediate composed files)
    await Promise.all(
        allTempFiles.map(fileName =>
            bucket.file(fileName).delete().catch(() => {})
        )
    );

    core.info("Parallel upload completed successfully");
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

    // Construct your GCS key / prefix.
    const gcsPrefix = getGcsPrefix(paths, {
        compressionMethod,
        enableCrossOsArchive
    });
    const gcsKey = `${gcsPrefix}/${key}`;

    // Get the cache size for logging
    const cacheSize = archiveFileSize
        ? archiveFileSize
        : statSync(archivePath).size;
    core.info(
        `Cache Size: ~${Math.round(
            cacheSize / (1024 * 1024)
        )} MB (${cacheSize} B)`
    );

    core.info(
        `Uploading cache from ${archivePath} to gs://${bucketName}/${gcsKey}`
    );

    // Initialize GCS client
    const storage = new Storage();

    // Use parallel upload for better performance
    await uploadInParallel(storage, bucketName, archivePath, gcsKey, cacheSize);

    core.info("Cache saved successfully.");
}
