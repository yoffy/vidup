#include <algorithm>
#include <cmath>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <immintrin.h>

#include <sqlite3.h>

namespace fs = std::filesystem;

typedef std::uint32_t Hash;
typedef std::uint32_t DurationMs;
typedef int           FileId; //!< 未設定は -1

//! シーンを一意に識別するための値
struct SceneId {
    Hash       hash;
    DurationMs durationMs;
};

static inline bool operator<(const SceneId& a, const SceneId& b)
{
    if ( a.hash != b.hash ) {
        return a.hash < b.hash;
    } else {
        return a.durationMs < b.durationMs;
    };
}

struct Scene {
    SceneId sceneId;
    FileId  fileId;
};

//! getTopHashes() の出力
struct HashCount {
    SceneId sceneId;
    int     count;
};

enum FileStatus {
    kNone     = 0,
    kAnalyzed = 1,
};

struct FileEntry {
    FileId   id;
    fs::path name;
    int      status; //!< FileStatus
};

//! gray 16x16px
static const std::size_t kFrameSize             = 16 * 16;
static const int         kFps                   = 12;
static const double      kSceneChangedThreshold = 4.5;

static bool g_isVerbose = false;

// TODO: use CLMUL
static std::uint32_t
crc32acc(std::uint32_t acc, std::size_t size, const std::uint8_t* __restrict buf)
{
    std::uint64_t acc64 = acc;
    std::size_t   i     = 0;
    for ( ; i + 8 <= size; i += sizeof(std::uint64_t) ) {
        acc64 = _mm_crc32_u64(acc, *reinterpret_cast<const std::uint64_t*>(&buf[i]));
    }
    acc = std::uint32_t(acc64);
    for ( ; i < size; i += 1 ) {
        acc = _mm_crc32_u8(acc, buf[i]);
    }
    return acc;
}

//! root mean squared error
static double rmse(const std::uint8_t* __restrict frame1, const std::uint8_t* __restrict frame2)
{
    // 8-bit の自乗なので 16-bit、kFrameSize が 16-bit 以下ならオーバーフローしない
    std::uint32_t rse = 0;
    for ( std::size_t i = 0; i < kFrameSize; i += 1 ) {
        std::int16_t delta = std::int16_t(frame1[i]) - frame2[i];
        rse += delta * delta;
    }

    // divide by kFrameSize*gray
    return std::sqrt(float(rse) / (kFrameSize * 256));
}

static bool readFrame(std::FILE* stream, std::uint8_t* __restrict dest)
{
    if ( std::fread(dest, 1, kFrameSize, stream) != kFrameSize ) {
        return false;
    }

    // dithering
    for ( std::size_t i = 0; i < kFrameSize; i += 1 ) {
        dest[i] = dest[i] & 0xF0;
    }

    return true;
}

static void debugPrintf(const char* __restrict format, ...)
{
    if ( ! g_isVerbose ) {
        return;
    }
    va_list arg;
    va_start(arg, format);
    std::vfprintf(stderr, format, arg);
    va_end(arg);
}

//! foreign keys を有効化する
//!
//! @return 成功なら 0
//!
//! 失敗した場合は標準エラーにメッセージを出力する。
static int enableForeignKeys(sqlite3* db)
{
    sqlite3_stmt* stmt   = nullptr;
    int           status = sqlite3_prepare_v2(db, "PRAGMA foreign_keys = ON", -1, &stmt, nullptr);
    if ( status ) {
        std::fprintf(stderr, "PRAGMA foreign_keys = ON: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    status = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if ( status != SQLITE_DONE ) {
        std::fprintf(stderr, "PRAGMA foreign_keys = ON: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    return 0;
}

//! データベースを作成する
//!
//! @return 成功なら 0
static int createDatabase(const fs::path& path)
{
    sqlite3*      db   = nullptr;
    sqlite3_stmt* stmt = nullptr;
    int           status;

    // open db
    if ( int err = sqlite3_open(path.c_str(), &db); err ) {
        std::fprintf(stderr, "sqlite3_open: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    // pragma
    if ( int err = enableForeignKeys(db); err ) {
        sqlite3_close(db);
        return err;
    }

    // create database files
    status = sqlite3_prepare_v2(
        db,
        "CREATE TABLE IF NOT EXISTS files("
        "id INTEGER PRIMARY KEY,"
        "path TEXT UNIQUE,"
        "status INTEGER"
        ")",
        -1,
        &stmt,
        nullptr
    );
    if ( status ) {
        std::fprintf(stderr, "CREATE TABLE files: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    status = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if ( status != SQLITE_DONE ) {
        std::fprintf(stderr, "CREATE TABLE files: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    // create database scenes
    status = sqlite3_prepare_v2(
        db,
        "CREATE TABLE IF NOT EXISTS scenes("
        "hash INTEGER,"
        "duration_ms INTEGER,"
        "file_id INTEGER,"
        "FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE"
        ")",
        -1,
        &stmt,
        nullptr
    );
    if ( status ) {
        std::fprintf(stderr, "CREATE TABLE scenes: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    status = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if ( status != SQLITE_DONE ) {
        std::fprintf(stderr, "CREATE TABLE scenes: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    // craete index scene_hash
    status = sqlite3_prepare_v2(
        db,
        "CREATE INDEX IF NOT EXISTS scene_hash_duration ON scenes(hash, duration_ms);",
        -1,
        &stmt,
        nullptr
    );
    if ( status ) {
        std::fprintf(stderr, "CREATE INDEX scene_hash_duration: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    status = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if ( status != SQLITE_DONE ) {
        std::fprintf(stderr, "CREATE INDEX scene_hash_duration: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    // create index scene_file_id
    status = sqlite3_prepare_v2(
        db, "CREATE INDEX IF NOT EXISTS scene_file_id ON scenes(file_id)", -1, &stmt, nullptr
    );
    if ( status ) {
        std::fprintf(stderr, "CREATE INDEX scene_file_id: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    status = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if ( status != SQLITE_DONE ) {
        std::fprintf(stderr, "CREATE INDEX scene_file_id: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    sqlite3_close(db);
    return 0;
}

//! name からファイルの情報を取得する
//!
//! @return 成功なら 0、失敗なら sqlite3 のエラーコード
//!
//! 失敗した場合、entry.id には -1 が入る。
static int getFileEntry(sqlite3* db, const fs::path& name, FileEntry& entry)
{
    sqlite3_stmt* stmt = nullptr;
    int           status;

    entry.id = -1;

    status
        = sqlite3_prepare_v2(db, "SELECT id, status FROM files WHERE path = ?", -1, &stmt, nullptr);
    if ( status ) {
        std::fprintf(stderr, "SELECT id from files: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_bind_text(stmt, 1, name.c_str(), -1, nullptr);
    if ( status ) {
        std::fprintf(stderr, "SELECT id from files: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_step(stmt);
    if ( status == SQLITE_DONE ) {
        sqlite3_finalize(stmt);
        return 0;
    } else if ( status != SQLITE_ROW ) {
        std::fprintf(stderr, "SELECT id from files: %d\n", status);
        sqlite3_finalize(stmt);
        return status;
    }

    entry.id     = sqlite3_column_int(stmt, 0);
    entry.name   = name;
    entry.status = sqlite3_column_int(stmt, 1);

    sqlite3_finalize(stmt);
    return 0;
}

//! fileId からファイル名を取得する
//!
//! @return 成功なら 0、失敗なら sqlite3 のエラーコード
static int getFileName(sqlite3* db, FileId fileId, fs::path& name)
{
    sqlite3_stmt* stmt = nullptr;
    int           status;

    status = sqlite3_prepare_v2(db, "SELECT path FROM files WHERE id = ?", -1, &stmt, nullptr);
    if ( status ) {
        std::fprintf(stderr, "SELECT path from files: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_bind_int(stmt, 1, fileId);
    if ( status ) {
        std::fprintf(stderr, "SELECT path from files: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_step(stmt);
    if ( status == SQLITE_DONE ) {
        name.clear();
        sqlite3_finalize(stmt);
        return 0;
    } else if ( status != SQLITE_ROW ) {
        std::fprintf(stderr, "SELECT path from files: %d\n", status);
        name.clear();
        sqlite3_finalize(stmt);
        return status;
    }

    name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));

    sqlite3_finalize(stmt);
    return 0;
}

//! fileId のシーンを取得する
//!
//! @return 成功なら 0、失敗なら sqlite3 のエラーコード
//!
//! scenes はクリアされず追記される。
static int getScenesByFile(sqlite3* db, FileId fileId, std::vector<Scene>& scenes)
{
    sqlite3_stmt* stmt = nullptr;
    int           status;

    status = sqlite3_prepare_v2(
        db, "SELECT hash, duration_ms FROM scenes WHERE file_id = ?", -1, &stmt, nullptr
    );
    if ( status ) {
        std::fprintf(stderr, "getScenesByFile: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_bind_int(stmt, 1, fileId);
    if ( status ) {
        std::fprintf(stderr, "getScenesByFile: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_step(stmt);
    while ( status == SQLITE_ROW ) {
        Hash       hash       = sqlite3_column_int(stmt, 0);
        DurationMs durationMs = sqlite3_column_int(stmt, 1);

        scenes.emplace_back(Scene { SceneId { hash, durationMs }, fileId });
        status = sqlite3_step(stmt);
    }
    sqlite3_finalize(stmt);

    if ( status != SQLITE_DONE ) {
        std::fprintf(stderr, "getScenesByFile: %s\n", sqlite3_errmsg(db));
        return status;
    }

    return 0;
}

//! hash のシーンを取得する
//!
//! @return 成功なら 0、失敗なら sqlite3 のエラーコード
//!
//! scenes はクリアされず追記される。
static int getScenesByHash(sqlite3* db, const SceneId& sceneId, std::vector<Scene>& scenes)
{
    sqlite3_stmt* stmt = nullptr;
    int           status;

    status = sqlite3_prepare_v2(
        db,
        "SELECT DISTINCT file_id FROM scenes WHERE (hash = ? AND duration_ms = ?)",
        -1,
        &stmt,
        nullptr
    );
    if ( status ) {
        std::fprintf(stderr, "getScenesByHash: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_bind_int(stmt, 1, sceneId.hash);
    if ( status ) {
        std::fprintf(stderr, "getScenesByHash: %s\n", sqlite3_errmsg(db));
        return status;
    }
    status = sqlite3_bind_int(stmt, 2, sceneId.durationMs);
    if ( status ) {
        std::fprintf(stderr, "getScenesByHash: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_step(stmt);
    while ( status == SQLITE_ROW ) {
        FileId fileId = sqlite3_column_int(stmt, 0);

        scenes.emplace_back(Scene { sceneId, fileId });
        status = sqlite3_step(stmt);
    }
    sqlite3_finalize(stmt);

    if ( status != SQLITE_DONE ) {
        std::fprintf(stderr, "getScenesByHash: %s\n", sqlite3_errmsg(db));
        return status;
    }

    return 0;
}

//! 重複したハッシュの上位を取得する
//!
//! @return 成功なら 0、失敗なら sqlite3 のエラーコード
//!
//! hashCounts はクリア後にカウントされる。
//! hashCounts は duration の降順でソートされている。
static int getTopHashes(sqlite3* db, int limit, std::vector<HashCount>& hashCounts)
{
    sqlite3_stmt* stmt = nullptr;
    int           status;

    hashCounts.clear();

    status = sqlite3_prepare_v2(
        db,
        "SELECT hash, duration_ms, COUNT(hash)"
        " FROM scenes"
        " GROUP BY hash, duration_ms"
        " HAVING COUNT(hash) > 1"
        " ORDER BY duration_ms DESC"
        " LIMIT ?",
        -1,
        &stmt,
        nullptr
    );
    if ( status ) {
        std::fprintf(stderr, "getTopHashes: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_bind_int(stmt, 1, limit);
    if ( status ) {
        std::fprintf(stderr, "getTopHashes: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_step(stmt);
    while ( status == SQLITE_ROW ) {
        Hash       hash       = sqlite3_column_int(stmt, 0);
        DurationMs durationMs = sqlite3_column_int(stmt, 1);
        int        count      = sqlite3_column_int(stmt, 2);

        hashCounts.emplace_back(HashCount { SceneId { hash, durationMs }, count });
        status = sqlite3_step(stmt);
    }
    sqlite3_finalize(stmt);

    if ( status != SQLITE_DONE ) {
        std::fprintf(stderr, "getTopHashes: %s\n", sqlite3_errmsg(db));
        return status;
    }

    return 0;
}

//! name を DB に登録する
//!
//! @return 成功なら 0、失敗なら sqlite3 のエラーコード
static int registerFile(sqlite3* db, const fs::path& name)
{
    sqlite3_stmt* stmt = nullptr;
    int           status;

    status = sqlite3_prepare_v2(
        db, "INSERT INTO files (path, status) VALUES (?, ?)", -1, &stmt, nullptr
    );
    if ( status ) {
        std::fprintf(stderr, "INSERT INTO files: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_bind_text(stmt, 1, name.c_str(), -1, nullptr);
    if ( status ) {
        std::fprintf(stderr, "INSERT INTO files: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_bind_int(stmt, 2, FileStatus::kNone);
    if ( status ) {
        std::fprintf(stderr, "INSERT INTO files: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if ( status != SQLITE_DONE ) {
        std::fprintf(stderr, "INSERT INTO files: %s\n", sqlite3_errmsg(db));
        return status;
    }

    return 0;
}

//! status を DB に登録する
//!
//! @return 成功なら 0、失敗なら sqlite3 のエラーコード
static int updateFileStatus(sqlite3* db, FileId fileId, FileStatus fileStatus)
{
    sqlite3_stmt* stmt = nullptr;
    int           status;

    status = sqlite3_prepare_v2(db, "UPDATE files SET status = ? WHERE id = ?", -1, &stmt, nullptr);
    if ( status ) {
        std::fprintf(stderr, "updateFileStatus: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_bind_int(stmt, 1, fileStatus);
    if ( status ) {
        std::fprintf(stderr, "updateFileStatus: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_bind_int(stmt, 2, fileId);
    if ( status ) {
        std::fprintf(stderr, "updateFileStatus: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if ( status != SQLITE_DONE ) {
        std::fprintf(stderr, "updateFileStatus: %s\n", sqlite3_errmsg(db));
        return status;
    }

    return 0;
}

//! scene を DB に登録する
//!
//! @return 成功なら 0、失敗なら sqlite3 のエラーコード
static int registerScene(sqlite3* db, const Scene& scene)
{
    sqlite3_stmt* stmt = nullptr;
    int           status;

    status = sqlite3_prepare_v2(
        db, "INSERT INTO scenes (hash, duration_ms, file_id) VALUES (?, ?, ?)", -1, &stmt, nullptr
    );
    if ( status ) {
        std::fprintf(stderr, "INSERT INTO scenes: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_bind_int(stmt, 1, scene.sceneId.hash);
    if ( status ) {
        std::fprintf(stderr, "INSERT INTO scenes: %s\n", sqlite3_errmsg(db));
        return status;
    }
    status = sqlite3_bind_int(stmt, 2, scene.sceneId.durationMs);
    if ( status ) {
        std::fprintf(stderr, "INSERT INTO scenes: %s\n", sqlite3_errmsg(db));
        return status;
    }
    status = sqlite3_bind_int(stmt, 3, scene.fileId);
    if ( status ) {
        std::fprintf(stderr, "INSERT INTO scenes: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if ( status != SQLITE_DONE ) {
        std::fprintf(stderr, "INSERT INTO scenes: %d\n", status);
        return status;
    }

    return 0;
}

//! ファイルを DB から削除する
//!
//! @return 成功なら 0、失敗なら sqlite3 のエラーコード
//!
//! ファイルに紐づくシーンもすべて削除される。
static int deleteFile(sqlite3* db, FileId fileId)
{
    sqlite3_stmt* stmt = nullptr;
    int           status;

    status = sqlite3_prepare_v2(db, "DELETE FROM files WHERE id = ?", -1, &stmt, nullptr);
    if ( status ) {
        std::fprintf(stderr, "DELETE FROM files: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_bind_int(stmt, 1, fileId);
    if ( status ) {
        std::fprintf(stderr, "DELETE FROM files: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if ( status != SQLITE_DONE ) {
        std::fprintf(stderr, "DELETE FROM files: %s\n", sqlite3_errmsg(db));
        return status;
    }

    return 0;
}

//! シーンを解析して DB に登録する
//!
//! @return 成功なら 0
static int analyzeScenes(sqlite3* db, std::FILE* inStream, FileId fileId)
{
    std::uint8_t  frames[kFrameSize * 3] = { 0 };
    std::uint8_t* firstFrame             = &frames[kFrameSize * 0];
    std::uint8_t* lastFrame              = &frames[kFrameSize * 1];
    std::uint8_t* frame                  = &frames[kFrameSize * 2];
    Hash          crc                    = 0;
    std::uint32_t nScenes                = 0;

    std::uint32_t i           = 0;
    std::uint32_t iFirstFrame = 0;

    while ( readFrame(inStream, frame) ) {
        double error = rmse(frame, lastFrame);
        debugPrintf("%8d (%6.1f): %6.1f: %08X", i, double(i) / kFps, error, crc);
        if ( error > kSceneChangedThreshold ) {
            // scene changed
            if ( i > 0 ) {
                debugPrintf(" scene changed\n");
                DurationMs durationMs = (i - iFirstFrame) * 1000 / kFps;
                if ( db && registerScene(db, { { crc, durationMs }, fileId }) ) {
                    return 1;
                }
            } else {
                debugPrintf("\n");
            }
            crc = 0;
            nScenes += 1;
            iFirstFrame = i;
            std::memcpy(firstFrame, frame, kFrameSize);
        } else {
            debugPrintf("\n");
        }

        crc = crc32acc(crc, kFrameSize, frame);
        std::swap(lastFrame, frame);

        i += 1;
    }

    {
        DurationMs durationMs = (i - iFirstFrame) * 1000 / kFps;
        if ( db && registerScene(db, { crc, durationMs, fileId }) ) {
            return 1;
        }
        nScenes += 1;
    }

    if ( db && updateFileStatus(db, fileId, FileStatus::kAnalyzed) ) {
        return 1;
    }

    std::fprintf(stderr, "%d scenes registered.\n", nScenes);

    return 0;
}

//! 各 fileId に含まれるシーンの数をカウントする
//!
//! @return 成功なら 0
//!
//! scenes は fileId でソートされていること。
//!
//! fileAndCounts はクリア後にカウントされる。
static int
countScenes(const std::vector<Scene>& scenes, std::vector<std::pair<FileId, int>>& fileAndCounts)
{
    fileAndCounts.clear();

    auto lower = scenes.begin();
    while ( lower != scenes.end() ) {
        FileId fileId = lower->fileId;

        lower = std::lower_bound(
            lower,
            scenes.end(),
            Scene { 0, 0, fileId },
            [&](const Scene& a, const Scene& b) { return a.fileId < b.fileId; }
        );
        auto upper = std::upper_bound(
            lower,
            scenes.end(),
            Scene { 0, 0, fileId },
            [&](const Scene& a, const Scene& b) { return a.fileId < b.fileId; }
        );

        fileAndCounts.emplace_back(std::pair(fileId, upper - lower));
        lower = upper;
    }

    return 0;
}

//! 類似のファイルを検索する
//!
//! @return 成功なら 0
static int searchFile(sqlite3* db, FileId fileId, int limit)
{
    std::vector<Scene> scenesOfFile;
    std::vector<Scene> foundScenes;

    // fileId のシーンを列挙
    if ( getScenesByFile(db, fileId, scenesOfFile) ) {
        return 1;
    }

    // fileId のシーンと同じハッシュを含むシーンを列挙
    // TODO: SQL で COUNT すればいいのでは
    for ( const auto& scene : scenesOfFile ) {
        if ( getScenesByHash(db, scene.sceneId, foundScenes) ) {
            return 1;
        }
    }

    // fileId でソート
    std::sort(foundScenes.begin(), foundScenes.end(), [&](const Scene& a, const Scene& b) {
        return a.fileId < b.fileId;
    });

    // シーンから fileId を削除
    // fileId でソートされているので lower_bound, upper_bound が使える。
    auto lower = std::lower_bound(
        foundScenes.begin(),
        foundScenes.end(),
        Scene { 0, 0, fileId },
        [&](const Scene& a, const Scene& b) { return a.fileId < b.fileId; }
    );
    auto upper = std::upper_bound(
        foundScenes.begin(),
        foundScenes.end(),
        Scene { 0, 0, fileId },
        [&](const Scene& a, const Scene& b) { return a.fileId < b.fileId; }
    );
    foundScenes.erase(lower, upper);

    std::vector<std::pair<FileId, int>> fileAndCounts;

    countScenes(foundScenes, fileAndCounts);

    // カウントの多い順にソート
    std::sort(fileAndCounts.begin(), fileAndCounts.end(), [&](const auto& a, const auto& b) {
        return a.second > b.second;
    });

    // 上位 limit 件を出力
    if ( fileAndCounts.empty() ) {
        std::fprintf(stderr, "no duplicated videos.\n");
        return 0;
    }

    int i = 0;
    for ( const auto& fileAndCount : fileAndCounts ) {
        fs::path name;

        if ( getFileName(db, fileAndCount.first, name) ) {
            return 1;
        }

        std::fprintf(stderr, "%8d %s\n", fileAndCount.second, name.c_str());

        i += 1;
        if ( i >= limit ) {
            break;
        }
    }

    return 0;
}

//! 類似のシーン上位 limit 件を出力する
//!
//! @return 成功なら 0
static int top(sqlite3* db, int limit)
{
    std::vector<HashCount> hashCounts;

    if ( getTopHashes(db, limit, hashCounts) ) {
        return 1;
    }

    // hashCounts と同じハッシュを含むシーンを列挙
    int                i = 0;
    std::vector<Scene> foundScenes;
    for ( const HashCount& hashCount : hashCounts ) {
        if ( getScenesByHash(db, hashCount.sceneId, foundScenes) ) {
            return 1;
        }

        // 同じシーンを含むファイル名を列挙
        debugPrintf("---- %8.1f seconds matched\n", hashCount.sceneId.durationMs / 1000.0);

        i += 1;
    }

    // ファイルを列挙
    std::set<FileId>           fileIds;
    std::map<FileId, fs::path> fileNames;

    for ( const Scene& scene : foundScenes ) {
        fileIds.insert(scene.fileId);
    }
    for ( FileId fileId : fileIds ) {
        fs::path name;
        if ( getFileName(db, fileId, name) ) {
            return 1;
        }
        fileNames[fileId] = std::move(name);
    }

    // foundScenes をファイルとハッシュをキーに分類
    // TODO: unorderd_multimap でいいのでは
    std::map<FileId, std::vector<const Scene*>>  fileScenes;
    std::map<SceneId, std::vector<const Scene*>> hashScenes;

    for ( const Scene& scene : foundScenes ) {
        fileScenes[scene.fileId].push_back(&scene);
        hashScenes[scene.sceneId].push_back(&scene);
    }

    // 注目しているファイルと共有している時間を積算
    //
    // A → B と B → A は同じになるため、一度積算したら fileIds から削除する。
    std::map<FileId, std::map<FileId, DurationMs>> relationMap;

    while ( ! fileIds.empty() ) {
        FileId fileId   = fileIds.extract(fileIds.begin()).value();
        auto&  relation = relationMap[fileId];

        for ( const Scene* fileScene : fileScenes[fileId] ) {
            for ( const Scene* hashScene : hashScenes[fileScene->sceneId] ) {
                if ( fileIds.count(hashScene->fileId) == 0 ) {
                    // B → A はやらない
                    continue;
                }
                relation[hashScene->fileId] += fileScene->sceneId.durationMs;
            }
        }
    }

    // 積算結果を vector に詰替え
    std::vector<std::tuple<FileId, FileId, DurationMs>> relations;

    // for ( const std::pair<FileId, std::map<FileId, DurationMs>>& relation : relationMap ) {
    for ( const auto& relation : relationMap ) {
        for ( const auto& fileDuration : relation.second ) {
            relations.emplace_back(relation.first, fileDuration.first, fileDuration.second);
        }
    }

    // duration でソート
    std::sort(relations.begin(), relations.end(), [&](const auto& a, const auto& b) {
        return std::get<2>(a) > std::get<2>(b);
    });

    // 結果を出力
    for ( const auto& relation : relations ) {
        std::fprintf(
            stderr,
            "---- %8.1f seconds matched\n%s\n%s\n",
            std::get<2>(relation) / 1000.0f,
            fileNames[std::get<0>(relation)].c_str(),
            fileNames[std::get<1>(relation)].c_str()
        );
    }

    return 0;
}

static void usage()
{
    std::puts("usage: vidup --init");
    std::puts("       vidup [--dry-run --force -v] file");
    std::puts("       vidup [--dry-run --force -v] --stdin filename");
    std::puts("       vidup --delete filename");
    std::puts("       vidup --search filename");
    std::puts("       vidup --top");
}

enum CommandMode { kAnalyze, kDelete, kSearch, kTop };

int main(int argc, char* argv[])
{
    fs::path    me       = argv[0];
    fs::path    basedir  = me.parent_path();
    fs::path    dbPath   = basedir / "database";
    std::FILE*  inStream = nullptr;
    CommandMode mode     = kAnalyze;
    bool        isDryRun = false;
    bool        isForced = false;

    int iArg     = 1;
    int exitCode = 0;

    // parse options
    while ( iArg < argc && argv[iArg][0] == '-' ) {
        if ( iArg >= argc ) {
            usage();
            return 1;
        }
        std::string arg = argv[iArg];
        if ( arg == "--init" ) {
            return createDatabase(dbPath);
        } else if ( arg == "--dry-run" ) {
            isDryRun = true;
        } else if ( arg == "--force" ) {
            isForced = true;
        } else if ( arg == "-v" ) {
            g_isVerbose = true;
        } else if ( arg == "--stdin" ) {
            inStream = stdin;
        } else if ( arg == "--delete" ) {
            mode = CommandMode::kDelete;
        } else if ( arg == "--search" ) {
            mode = CommandMode::kSearch;
        } else if ( arg == "--top" ) {
            mode = CommandMode::kTop;
        } else {
            std::fprintf(stderr, "unknown: %s\n", arg.c_str());
            usage();
            return 1;
        }

        iArg += 1;
    }

    // open db
    sqlite3*  db = nullptr;
    FileEntry fileEntry {};
    fs::path  inPath;
    fs::path  inName;

    if ( int err = sqlite3_open(dbPath.c_str(), &db); err ) {
        std::fprintf(stderr, "sqlite3_open: %s\n", sqlite3_errmsg(db));
        exitCode = 1;
        goto Lfinalize;
    }

    if ( int err = enableForeignKeys(db); err ) {
        exitCode = 1;
        goto Lfinalize;
    }

    if ( mode == CommandMode::kTop ) {
        if ( top(db, 10) ) {
            exitCode = 1;
        }

        sqlite3_close(db);
        return exitCode;
    }

    // inPath
    if ( iArg >= argc ) {
        usage();
        return 1;
    }

    inPath = argv[iArg];
    inName = inPath.stem();

    // exists file in db?
    if ( getFileEntry(db, inName, fileEntry) ) {
        exitCode = 1;
        goto Lfinalize;
    }

    if ( mode == CommandMode::kAnalyze ) {
        // open inStream
        if ( ! inStream ) {
            const char* inputPath = argv[iArg];
            iArg += 1;
            inStream = std::fopen(inputPath, "r");
        }
        if ( ! inStream ) {
            std::perror("fopen for read");
            return 1;
        }

        if ( fileEntry.id >= 0 ) {
            if ( fileEntry.status == FileStatus::kAnalyzed ) {
                if ( ! isForced ) {
                    std::fprintf(stderr, "\"%s\" already exists.\n", inName.c_str());
                    exitCode = 0;
                    goto Lfinalize;
                }
            }

            // どんな状態であろうとエントリが存在するなら消す
            if ( ! isDryRun && deleteFile(db, fileEntry.id) ) {
                exitCode = 1;
                goto Lfinalize;
            }
        }

        if ( ! isDryRun ) {
            if ( registerFile(db, inName) ) {
                exitCode = 1;
                goto Lfinalize;
            }
            if ( getFileEntry(db, inName, fileEntry) ) {
                exitCode = 1;
                goto Lfinalize;
            }
        }

        std::fprintf(stderr, "analyzing \"%s\"\n", inName.c_str());
        if ( analyzeScenes(isDryRun ? nullptr : db, inStream, fileEntry.id) ) {
            exitCode = 1;
            goto Lfinalize;
        }
    } else if ( mode == CommandMode::kDelete ) {
        if ( fileEntry.id < 0 ) {
            std::fprintf(stderr, "\"%s\" not found.\n", inName.c_str());
            exitCode = 1;
            goto Lfinalize;
        }

        if ( deleteFile(db, fileEntry.id) ) {
            exitCode = 1;
            goto Lfinalize;
        }
    } else if ( mode == CommandMode::kSearch ) {
        if ( fileEntry.id < 0 ) {
            std::fprintf(stderr, "\"%s\" not found.\n", inName.c_str());
            exitCode = 1;
            goto Lfinalize;
        }

        if ( searchFile(db, fileEntry.id, 10) ) {
            exitCode = 1;
            goto Lfinalize;
        }
    }

Lfinalize:
    sqlite3_close(db);
    if ( inStream ) {
        std::fclose(inStream);
        inStream = nullptr;
    }

    return exitCode;
}
