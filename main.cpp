#include <cmath>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>

#include <immintrin.h>

#include <sqlite3.h>

namespace fs = std::filesystem;

//! gray 16x16px
static const std::size_t kFrameSize = 16*16;
static const int kFps = 12;
static const double kSceneChangedThreshold = 4.5;

static bool g_isVerbose = false;

// TODO: use CLMUL
static std::uint32_t crc32acc(std::uint32_t acc, std::size_t size, const std::uint8_t* __restrict buf)
{
    std::uint64_t acc64 = acc;
    std::size_t i = 0;
    for (; i + 8 <= size; i += sizeof(std::uint64_t)) {
        acc64 = _mm_crc32_u64(acc, *reinterpret_cast<const std::uint64_t*>(&buf[i]));
    }
    acc = std::uint32_t(acc64);
    for (; i < size; i += 1) {
        acc = _mm_crc32_u8(acc, buf[i]);
    }
    return acc;
}

static std::uint32_t crc32(std::size_t size, const std::uint8_t* __restrict buf)
{
    return crc32acc(0, size, buf);
}

//! root mean squared error
static double rmse(const std::uint8_t* __restrict frame1, const std::uint8_t* __restrict frame2)
{
    double rse = 0;
    for ( int i = 0; i < 16*16; i += 1 ) {
        double delta = double(frame1[i]) - frame2[i];
        rse += delta * delta;
    }

    // divide by w*h*gray
    return std::sqrt(rse / (16*16*256));
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
    sqlite3_stmt* stmt = nullptr;
    int status = sqlite3_prepare_v2(
        db,
        "PRAGMA foreign_keys = ON",
        -1,
        &stmt,
        nullptr
    );
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
    sqlite3* db = nullptr;
    sqlite3_stmt* stmt = nullptr;
    int status;

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
            "path TEXT UNIQUE"
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
        "CREATE INDEX IF NOT EXISTS scene_hash ON scenes(hash)",
        -1,
        &stmt,
        nullptr
    );
    if ( status ) {
        std::fprintf(stderr, "CREATE INDEX scene_hash: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    status = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if ( status != SQLITE_DONE ) {
        std::fprintf(stderr, "CREATE INDEX scene_hash: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }

    // create index scene_file_id
    status = sqlite3_prepare_v2(
        db,
        "CREATE INDEX IF NOT EXISTS scene_file_id ON scenes(file_id)",
        -1,
        &stmt,
        nullptr
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

//! @param 成功なら 0、失敗なら sqlite3 のエラーコード
static int getFileId(sqlite3* db, const fs::path& name, int& id)
{
    sqlite3_stmt* stmt = nullptr;
    int status;

    status = sqlite3_prepare_v2(
        db,
        "SELECT id FROM files WHERE path = ?",
        -1,
        &stmt,
        nullptr
    );
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
        id = -1;
        sqlite3_finalize(stmt);
        return 0;
    } else if ( status != SQLITE_ROW ) {
        std::fprintf(stderr, "SELECT id from files: %d\n", status);
        id = -1;
        sqlite3_finalize(stmt);
        return status;
    }

    id = sqlite3_column_int(stmt, 0);

    sqlite3_finalize(stmt);
    return 0;
}

//! name を DB に登録する
//!
//! @return 成功なら 0、失敗なら sqlite3 のエラーコード
static int registerFile(sqlite3* db, const fs::path& name)
{
    sqlite3_stmt* stmt = nullptr;
    int status;

    status = sqlite3_prepare_v2(
        db,
        "INSERT INTO files (path) VALUES (?)",
        -1,
        &stmt,
        nullptr
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

    status = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if ( status != SQLITE_DONE ) {
        std::fprintf(stderr, "INSERT INTO files: %s\n", sqlite3_errmsg(db));
        return status;
    }

    return 0;
}

//! scene を DB に登録する
//!
//! @return 成功なら 0、失敗なら sqlite3 のエラーコード
static int registerScene(
    sqlite3* db,
    std::uint32_t hash,
    std::uint32_t durationMs,
    int fileId
) {
    sqlite3_stmt* stmt = nullptr;
    int status;

    status = sqlite3_prepare_v2(
        db,
        "INSERT INTO scenes (hash, duration_ms, file_id) VALUES (?, ?, ?)",
        -1,
        &stmt,
        nullptr
    );
    if ( status ) {
        std::fprintf(stderr, "INSERT INTO scenes: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_bind_int(stmt, 1, hash);
    if ( status ) {
        std::fprintf(stderr, "INSERT INTO scenes: %s\n", sqlite3_errmsg(db));
        return status;
    }
    status = sqlite3_bind_int(stmt, 2, durationMs);
    if ( status ) {
        std::fprintf(stderr, "INSERT INTO scenes: %s\n", sqlite3_errmsg(db));
        return status;
    }
    status = sqlite3_bind_int(stmt, 3, fileId);
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
static int deleteFile(sqlite3* db, const fs::path& name)
{
    sqlite3_stmt* stmt = nullptr;
    int status;

    status = sqlite3_prepare_v2(
        db,
        "DELETE FROM files WHERE path = ?",
        -1,
        &stmt,
        nullptr
    );
    if ( status ) {
        std::fprintf(stderr, "DELETE FROM files: %s\n", sqlite3_errmsg(db));
        return status;
    }

    status = sqlite3_bind_text(stmt, 1, name.c_str(), -1, nullptr);
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
static int analyzeScenes(sqlite3* db, std::FILE* inStream, int fileId)
{
    std::uint8_t frames[kFrameSize * 3] = {0};
    std::uint8_t* firstFrame = &frames[kFrameSize * 0];
    std::uint8_t* lastFrame = &frames[kFrameSize * 1];
    std::uint8_t* frame = &frames[kFrameSize * 2];
    std::uint32_t crc = 0;
    std::uint32_t nScenes = 0;

    std::uint32_t i = 0;
    std::uint32_t iFirstFrame = 0;

    while ( readFrame(inStream, frame) ) {
        double error = rmse(frame, lastFrame);
        debugPrintf("%8d (%6.1f): %6.1f: %08X", i, double(i)/kFps, error, crc);
        if ( error > 4.5 ) {
            // scene changed
            if ( i > 0 ) {
                debugPrintf(" scene changed\n");
                std::uint32_t durationMs = (i - iFirstFrame) * 1000 / kFps;
                if ( registerScene(db, crc, durationMs, fileId) ) {
                    return 1;
                }
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
        std::uint32_t durationMs = (i - iFirstFrame) * 1000 / kFps;
        if ( registerScene(db, crc, durationMs, fileId) ) {
            return 1;
        }
        nScenes += 1;
    }

    std::fprintf(stdout, "%d scenes registered.\n", nScenes);

    return 0;
}

static void usage()
{
    std::puts("usage: vidup --init");
    std::puts("usage: vidup [--force -v] file");
    std::puts("usage: vidup [--force -v] --stdin filename");
    std::puts("usage: vidup --delete filename");
}

enum CommandMode
{
    kAnalyze,
    kDelete
};

int main(int argc, char* argv[])
{
    fs::path me = argv[0];
    fs::path basedir = me.parent_path();
    fs::path dbPath = basedir / "database";
    std::FILE* inStream = nullptr;
    CommandMode mode = kAnalyze;
    bool isForced = false;

    int iArg = 1;
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
        } else if ( arg == "--force" ) {
            isForced = true;
        } else if ( arg == "-v" ) {
            g_isVerbose = true;
        } else if ( arg == "--stdin" ) {
            inStream = stdin;
        } else if ( arg == "--delete" ) {
            mode = kDelete;
        }

        iArg += 1;
    }

    // inPath
    if ( iArg >= argc ) {
        usage();
        return 1;
    }

    fs::path inPath = argv[iArg];
    fs::path inName = inPath.stem();

    // open db
    sqlite3* db = nullptr;
    int fileId = 0;

    if ( int err = sqlite3_open(dbPath.c_str(), &db); err ) {
        std::fprintf(stderr, "sqlite3_open: %s\n", sqlite3_errmsg(db));
        exitCode = 1;
        goto Lfinalize;
    }

    // exists file in db?
    if ( getFileId(db, inName, fileId) ) {
        exitCode = 1;
        goto Lfinalize;
    }

    if ( mode == kAnalyze ) {
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

        if ( fileId >= 0 && ! isForced ) {
            std::fprintf(stderr, "\"%s\" already exists.\n", inName.c_str());
            exitCode = 1;
            goto Lfinalize;
        }

        if ( fileId < 0 && registerFile(db, inName) ) {
            exitCode = 1;
            goto Lfinalize;
        }
        if ( getFileId(db, inName, fileId) ) {
            exitCode = 1;
            goto Lfinalize;
        }

        std::fprintf(stdout, "analyzing \"%s\"\n", inName.c_str());
        if ( analyzeScenes(db, inStream, fileId) ) {
            exitCode = 1;
            goto Lfinalize;
        }
    } else if ( mode == kDelete ) {
        if ( fileId < 0 ) {
            std::fprintf(stderr, "\"%s\" not found.\n", inName.c_str());
            exitCode = 1;
            goto Lfinalize;
        }

        if ( deleteFile(db, inName) ) {
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