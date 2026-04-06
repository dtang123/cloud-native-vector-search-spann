#include "inc/Helper/DiskIO.h"
#include "inc/Helper/AsyncFileReader.h"

#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <mutex>
#include <aws/core/utils/threading/DefaultExecutor.h>

#include <atomic>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <iomanip>
#include <sstream>

namespace SPTAG {
namespace Helper {

class S3FileIO : public DiskIO
{
public:
    static constexpr std::uint64_t PREFETCH_SIZE = 64ULL * 1024 * 1024; // 64MB

    explicit S3FileIO(DiskIOScenario scenario = DiskIOScenario::DIS_UserRead)
        : m_fileSize(0), m_readPos(0), m_available(false), m_prefetchDone(false)
    {}

    ~S3FileIO() override { ShutDown(); }

    bool Initialize(const char* filePath,
                    int         /*openMode*/,
                    std::uint64_t /*maxIOSize*/    = (1 << 20),
                    std::uint32_t /*maxReadRetries*/ = 2,
                    std::uint32_t /*maxWriteRetries*/ = 2,
                    std::uint16_t /*threadPoolSize*/ = 4,
                    std::uint64_t /*maxFileSize*/   = (300ULL << 30)) override
    {
        std::cerr << "[S3FileIO] Initialize called with path: " 
                  << (filePath ? filePath : "null") << "\n";
        std::cerr.flush();

        const char* bucket = std::getenv("SPTAG_S3_BUCKET");
        const char* key    = std::getenv("SPTAG_S3_KEY");
        const char* region = std::getenv("AWS_DEFAULT_REGION");

        std::cerr << "[S3FileIO] Bucket=" << (bucket ? bucket : "NOT SET")
                  << " Key=" << (key ? key : "NOT SET") << "\n";
        std::cerr.flush();

        if (!bucket || !key) {
            std::cerr << "[S3FileIO] SPTAG_S3_BUCKET and SPTAG_S3_KEY must be set.\n";
            return false;
        }
        m_bucket = bucket;
        m_key    = key;

        Aws::Client::ClientConfiguration cfg;
        if (region) cfg.region = region;
        cfg.maxConnections   = 512;
        cfg.requestTimeoutMs = 30000;
        cfg.connectTimeoutMs = 5000;

        // m_client = std::make_shared<Aws::S3::S3Client>(cfg);
    
    	m_client = std::make_shared<Aws::S3::S3Client>(cfg);
        // HEAD to get file size
        {
            Aws::S3::Model::HeadObjectRequest req;
            req.SetBucket(m_bucket);
            req.SetKey(m_key);
            auto outcome = m_client->HeadObject(req);
            if (!outcome.IsSuccess()) {
                std::cerr << "[S3FileIO] Cannot HEAD s3://" << m_bucket
                          << "/" << m_key << " : "
                          << outcome.GetError().GetMessage() << "\n";
                return false;
            }
            m_fileSize = static_cast<std::uint64_t>(outcome.GetResultWithOwnership().GetContentLength());
        }

        // Prefetch first 64MB in one GET to serve LoadingHeadInfo fast
        std::cerr << "[S3FileIO] Prefetching first " << (PREFETCH_SIZE / 1024 / 1024) 
                  << "MB from S3...\n";
        std::cerr.flush();

        std::uint64_t prefetchEnd = std::min(PREFETCH_SIZE, m_fileSize) - 1;
        std::string range = MakeRange(0, prefetchEnd);

        Aws::S3::Model::GetObjectRequest prefetchReq;
        prefetchReq.SetBucket(m_bucket);
        prefetchReq.SetKey(m_key);
        prefetchReq.SetRange(range);

        auto outcome = m_client->GetObject(prefetchReq);
        if (!outcome.IsSuccess()) {
            std::cerr << "[S3FileIO] Prefetch failed: " 
                      << outcome.GetError().GetMessage() << "\n";
            // Don't fail — fall back to per-read S3 GETs
        } else {
            m_prefetchBuf.resize(prefetchEnd + 1);
	    auto result = outcome.GetResultWithOwnership();
            auto& body = result.GetBody();
            body.read(m_prefetchBuf.data(), static_cast<std::streamsize>(prefetchEnd + 1));
            m_prefetchSize = static_cast<std::uint64_t>(body.gcount());
            m_prefetchDone = true;
            std::cerr << "[S3FileIO] Prefetch complete, got " 
                      << m_prefetchSize << " bytes\n";
            std::cerr.flush();
        }

        std::cerr << "[S3FileIO] Opened s3://" << m_bucket << "/" << m_key
                  << "  size=" << m_fileSize << " bytes\n";
        std::cerr.flush();

        m_readPos   = 0;
        m_available = true;
        return true;
    }

    bool UsesLinuxAIO() const override { return false; }

    bool Available() override { return m_available.load(); }

    std::uint64_t ReadBinary(std::uint64_t readSize,
                             char*         buffer,
                             std::uint64_t offset = UINT64_MAX) override
    {
        if (!m_available) return 0;

        std::uint64_t pos = (offset == UINT64_MAX) ? m_readPos.load() : offset;
        if (pos >= m_fileSize) return 0;
        readSize = std::min(readSize, m_fileSize - pos);

        // Serve from prefetch buffer if possible
        if (m_prefetchDone && pos + readSize <= m_prefetchSize) {
            std::memcpy(buffer, m_prefetchBuf.data() + pos, readSize);
            m_readPos.store(pos + readSize);
            return readSize;
        }

        // Fall back to S3 GET for reads beyond prefetch buffer
        std::string range = MakeRange(pos, pos + readSize - 1);

        Aws::S3::Model::GetObjectRequest req;
        req.SetBucket(m_bucket);
        req.SetKey(m_key);
        req.SetRange(range);

        auto outcome = m_client->GetObject(req);
        if (!outcome.IsSuccess()) {
            std::cerr << "[S3FileIO] ReadBinary failed range=" << range
                      << " : " << outcome.GetError().GetMessage() << "\n";
            return 0;
        }
        auto result = outcome.GetResultWithOwnership();
	auto& body = result.GetBody();
        body.read(buffer, static_cast<std::streamsize>(readSize));
        std::uint64_t got = static_cast<std::uint64_t>(body.gcount());
        if (got < readSize) {
    	    std::cerr << "[CRITICAL] S3 short read: expected " << readSize << " but got " << got << "\n";
	}
	m_readPos.store(pos + got);
        return got;
    }

    bool ReadFileAsync(AsyncReadRequest& req)
{
    std::uint32_t count = 1;
    BatchReadFile(&req, count, std::chrono::microseconds(0));
    return true;
}

    std::uint32_t BatchReadFile(AsyncReadRequest* reqs,
                            std::uint32_t     count,
                            const std::chrono::microseconds& /*timeout*/,
                            int /*batchSize*/ = -1) override
{
    if (count == 0) return 0;

    std::vector<std::future<bool>> futures;
    futures.reserve(count);

    for (std::uint32_t i = 0; i < count; ++i) {
        auto& req = reqs[i];

        if (!m_available.load() || !req.m_buffer) {
            if (req.m_callback) req.m_callback(false);
            futures.push_back(std::async(std::launch::deferred, 
                                         []{ return false; }));
            continue;
        }

        // Fast path: serve from prefetch buffer synchronously,
        // no need to launch a thread for this
        if (m_prefetchDone && 
            req.m_offset + req.m_readSize <= m_prefetchSize) {
            std::memcpy(req.m_buffer, 
                       m_prefetchBuf.data() + req.m_offset, 
                       req.m_readSize);
            if (req.m_callback) req.m_callback(true);
            futures.push_back(std::async(std::launch::deferred, 
                                         []{ return true; }));
            continue;
        }

        // S3 path: launch each GET concurrently
        futures.push_back(std::async(std::launch::async,
            [this, &req]() -> bool {
                Aws::S3::Model::GetObjectRequest s3req;
                s3req.SetBucket(m_bucket);
                s3req.SetKey(m_key);
                s3req.SetRange(MakeRange(req.m_offset,
                                        req.m_offset + req.m_readSize - 1));

                auto outcome = m_client->GetObject(s3req);
                bool success = false;
                if (outcome.IsSuccess()) {
                    auto result = outcome.GetResultWithOwnership();
                    auto& body = result.GetBody();
                    std::uint64_t totalRead = 0;
                    while (totalRead < req.m_readSize) {
                        body.read(req.m_buffer + totalRead,
                                 static_cast<std::streamsize>(
                                     req.m_readSize - totalRead));
                        std::uint64_t got = 
                            static_cast<std::uint64_t>(body.gcount());
                        if (got == 0) {
                            std::cerr << "[S3FileIO] Short read offset="
                                      << req.m_offset
                                      << " got=" << totalRead
                                      << " expected=" << req.m_readSize
                                      << "\n";
                            break;
                        }
                        totalRead += got;
                    }
                    success = (totalRead == req.m_readSize);
                } else {
                    std::cerr << "[S3FileIO] GET failed offset=" 
                              << req.m_offset
                              << " size=" << req.m_readSize << " : "
                              << outcome.GetError().GetMessage() << "\n";
                }

                if (req.m_callback) req.m_callback(success);
                return success;
            }
        ));
    }

    // Wait for all S3 GETs to complete before returning
    std::uint32_t succeeded = 0;
    for (auto& f : futures) {
        if (f.get()) succeeded++;
    }
        return succeeded;
    }

    std::uint64_t TellP() override { return m_readPos.load(); }

    std::uint64_t WriteBinary(std::uint64_t, const char*, std::uint64_t = UINT64_MAX) override
    {
        std::cerr << "[S3FileIO] WriteBinary: S3 backend is read-only.\n";
        return 0;
    }

    std::uint64_t ReadString(std::uint64_t& readSize,
                             std::unique_ptr<char[]>& buffer,
                             char delim = '\n',
                             std::uint64_t offset = UINT64_MAX) override
    {
        if (!m_available) return 0;
        std::uint64_t pos = (offset == UINT64_MAX) ? m_readPos.load() : offset;
        if (pos >= m_fileSize) return 0;

        std::uint64_t toRead = std::min(readSize, m_fileSize - pos);
        std::string range = MakeRange(pos, pos + toRead - 1);

        Aws::S3::Model::GetObjectRequest req;
        req.SetBucket(m_bucket);
        req.SetKey(m_key);
        req.SetRange(range);
        auto outcome = m_client->GetObject(req);
        if (!outcome.IsSuccess()) return 0;
	auto result = outcome.GetResultWithOwnership();
        auto& body = result.GetBody();
        std::uint64_t count = 0;
        for (int c = body.get(); c != EOF && count < readSize; c = body.get()) {
            if (c == '\r') c = '\n';
            if (count >= readSize) {
                readSize *= 2;
                std::unique_ptr<char[]> nb(new char[readSize]);
                memcpy(nb.get(), buffer.get(), count);
                buffer.swap(nb);
            }
            if (c == delim) { buffer[count++] = '\0'; break; }
            buffer[count++] = static_cast<char>(c);
        }
        m_readPos.store(pos + count);
        return count;
    }
    

    std::uint64_t WriteString(const char*, std::uint64_t = UINT64_MAX) override
    {
        std::cerr << "[S3FileIO] WriteString: S3 backend is read-only.\n";
        return 0;
    }

    void ShutDown() override
    {
        m_available.store(false);
        m_client.reset();
        m_prefetchBuf.clear();
        m_prefetchBuf.shrink_to_fit();
    }

private:
    std::shared_ptr<Aws::S3::S3Client> m_client;
    std::mutex m_clientMutex;
    std::string                m_bucket;
    std::string                m_key;
    std::uint64_t              m_fileSize;
    std::atomic<std::uint64_t> m_readPos;
    std::atomic<bool>          m_available;

    // Prefetch buffer for fast sequential header reads
    std::vector<char>          m_prefetchBuf;
    std::uint64_t              m_prefetchSize = 0;
    bool                       m_prefetchDone = false;

    static std::string MakeRange(std::uint64_t first, std::uint64_t last)
    {
        return "bytes=" + std::to_string(first) + "-" + std::to_string(last);
    }
};

} // namespace Helper
} // namespace SPTAG
