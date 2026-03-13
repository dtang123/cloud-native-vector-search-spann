// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Core/Common/CommonUtils.h"
#include "inc/Core/Common/QueryResultSet.h"
#include "inc/Core/Common/TruthSet.h"
#include "inc/Core/VectorIndex.h"
#ifdef __linux__
#include "inc/Helper/AsyncFileReader.h"
#endif
#include "inc/Helper/CommonHelper.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Helper/VectorSetReader.h"
#include "inc/Helper/DiskIO.h" //CHANGED added
#include "inc/Helper/S3FileIO.h" // CHANGED added
#include <aws/core/Aws.h> // CHANGED added
#include <algorithm>
#include <cctype>
#include <atomic>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <set>
#include <thread>

using namespace SPTAG;
//CHANGED start
/*namespace SPTAG {
namespace SPANN {
    std::function<std::shared_ptr<Helper::DiskIO>(void)> f_createAsyncIO = []() -> std::shared_ptr<Helper::DiskIO> {
    	std::cerr << "[S3FileIO] f_createAsyncIO lambda called\n";
        std::cerr.flush();
	return std::make_shared<Helper::S3FileIO>();
    };
}
}
//CHANGED end
*/
class SearcherOptions : public Helper::ReaderOptions
{
  public:
    SearcherOptions() : Helper::ReaderOptions(VectorValueType::Float, 0, VectorFileType::DEFAULT, "|", 32) //Changed TXT to DEFAULT
    {
        AddRequiredOption(m_queryFile, "-i", "--input", "Input raw data.");
        AddRequiredOption(m_indexFolder, "-x", "--index", "Index folder.");
        AddOptionalOption(m_truthFile, "-r", "--truth", "Truth file.");
        AddOptionalOption(m_resultFile, "-o", "--result", "Output result file.");
        AddOptionalOption(m_maxCheck, "-m", "--maxcheck", "MaxCheck for index.");
        AddOptionalOption(m_withMeta, "-a", "--withmeta", "Output metadata instead of vector id.");
        AddOptionalOption(m_K, "-k", "--KNN", "K nearest neighbors for search.");
        AddOptionalOption(m_truthK, "-tk", "--truthKNN", "truth set number.");
        AddOptionalOption(m_dataFile, "-df", "--data", "original data file.");
        AddOptionalOption(m_dataFileType, "-dft", "--dataFileType", "original data file type. (TXT, or DEFAULT)");
        AddOptionalOption(m_batch, "-b", "--batchsize", "Batch query size.");
        AddOptionalOption(m_genTruth, "-g", "--gentruth", "Generate truth file.");
        AddOptionalOption(m_debugQuery, "-q", "--debugquery", "Debug query number.");
        AddOptionalOption(m_enableADC, "-adc", "--adc", "Enable ADC Distance computation");
        AddOptionalOption(m_outputformat, "-of", "--ouputformat", "0: TXT 1: BINARY.");
        
        // Common options used by experiment scripts
        AddOptionalOption(m_dimensions, "-d", "--dimensions", "Vector dimensions.");
        AddOptionalOption(m_inputValueType, "-v", "--valuetype", "Dataset value type. (Float, UInt8, Int8)");
        AddOptionalOption(m_dataFileType, "-f", "--filetype", "Original data file type. (TXT or DEFAULT)");
        AddOptionalOption(m_threadNum, "-t", "--threads", "Number of concurrent queries / threads.");

        // Cache control (SPANN BuildSSDIndex.CacheSizeGB)
        AddOptionalOption(m_enableCache, "-ec", "--enablecache", "Enable cache (true/false)");
        AddOptionalOption(m_cacheConfig, "-cc", "--cachesize", "Cache size string (e.g. 4096M, 4G)");
    }

    ~SearcherOptions()
    {
    }

    std::string m_queryFile;

    std::string m_indexFolder;

    std::string m_dataFile = "";

    std::string m_truthFile = "";

    std::string m_resultFile = "";

    std::string m_maxCheck = "8192";

    VectorFileType m_dataFileType = VectorFileType::DEFAULT;

    int m_withMeta = 0;

    int m_K = 32;

    int m_truthK = -1;

    int m_batch = 10000;

    int m_genTruth = 0;

    int m_debugQuery = -1;

    bool m_enableADC = false;

    int m_outputformat = 0;

    int m_dimensions = 0;

    bool m_enableCache = false;
    
    int m_threadNum = 1;

    std::string m_inputValueType = "";

    std::string m_cacheConfig = "";
    
};

template <typename T> int Process(std::shared_ptr<SearcherOptions> options, VectorIndex &index)
{
    std::ofstream log("Recall-result.out", std::ios::app);
    if (!log.is_open())
    {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "ERROR: Cannot open logging file!\n");
        exit(-1);
    }

    auto vectorReader = Helper::VectorSetReader::CreateInstance(options);
    if (ErrorCode::Success != vectorReader->LoadFile(options->m_queryFile))
    {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read query file.\n");
        exit(1);
    }
    auto queryVectors = vectorReader->GetVectorSet(0, options->m_debugQuery);
    auto queryMetas = vectorReader->GetMetadataSet();

    std::shared_ptr<Helper::ReaderOptions> dataOptions(
        new Helper::ReaderOptions(queryVectors->GetValueType(), queryVectors->Dimension(), options->m_dataFileType));
    auto dataReader = Helper::VectorSetReader::CreateInstance(dataOptions);
    std::shared_ptr<VectorSet> dataVectors;
    if (options->m_dataFile != "")
    {
        if (ErrorCode::Success != dataReader->LoadFile(options->m_dataFile))
        {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read data file.\n");
            exit(1);
        }
        dataVectors = dataReader->GetVectorSet();
    }

    std::shared_ptr<Helper::DiskIO> ftruth;
    int truthDim = 0;
    if (options->m_truthFile != "")
    {
        if (options->m_genTruth)
        {
            if (dataVectors == nullptr)
            {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Cannot load data vectors to generate groundtruth! Please "
                                                         "speicify data vector file by setting -df option.\n");
                exit(1);
            }
            COMMON::TruthSet::GenerateTruth<T>(
                queryVectors, dataVectors, options->m_truthFile, index.GetDistCalcMethod(), options->m_truthK,
                (options->m_truthFile.find("bin") != std::string::npos) ? TruthFileType::DEFAULT : TruthFileType::TXT,
                index.m_pQuantizer);
        }

        ftruth = SPTAG::f_createIO();
        if (ftruth == nullptr || !ftruth->Initialize(options->m_truthFile.c_str(), std::ios::in | std::ios::binary))
        {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "ERROR: Cannot open %s for read!\n", options->m_truthFile.c_str());
            exit(1);
        }
        if (options->m_truthFile.find("bin") != std::string::npos)
        {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Load binary truth...\n");
        }
        else
        {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Load txt truth...\n");
        }
    }

    std::shared_ptr<Helper::DiskIO> fp;
    if (options->m_resultFile != "")
    {
        fp = SPTAG::f_createIO();
        if (fp == nullptr || !fp->Initialize(options->m_resultFile.c_str(), std::ios::out | std::ios::binary))
        {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "ERROR: Cannot open %s for write!\n",
                         options->m_resultFile.c_str());
        }

        if (options->m_outputformat == 1)
        {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Using output format binary...");

            int32_t i32Val = queryVectors->Count();
            if (fp->WriteBinary(sizeof(i32Val), reinterpret_cast<char *>(&i32Val)) != sizeof(i32Val))
            {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to write result file!\n");
                exit(1);
            }
            i32Val = options->m_K;
            if (fp->WriteBinary(sizeof(i32Val), reinterpret_cast<char *>(&i32Val)) != sizeof(i32Val))
            {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to write result file!\n");
                exit(1);
            }
        }
    }

    std::vector<std::string> maxCheck = Helper::StrUtils::SplitString(options->m_maxCheck, "#");
    if (options->m_truthK < 0)
        options->m_truthK = options->m_K;

    std::vector<std::set<SizeType>> truth(options->m_batch);
    int internalResultNum = options->m_K;
    if (index.GetIndexAlgoType() == IndexAlgoType::SPANN)
    {
        int SPANNInternalResultNum;
        if (SPTAG::Helper::Convert::ConvertStringTo<int>(
                index.GetParameter("SearchInternalResultNum", "BuildSSDIndex").c_str(), SPANNInternalResultNum))
            internalResultNum = max(internalResultNum, SPANNInternalResultNum);
    }
    std::vector<QueryResult> results(options->m_batch, QueryResult(NULL, internalResultNum, options->m_withMeta != 0));
    std::vector<float> latencies(options->m_batch, 0);
    int baseSquare = SPTAG::COMMON::Utils::GetBase<T>() * SPTAG::COMMON::Utils::GetBase<T>();

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                 "[query]\t\t[maxcheck]\t[avg] \t[99%] \t[95%] \t[recall] \t[qps] \t[mem]\n");
    std::vector<float> totalAvg(maxCheck.size(), 0.0), total99(maxCheck.size(), 0.0), total95(maxCheck.size(), 0.0),
        totalRecall(maxCheck.size(), 0.0), totalLatency(maxCheck.size(), 0.0);
    for (int startQuery = 0; startQuery < queryVectors->Count(); startQuery += options->m_batch)
    {
        int numQuerys = min(options->m_batch, queryVectors->Count() - startQuery);
        for (SizeType i = 0; i < numQuerys; i++)
            results[i].SetTarget(queryVectors->GetVector(startQuery + i));
        if (ftruth != nullptr)
            COMMON::TruthSet::LoadTruth(ftruth, truth, numQuerys, truthDim, options->m_truthK,
                                        (options->m_truthFile.find("bin") != std::string::npos) ? TruthFileType::DEFAULT
                                                                                                : TruthFileType::TXT);

        for (int mc = 0; mc < maxCheck.size(); mc++)
        {
            index.SetParameter("MaxCheck", maxCheck[mc].c_str());

            for (SizeType i = 0; i < numQuerys; i++)
                results[i].Reset();

            std::atomic_size_t queriesSent(0);
            std::vector<std::thread> threads;
            threads.reserve(options->m_threadNum);
            auto batchstart = std::chrono::high_resolution_clock::now();

            for (std::uint32_t i = 0; i < options->m_threadNum; i++)
            {
                threads.emplace_back([&, i] {
                    NumaStrategy ns = (index.GetIndexAlgoType() == IndexAlgoType::SPANN)
                                          ? NumaStrategy::SCATTER
                                          : NumaStrategy::LOCAL; // Only for SPANN, we need to avoid IO threads overlap
                                                                 // with search threads.
                    Helper::SetThreadAffinity(i, threads[i], ns, OrderStrategy::ASC);

                    size_t qid = 0;
                    while (true)
                    {
                        qid = queriesSent.fetch_add(1);
                        if (qid < numQuerys)
                        {
                            auto t1 = std::chrono::high_resolution_clock::now();
                            index.SearchIndex(results[qid]);
                            auto t2 = std::chrono::high_resolution_clock::now();
                            latencies[qid] =
                                (float)(std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count() /
                                        1000000.0);
                        }
                        else
                        {
                            return;
                        }
                    }
                });
            }
            for (auto &thread : threads)
            {
                thread.join();
            }

            auto batchend = std::chrono::high_resolution_clock::now();
            float batchLatency =
                (float)(std::chrono::duration_cast<std::chrono::microseconds>(batchend - batchstart).count() /
                        1000000.0);

            float timeMean = 0, timeMin = MaxDist, timeMax = 0, timeStd = 0;
            for (int qid = 0; qid < numQuerys; qid++)
            {
                timeMean += latencies[qid];
                if (latencies[qid] > timeMax)
                    timeMax = latencies[qid];
                if (latencies[qid] < timeMin)
                    timeMin = latencies[qid];
            }
            timeMean /= numQuerys;
            for (int qid = 0; qid < numQuerys; qid++)
                timeStd += ((float)latencies[qid] - timeMean) * ((float)latencies[qid] - timeMean);
            timeStd = std::sqrt(timeStd / numQuerys);
            log << timeMean << " " << timeStd << " " << timeMin << " " << timeMax << " ";

            std::sort(latencies.begin(), latencies.begin() + numQuerys);
            float l99 = latencies[SizeType(numQuerys * 0.99)];
            float l95 = latencies[SizeType(numQuerys * 0.95)];

            float recall = 0;
            if (ftruth != nullptr)
            {
                recall = COMMON::TruthSet::CalculateRecall<T>(&index, results, truth, options->m_K, options->m_truthK,
                                                              queryVectors, dataVectors, numQuerys, &log,
                                                              options->m_debugQuery > 0);
            }

#ifndef _MSC_VER
            struct rusage rusage;
            getrusage(RUSAGE_SELF, &rusage);
            unsigned long long peakWSS = rusage.ru_maxrss * 1024 / 1000000000;
#else
            PROCESS_MEMORY_COUNTERS pmc;
            GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc));
            unsigned long long peakWSS = pmc.PeakWorkingSetSize / 1000000000;
#endif
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "%d-%d\t%s\t%.4f\t%.4f\t%.4f\t%.4f\t\t%.4f\t\t%lluGB\n", startQuery,
                         (startQuery + numQuerys), maxCheck[mc].c_str(), timeMean, l99, l95, recall,
                         (numQuerys / batchLatency), peakWSS);
            totalAvg[mc] += timeMean * numQuerys;
            total95[mc] += l95 * numQuerys;
            total99[mc] += l99 * numQuerys;
            totalRecall[mc] += recall * numQuerys;
            totalLatency[mc] += batchLatency;
        }

        if (fp != nullptr)
        {
            if (options->m_outputformat == 0)
            {
                for (SizeType i = 0; i < numQuerys; i++)
                {
                    if (queryMetas != nullptr)
                    {
                        ByteArray qmeta = queryMetas->GetMetadata(startQuery + i);
                        if (fp->WriteBinary(qmeta.Length(), (const char *)qmeta.Data()) != qmeta.Length())
                        {
                            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Cannot write qmeta %d bytes!\n", qmeta.Length());
                            exit(1);
                        }
                    }
                    else
                    {
                        std::string qid = std::to_string(i);
                        if (fp->WriteBinary(qid.length(), qid.c_str()) != qid.length())
                        {
                            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Cannot write qid %d bytes!\n", qid.length());
                            exit(1);
                        }
                    }
                    fp->WriteString(":");
                    for (int j = 0; j < options->m_K; j++)
                    {
                        std::string sd = std::to_string(results[i].GetResult(j)->Dist / baseSquare);
                        if (fp->WriteBinary(sd.length(), sd.c_str()) != sd.length())
                        {
                            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Cannot write dist %d bytes!\n", sd.length());
                            exit(1);
                        }
                        fp->WriteString("@");
                        if (results[i].GetResult(j)->VID < 0)
                        {
                            fp->WriteString("NULL|");
                            continue;
                        }

                        if (!options->m_withMeta)
                        {
                            std::string vid = std::to_string(results[i].GetResult(j)->VID);
                            if (fp->WriteBinary(vid.length(), vid.c_str()) != vid.length())
                            {
                                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Cannot write vid %d bytes!\n", sd.length());
                                exit(1);
                            }
                        }
                        else
                        {
                            ByteArray vm = index.GetMetadata(results[i].GetResult(j)->VID);
                            if (fp->WriteBinary(vm.Length(), (const char *)vm.Data()) != vm.Length())
                            {
                                SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Cannot write vmeta %d bytes!\n", vm.Length());
                                exit(1);
                            }
                        }
                        fp->WriteString("|");
                    }
                    fp->WriteString("\n");
                }
            }
            else
            {
                for (SizeType i = 0; i < numQuerys; ++i)
                {
                    for (int j = 0; j < options->m_K; ++j)
                    {
                        SizeType i32Val = results[i].GetResult(j)->VID;
                        if (fp->WriteBinary(sizeof(i32Val), reinterpret_cast<char *>(&i32Val)) != sizeof(i32Val))
                        {
                            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to write result file!\n");
                            exit(1);
                        }

                        float fVal = results[i].GetResult(j)->Dist;
                        if (fp->WriteBinary(sizeof(fVal), reinterpret_cast<char *>(&fVal)) != sizeof(fVal))
                        {
                            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Fail to write result file!\n");
                            exit(1);
                        }
                    }
                }
            }
        }
    }
    for (int mc = 0; mc < maxCheck.size(); mc++)
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "%d-%d\t%s\t%.4f\t%.4f\t%.4f\t%.4f\t%.4f\n", 0, queryVectors->Count(),
                     maxCheck[mc].c_str(), (totalAvg[mc] / queryVectors->Count()),
                     (total99[mc] / queryVectors->Count()), (total95[mc] / queryVectors->Count()),
                     (totalRecall[mc] / queryVectors->Count()), (queryVectors->Count() / totalLatency[mc]));

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Output results finish!\n");

    if (fp != nullptr)
        fp->ShutDown();
    log.close();
    return 0;
}

int main(int argc, char **argv)
{
    std::shared_ptr<SearcherOptions> options(new SearcherOptions);
    if (!options->Parse(argc - 1, argv + 1))
    {
        exit(1);
    }
    // CHANGED START
    std::cerr << "[DEBUG] INIT AWS SDK \n";
    std::cerr.flush();
    Aws::SDKOptions awsOptions;
    Aws::InitAPI(awsOptions);
    {

    /*SPTAG::f_createIO = []() -> std::shared_ptr<Helper::DiskIO> {
    	return std::shared_ptr<Helper::DiskIO>(new Helper::SimpleFileIO());
    };*/
    // CHANGED END
    std::cerr << "[DEBUG] Calling Load Index \n";
    std::cerr.flush();
    std::shared_ptr<SPTAG::VectorIndex> vecIndex;
    auto ret = SPTAG::VectorIndex::LoadIndex(options->m_indexFolder, vecIndex);
    if (SPTAG::ErrorCode::Success != ret || nullptr == vecIndex)
    {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Cannot open index configure file!");
        return -1;
    }
    vecIndex->SetQuantizerADC(options->m_enableADC);

    Helper::IniReader iniReader;
    for (int i = 1; i < argc; i++)
    {
        std::string param(argv[i]);
        size_t idx = param.find("=");
        if (idx == std::string::npos)
            continue;

        std::string paramName = param.substr(0, idx);
        std::string paramVal = param.substr(idx + 1);
        std::string sectionName;
        idx = paramName.find(".");
        if (idx != std::string::npos)
        {
            sectionName = paramName.substr(0, idx);
            paramName = paramName.substr(idx + 1);
        }
        iniReader.SetParameter(sectionName, paramName, paramVal);
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Set [%s]%s = %s\n", sectionName.c_str(), paramName.c_str(),
                     paramVal.c_str());
    }

    std::cerr << "[DEBUG Main.cpp] Finished Loading Parameters \n";
    std::cerr << "[DEBUG Main.cpp] Starting Cache configs \n";
    std::cerr.flush();
    // NEW: Handle cache options passed via short flags (-ec, -cc)
    if (options->m_enableCache || !options->m_cacheConfig.empty())
    {
        int cacheGB = 0;
        if (!options->m_cacheConfig.empty())
        {
            std::string s = options->m_cacheConfig;
            try
            {
                char unit = s.back();
                if (std::isalpha(unit))
                {
                    std::string numstr = s.substr(0, s.size() - 1);
                    long long num = std::stoll(numstr);
                    if (unit == 'M' || unit == 'm')
                        cacheGB = (int)(num / 1024);
                    else if (unit == 'G' || unit == 'g')
                        cacheGB = (int)num;
                    else if (unit == 'K' || unit == 'k')
                        cacheGB = (int)(num / 1024 / 1024);
                }
                else
                {
                    long long num = std::stoll(s);
                    cacheGB = (int)(num / 1024); // treat as MB if no unit
                }
            }
            catch (...) { cacheGB = 0; }
        }

        if (cacheGB > 0)
        {
            iniReader.SetParameter("BuildSSDIndex", "CacheSizeGB", std::to_string(cacheGB));
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Set [BuildSSDIndex]CacheSizeGB = %d\n", cacheGB);
        }
        else if (options->m_enableCache)
        {
            iniReader.SetParameter("BuildSSDIndex", "CacheSizeGB", "1");
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Set [BuildSSDIndex]CacheSizeGB = 1 (enable cache)\n");
        }
    }

    VectorValueType valueType = VectorValueType::Undefined;
    std::string vType = options->m_inputValueType;
    std::transform(vType.begin(), vType.end(), vType.begin(), [](unsigned char c){ return std::tolower(c); });

    if (vType == "float") valueType = VectorValueType::Float;
    else if (vType == "int8") valueType = VectorValueType::Int8;
    else if (vType == "uint8") valueType = VectorValueType::UInt8;

    std::string sections[] = {"Base", "SelectHead", "BuildHead", "BuildSSDIndex", "Index"};
    std::cerr << "[DEBUG Main.cpp] Starting INI sectionprocessing \n";
    std::cerr.flush();
    for (int i = 0; i < 5; i++)
    {
        if (!iniReader.DoesParameterExist(sections[i], "NumberOfThreads"))
        {
            iniReader.SetParameter(sections[i], "NumberOfThreads", std::to_string(options->m_threadNum));
        }
        for (const auto &iter : iniReader.GetParameters(sections[i]))
        {
            vecIndex->SetParameter(iter.first.c_str(), iter.second.c_str(), sections[i]);
        }
    }

    vecIndex->UpdateIndex();
    std::cerr << "[DEBUG] m_inputValueType='" << options->m_inputValueType << "' valueType=" << (int)valueType << "\n";
    std::cerr.flush();
    switch (valueType)
    {
#define DefineVectorValueType(Name, Type)                                                                              \
    case VectorValueType::Name:                                                                                        \
        Process<Type>(options, *(vecIndex.get()));                                                                     \
        break;

#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType

    default:
        break;
    }
    }
    Aws::ShutdownAPI(awsOptions); // CHANGED added
    return 0;
}
