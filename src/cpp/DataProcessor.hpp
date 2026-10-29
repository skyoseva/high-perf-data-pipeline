#pragma once
#include <cmath>
#include <cstdint>
#include <vector>
#include <deque>

using namespace std;

struct DataPacket
{
    uint64_t timestamp;
    uint32_t asset_id;
    double value;
};

struct AnalysisResult
{
    uint64_t timestamp;
    double moving_average;
    bool is_anomaly;
};

class StreamProcessor
{
private:

    size_t window_size_;
    double anomaly_threshold_multiplier_;
    deque<double> window_values_;
    double running_sum_;
public:
    StreamProcessor(size_t window_size = 50, double anomaly_threshold_multplier = 2.5);
    AnalysisResult process_point(uint64_t timestamp, double value);
    std::vector<AnalysisResult> process_stream(const std::vector<DataPacket> packets);
    void reset();
};