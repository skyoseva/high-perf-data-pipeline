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
public:
    virtual ~StreamProcessor() = default;
    virtual AnalysisResult process_point(uint64_t timestamp, double value) = 0;
    virtual std::vector<AnalysisResult> process_stream(const std::vector<DataPacket>& packets);
    virtual void reset() = 0;
};

class WindowStreamProcessor: public StreamProcessor
{
private:
    size_t window_size_;
    double threshold_multiplier_;
    deque<double> window_values_;
    double running_sum_;
    double min_percentage_floor_;
public:
    WindowStreamProcessor(size_t window_size = 50, double threshold_multiplier = 2.5, double min_percentage_floor = 0.0002);
    AnalysisResult process_point(uint64_t timestamp, double value) override;
    void reset() override;
};

class EwmaStreamProcessor : public StreamProcessor {
private:
    double alpha_;
    double threshold_multiplier_;
    double current_ema_;
    double current_deviation_;
    bool initialized_;
    double min_percentage_floor_;

public:
    EwmaStreamProcessor(double alpha = 0.2, double threshold_multiplier = 2.5,
                        double min_percentage_floor = 0.0002);
    AnalysisResult process_point(uint64_t timestamp, double value) override;
    void reset() override;
};