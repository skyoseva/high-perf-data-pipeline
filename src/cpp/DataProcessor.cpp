#include "DataProcessor.hpp"

StreamProcessor::StreamProcessor(size_t window_size, double anomaly_threshold_multiplier):window_size_(window_size), anomaly_threshold_multiplier_(anomaly_threshold_multiplier){}
AnalysisResult StreamProcessor::process_point(uint64_t timestamp, double value)
{
    window_values_.push_back(value);
    running_sum_ += value;

    if(window_size_ < window_values_.size())
    {
        running_sum_ -= window_values_.front();
        window_values_.pop_front();
    }

    double mean = running_sum_/window_values_.size();

    double variance_sum = 0;

    for(double value: window_values_)
        variance_sum += (value - mean) * (value - mean);

    double stddev = sqrt(variance_sum/window_values_.size());

    bool is_anomaly = false;

    if(stddev > 1e-5 && window_values_.size() == window_size_)
        is_anomaly = abs(mean - value) > (stddev * anomaly_threshold_multiplier_);

    return {timestamp, mean, is_anomaly};
}

std::vector<AnalysisResult> StreamProcessor::process_stream(const std::vector<DataPacket> packets)
{
    std::vector<AnalysisResult> result;
    result.reserve(packets.size());

    for(DataPacket packet: packets)
        result.push_back(process_point(packet.timestamp, packet.value));

    return result;
}

void StreamProcessor::reset()
{
    window_values_.clear();
    running_sum_ = 0;
}
