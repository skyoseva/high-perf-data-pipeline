#include "DataProcessor.hpp"

std::vector<AnalysisResult> StreamProcessor::process_stream(const std::vector<DataPacket>& packets)
{
    std::vector<AnalysisResult> result;
    result.reserve(packets.size());

    for(const auto& packet: packets)
        result.push_back(process_point(packet.timestamp, packet.value));
    return result;
}

WindowStreamProcessor::WindowStreamProcessor(size_t window_size, double threshold_multiplier, double min_percentage_floor)
    : window_size_(window_size), 
      threshold_multiplier_(threshold_multiplier),
      min_percentage_floor_(min_percentage_floor) {}

AnalysisResult WindowStreamProcessor::process_point(uint64_t timestamp, double value)
{
    window_values_.push_back(value);
    running_sum_ += value;

    if(window_size_ < window_values_.size())
    {
        running_sum_ -= window_values_.front();
        window_values_.pop_front();
    }

    double mean = value;
    double stddev = 0.0;
    bool is_anomaly = false;

    if (window_values_.size() == window_size_) {
        mean = running_sum_ / window_size_;

        double sq_sum = 0.0;
        for (double val : window_values_) {
            sq_sum += (val - mean) * (val - mean);
        }
        stddev = std::sqrt(sq_sum / window_size_);

        double percentage_floor = mean * min_percentage_floor_;
        double effective_stddev = std::max(stddev, std::max(percentage_floor, 1e-5));

        if (std::abs(mean - value) > (effective_stddev * threshold_multiplier_)) {
            is_anomaly = true;
        }
    }

    return {timestamp, mean, is_anomaly};
}

void WindowStreamProcessor::reset()
{
    window_values_.clear();
    running_sum_ = 0;
    threshold_multiplier_ = 2.5; // Reset to default value
    window_size_ = 50; // Reset to default value
    min_percentage_floor_ = 0.0002; // Reset to default value
}

EwmaStreamProcessor::EwmaStreamProcessor(double alpha, double threshold_multiplier,double min_percentage_floor)
    : alpha_(alpha), threshold_multiplier_(threshold_multiplier), current_ema_(0.0), current_deviation_(0.0), initialized_(false), min_percentage_floor_(min_percentage_floor) {}

AnalysisResult EwmaStreamProcessor::process_point(uint64_t timestamp, double value) {
    bool is_anomaly = false;

    if (!initialized_) {
        current_ema_ = value;
        current_deviation_ = 1.0;
        initialized_ = true;
        return {timestamp, current_ema_, false};
    }

    double residual = std::abs(value - current_ema_);
    double percentage_floor = current_ema_ * min_percentage_floor_;
    double min_floor = std::max(percentage_floor, 1e-5);
    double effective_deviation = std::max(current_deviation_, min_floor);

    if (residual > (effective_deviation * threshold_multiplier_)) {
        is_anomaly = true;
    }

    if (!is_anomaly) {
        current_ema_ = alpha_ * value + (1.0 - alpha_) * current_ema_;
        current_deviation_ = alpha_ * residual + (1.0 - alpha_) * current_deviation_;
    }

    return {timestamp, current_ema_, is_anomaly};
}

void EwmaStreamProcessor::reset() {
    current_ema_ = 0.0;
    current_deviation_ = 0.0;
    initialized_ = false;
    alpha_ = 0.2; // Reset to default value
    threshold_multiplier_ = 2.5; // Reset to default value
}

