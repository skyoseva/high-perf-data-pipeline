#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/eval.h>
#include "DataProcessor.hpp"

namespace py = pybind11;

PYBIND11_MODULE(fast_processor, m) {
    m.doc() = "High-performance C++ core for IoT/Financial data processing";

    py::class_<DataPacket>(m, "DataPacket")
        .def(py::init<uint64_t, uint32_t, double>(),
             py::arg("timestamp"), py::arg("device_id"), py::arg("value"))
        .def_readwrite("timestamp", &DataPacket::timestamp)
        .def_readwrite("device_id", &DataPacket::device_id)
        .def_readwrite("value", &DataPacket::value);

    py::class_<AnalysisResult>(m, "AnalysisResult")
        .def_readwrite("timestamp", &AnalysisResult::timestamp)
        .def_readwrite("moving_average", &AnalysisResult::moving_average)
        .def_readwrite("is_anomaly", &AnalysisResult::is_anomaly);

    py::class_<StreamProcessor>(m, "StreamProcessor")
        .def(py::init<size_t, double>(), 
             py::arg("window_size") = 50, 
             py::arg("threshold_multiplier") = 2.5)
        .def("process_point", &StreamProcessor::process_point,
             py::arg("timestamp"), py::arg("value"),
             "Process a single data point.")
        .def("process_stream", &StreamProcessor::process_stream,
             py::arg("packets"),
             "Process a batch of DataPackets efficiently in C++.")
        .def("reset", &StreamProcessor::reset,
             "Reset internal sliding window state.");
}