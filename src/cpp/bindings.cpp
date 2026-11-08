#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/eval.h>
#include "DataProcessor.hpp"

namespace py = pybind11;

PYBIND11_MODULE(fast_processor, m) {
    m.doc() = "High-performance C++ core for Financial data processing";

    py::class_<DataPacket>(m, "DataPacket")
        .def(py::init<>())
        .def(py::init<uint64_t, uint32_t, double>(),
             py::arg("timestamp"), 
             py::arg("asset_id"), 
             py::arg("value"))
        .def_readwrite("timestamp", &DataPacket::timestamp)
        .def_readwrite("asset_id", &DataPacket::asset_id)
        .def_readwrite("value", &DataPacket::value);

    py::class_<AnalysisResult>(m, "AnalysisResult")
        .def_readwrite("timestamp", &AnalysisResult::timestamp)
        .def_readwrite("moving_average", &AnalysisResult::moving_average)
        .def_readwrite("is_anomaly", &AnalysisResult::is_anomaly);

    py::class_<StreamProcessor, std::shared_ptr<StreamProcessor>>(m, "StreamProcessor")
        .def("process_point", &StreamProcessor::process_point)
        .def("process_stream", &StreamProcessor::process_stream)
        .def("reset", &StreamProcessor::reset);

     py::class_<WindowStreamProcessor, StreamProcessor, std::shared_ptr<WindowStreamProcessor>>(m, "WindowStreamProcessor")
          .def(py::init<>())
          .def(py::init<size_t, double, double>(), 
             py::arg("window_size") = 50, 
             py::arg("threshold_multiplier") = 2.5, 
             py::arg("min_percentage_floor") = 0.0002)
        .def("process_point", &WindowStreamProcessor::process_point)
        .def("reset", &WindowStreamProcessor::reset);

    py::class_<EwmaStreamProcessor, StreamProcessor, std::shared_ptr<EwmaStreamProcessor>>(m, "EwmaStreamProcessor")
        .def(py::init<double, double, double>(), 
             py::arg("alpha") = 0.2, 
             py::arg("threshold_multiplier") = 2.5, 
             py::arg("min_percentage_floor") = 0.0002)
        .def("process_point", &EwmaStreamProcessor::process_point)    
        .def("reset", &EwmaStreamProcessor::reset);
}