#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <vector> 

namespace py = pybind11;

class DataProcessor
{
    public:

    DataProcessor() = default;

    std::vector<double> calculate_sma(const std::vector<double>& prices, int window)
    {
        if(window == 0 || window > prices.size())
        {
            throw std::invalid_argument("Invalid window size");
        }

        std::vector<double> sma;
        sma.reserve(prices.size() - window + 1);

        double sma_sum = 0.0;

        for(int i = 0; i < window; i++)
            sma_sum += prices[i];

        sma.push_back(sma_sum/window);

        for(int i = window; i < prices.size(); i++)
        {
            sma_sum += prices[i] - prices[i - window];
            sma.push_back(sma_sum/window);
        }

        return sma;
    } 
};

PYBIND11_MODULE(cpp_processor, m)
{
    m.doc() = "High performance data processing module";

    py::class_<DataProcessor>(m, "DataProcessor")
        .def(py::init<>())
        .def("calculate_sma", &DataProcessor::calculate_sma, 
             py::arg("prices"), py::arg("window"),
             "Calculates Simple Moving Average on a vector of prices.");
}