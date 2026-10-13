import cpp_processor 

processor = cpp_processor.DataProcessor()
prices = [10.0, 12.0, 14.0, 16.0, 18.0, 20.0]
result = processor.calculate_sma(prices, window = 3)

print("Cpp result: ", result)