#include <iostream>
#include <vector>
#include <numeric>
#include <cmath>

// 计算均值
double mean(const std::vector<double>& data) {
    return std::accumulate(data.begin(), data.end(), 0.0) / data.size();
}

// 计算协方差
double covariance(const std::vector<double>& x, const std::vector<double>& y) {
    double mean_x = mean(x);
    double mean_y = mean(y);
    double cov = 0.0;
    for (size_t i = 0; i < x.size(); ++i) {
        cov += (x[i] - mean_x) * (y[i] - mean_y);
    }
    return cov / x.size();
}

// 计算方差
double variance(const std::vector<double>& x) {
    double mean_x = mean(x);
    double var = 0.0;
    for (size_t i = 0; i < x.size(); ++i) {
        var += (x[i] - mean_x) * (x[i] - mean_x);
    }
    return var / x.size();
}

// 线性回归：根据输入的时间步和数据值预测下一个值
double linear_regression(const std::vector<double>& x, const std::vector<double>& y, double next_x) {
    double b1 = covariance(x, y) / variance(x);
    double b0 = mean(y) - b1 * mean(x);
    return b0 + b1 * next_x;  // 预测下一个值
}

// 使用滚动窗口法预测未来的值，递归预测多个点
std::vector<double> predict_future_values(const std::vector<double>& data, int window_size, int future_steps) {
    std::vector<double> future_predictions;

    // 当前数据序列拷贝
    std::vector<double> current_data = data;

    // 开始预测future_steps步
    for (int step = 0; step < future_steps; ++step) {
        std::vector<double> x(window_size);
        std::vector<double> y(window_size);

        // 准备当前窗口的数据
        for (int j = 0; j < window_size; ++j) {
            x[j] = j;  // 时间步长
            y[j] = current_data[current_data.size() - window_size + j];  // 对应的y值
        }

        // 预测下一个数据点
        double next_x = window_size;  // 下一个时间点
        double predicted_value = linear_regression(x, y, next_x);

        // 将预测的值存入结果
        future_predictions.push_back(predicted_value);

        // 将预测值加入到数据序列，用于下一步预测
        current_data.push_back(predicted_value);
    }

    return future_predictions;
}

int main() {
    // 模拟的时间序列数据
    std::vector<double> data = {1, 2, 3, 4, 5, 6, 8, 10, 12, 14, 16, 18, 20, 21, 23, 27, 28, 30, 33, 35, 37, 100, 23, 24};

    int window_size = 5;  // 滚动窗口大小
    int future_steps = 5;  // 预测未来5个点

    // 预测未来的值
    std::vector<double> future_predictions = predict_future_values(data, window_size, future_steps);

    // 输出预测结果
    std::cout << "预测未来的 " << future_steps << " 个点的值: " << std::endl;
    for (size_t i = 0; i < future_predictions.size(); ++i) {
        std::cout << "预测点 " << i + data.size() << ": " << future_predictions[i] << std::endl;
    }

    return 0;
}
