# 加密货币资费数据分析工具

一个用于获取和分析加密货币交易所资费数据的Python工具包，支持Binance和Bybit两大主流交易所的数据获取。

## 📋 项目简介

本项目提供了一套完整的数据获取和分析工具，可以帮助用户：
- 获取多个交易所的价格、资金费率、溢价指数等数据
- 支持批量和单个交易对数据获取
- 提供数据缓存机制，避免重复请求
- 支持多种时间间隔的历史数据获取

## ✨ 功能特性

### 支持的交易所
- **Binance** (币安期货)
- **Bybit** (Bybit期货)

### 支持的数据类型
- **价格数据** (price) - K线价格数据
- **价格指数** (price_index) - 标的资产价格指数
- **资金费率** (funding_rate) - 永续合约资金费率
- **溢价指数** (premium_index) - 合约溢价指数

### 支持的时间间隔
`1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `3d`, `1w`, `1M`

## 🚀 快速开始

### 环境要求
- Python 3.7+
- 依赖包：`requests`, `pandas`, `logging`

### 安装依赖
```bash
pip install requests pandas
```

### 项目结构
```
资费数据分析/
├── dataFetcher/                 # 数据获取模块
│   ├── __init__.py
│   ├── data_fetcher_base.py     # 基础数据获取器
│   ├── binance_data_fetcher.py  # Binance数据获取器
│   └── bybit_data_fetcher.py    # Bybit数据获取器
├── getAllData.py                # 批量数据获取脚本
├── getSingleData.py             # 单个交易对数据获取脚本
└── README.md                    # 项目说明文档
```

## 📖 使用说明

### 1. 批量获取数据 (getAllData.py)

获取所有共同交易对的数据（默认最近30天）：

```bash
python getAllData.py
```

此脚本会：
- 自动获取Binance和Bybit的共同交易对
- 获取最近30天的所有数据类型
- 数据保存到 `./data` 目录

### 2. 单个交易对数据获取 (getSingleData.py)

更灵活的数据获取方式，支持自定义参数：

#### 基本用法
```bash
# 获取BTCUSDT的价格和资金费率数据
python getSingleData.py BTCUSDT 2025-01-01 2025-01-10 --data-types price funding_rate
```

#### 高级用法
```bash
# 只获取Binance的数据
python getSingleData.py BTCUSDT 2025-01-01 2025-01-10 --exchanges binance --data-types price

# 获取5分钟间隔的数据
python getSingleData.py BTCUSDT 2025-01-01 2025-01-10 --interval 5m --data-types price premium_index

# 指定输出目录和并发数
python getSingleData.py BTCUSDT 2025-01-01 2025-01-10 --output-dir ./custom_data --max-workers 3

# 显示详细日志
python getSingleData.py BTCUSDT 2025-01-01 2025-01-10 --verbose
```

#### 参数说明

| 参数 | 类型 | 必需 | 说明 | 默认值 |
|------|------|------|------|--------|
| `symbol` | str | ✓ | 交易对符号（如 BTCUSDT） | - |
| `start_date` | str | ✓ | 开始日期（YYYY-MM-DD） | - |
| `end_date` | str | ✓ | 结束日期（YYYY-MM-DD） | - |
| `--exchanges` | list | ✗ | 指定交易所 | `["binance", "bybit"]` |
| `--data-types` | list | ✗ | 指定数据类型 | `["price_index", "price", "funding_rate", "premium_index"]` |
| `--interval` | str | ✗ | 时间间隔 | `"1m"` |
| `--output-dir` | str | ✗ | 数据保存目录 | `"./data"` |
| `--max-workers` | int | ✗ | 最大并发线程数 | `5` |
| `--verbose` | flag | ✗ | 显示详细日志 | `False` |

## 💾 数据存储

数据以CSV格式保存在指定目录中，文件命名规则：
```
{exchange}_{symbol}_{data_type}_{interval}_{start_date}_to_{end_date}.csv
```

示例：
```
binance_BTCUSDT_price_1m_2025-01-01_to_2025-01-10.csv
bybit_ETHUSDT_funding_rate_1h_2025-01-01_to_2025-01-10.csv
```

## ⚙️ 配置说明

### API限制配置

项目已针对各交易所API限制进行了优化配置：

**Binance限制：**
- 价格数据：1500条/请求
- 价格指数：1500条/请求
- 资金费率：1000条/请求
- 溢价指数：1500条/请求

**Bybit限制：**
- 价格数据：1000条/请求
- 价格指数：1000条/请求
- 资金费率：200条/请求
- 溢价指数：1000条/请求

### 请求频率控制

- 自动添加1-3秒随机等待时间
- 支持最大重试次数配置（默认3次）
- 支持请求超时配置（默认30秒）
- 支持并发控制（默认3-5个线程）

## 🔧 高级功能

### 数据缓存
- 自动检测已存在的数据文件
- 避免重复下载相同时间范围的数据
- 支持增量数据更新

### 错误处理
- 完善的异常处理机制
- 详细的日志记录
- 自动重试失败的请求

### 并发优化
- 支持多线程并发获取
- 可配置并发数量
- 智能频率控制

## 📊 数据格式

所有数据都以标准化的pandas DataFrame格式返回，包含时间戳索引和相应的数据列。

**价格数据列：**
- `open`, `high`, `low`, `close`, `volume`

**资金费率数据列：**
- `funding_rate`, `funding_time`

**价格指数数据列：**
- `price`

**溢价指数数据列：**
- `premium_index`

## 📝 数据文件示例

### 价格数据 (price)
```csv
timestamp,open,high,low,close,volume
2025-01-01 00:00:00,95000.0,95100.0,94900.0,95050.0,123.45
2025-01-01 00:01:00,95050.0,95200.0,95000.0,95150.0,89.12
```

### 资金费率数据 (funding_rate)
```csv
timestamp,funding_rate,funding_time
2025-01-01 00:00:00,0.0001,2025-01-01 08:00:00
2025-01-01 08:00:00,0.00012,2025-01-01 16:00:00
```

## 🚀 代码示例

### 使用 getSingleData.py 获取单个交易对数据

```python
from getSingleData import SingleSymbolDataFetcher

# 初始化数据获取器
fetcher = SingleSymbolDataFetcher(output_dir="./data", max_workers=5)

# 获取BTCUSDT的数据
results = fetcher.fetch_single_symbol_data(
    symbol="BTCUSDT",
    start_date="2025-01-01",
    end_date="2025-01-10",
    exchanges=["binance", "bybit"],
    data_types=["price", "funding_rate"],
    interval="1m"
)

# 访问数据
binance_price_data = results["binance"]["price"]
bybit_funding_rate = results["bybit"]["funding_rate"]
```

### 批量分析数据示例

```python
import pandas as pd
import os

# 读取价格数据
binance_btc_price = pd.read_csv("./data/binance_BTCUSDT_price_1m_2025-01-01_to_2025-01-10.csv", index_col=0, parse_dates=True)
bybit_btc_price = pd.read_csv("./data/bybit_BTCUSDT_price_1m_2025-01-01_to_2025-01-10.csv", index_col=0, parse_dates=True)

# 比较两个交易所的价格差异
price_diff = binance_btc_price['close'] - bybit_btc_price['close']
print(f"平均价格差异: {price_diff.mean():.2f}")
print(f"最大价格差异: {price_diff.max():.2f}")
```

## ⚠️ 注意事项

1. **API限制：** 请遵守各交易所的API使用限制，避免过于频繁的请求
2. **网络环境：** 确保网络连接稳定，建议在良好的网络环境下使用
3. **数据准确性：** 数据来源于各交易所官方API，但请注意可能存在的延迟或异常
4. **存储空间：** 大量数据获取可能占用较多磁盘空间，请确保有足够的存储空间
5. **时区问题：** 所有时间戳都基于UTC时间，请注意时区转换

## 🐛 故障排除

### 常见问题

**1. 连接超时**
```
问题：requests.exceptions.ConnectTimeout
解决方案：检查网络连接，或增加timeout参数值
```

**2. API限制错误**
```
问题：HTTP 429 Too Many Requests
解决方案：减少并发数量，增加请求间隔时间
```

**3. 数据为空**
```
问题：返回的DataFrame为空
解决方案：检查交易对是否存在，确认时间范围是否合理
```

**4. 模块导入错误**
```
问题：ModuleNotFoundError: No module named 'dataFetcher'
解决方案：确保在项目根目录运行脚本，或检查Python路径配置
```

**5. 权限错误**
```
问题：PermissionError: [Errno 13] Permission denied
解决方案：检查数据保存目录的写入权限
```

## 📊 性能优化建议

1. **合理设置并发数：** 根据网络状况调整max_workers参数
2. **使用数据缓存：** 避免重复下载相同时间范围的数据
3. **分时段获取：** 对于大时间范围的数据，建议分段获取
4. **监控API限制：** 注意观察请求频率，避免触发限制

## 📈 数据分析建议

1. **数据清洗：** 在分析前检查数据完整性和异常值
2. **时间对齐：** 不同交易所的数据时间戳可能略有差异，需要对齐
3. **统计分析：** 可以计算价格差异、资金费率趋势等指标
4. **可视化：** 建议使用matplotlib或plotly进行数据可视化

## 📝 更新日志

### v1.0.0 (2025-01-15)
- 初始版本发布
- 支持Binance和Bybit数据获取
- 实现批量和单个交易对数据获取
- 添加数据缓存机制
- 支持多种数据类型和时间间隔
- 完善的错误处理和日志记录

## 🔮 未来计划

- [ ] 支持更多交易所（OKX、Gate.io等）
- [ ] 添加实时数据获取功能
- [ ] 实现数据分析和可视化模块
- [ ] 添加数据库存储支持
- [ ] 提供Web界面管理
- [ ] 支持更多数据类型（订单簿、交易记录等）

## 🤝 贡献

欢迎提交Issue和Pull Request来帮助改进这个项目！

### 贡献指南
1. Fork本项目
2. 创建feature分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 创建Pull Request

## 📄 许可证

本项目采用MIT许可证，详情请参阅LICENSE文件。

---

**免责声明：** 本工具仅用于数据获取和分析，不构成投资建议。使用本工具进行交易决策的风险由用户自行承担。

**联系方式：** 如有问题或建议，请通过GitHub Issues联系我们。