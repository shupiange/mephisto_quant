#!/bin/bash

# --- 脚本信息 ---
SCRIPT_NAME="main.py"
CONDA_ENV="quant"

# --- 1. 参数定义与解析 ---
START_DATE="$1"
END_DATE="$2"
ADJUST_FACTOR="$3"
FIX="$4"
DATA_PATH="./dataset"

# --- 2. 参数输入检查 ---
if [ $# -lt 4 ]; then
    echo "🚨 错误: 参数不足！"
    echo "--------------------------------------------------------"
    echo "用法: $0 <开始日期> <结束日期> <调整因子> <是否修复>"
    echo "示例: $0 2023-01-01 2023-01-15 1 False ./dataset"
    echo "--------------------------------------------------------"
    exit 1
fi

# --- 3. 环境初始化与激活 ---
echo "⚙️ 正在初始化 Conda 环境..."
# 确保 conda 命令可用
if ! command -v conda &> /dev/null; then
    echo "❌ 致命错误: 找不到 'conda' 命令。请确保 Conda 已安装并配置到 PATH 中。"
    exit 1
fi

# 激活环境
source "$(conda info --base)/etc/profile.d/conda.sh" # 初始化环境（推荐方式）
if conda activate "${CONDA_ENV}"; then
    echo "✅ Conda 环境 '${CONDA_ENV}' 激活成功。"
else
    echo "❌ 致命错误: Conda 环境 '${CONDA_ENV}' 激活失败。请检查环境名称。"
    exit 1
fi

# --- 4. 打印执行信息 ---
echo ""
echo "🔥 准备执行量化分析脚本 ${SCRIPT_NAME}..."
echo "========================================================"
echo "    ▸ 数据范围: ${START_DATE} 至 ${END_DATE}"
echo "    ▸ 调整因子: ${ADJUST_FACTOR}"
echo "    ▸ 修复模式: ${FIX}"
echo "    ▸ 数据路径: ${DATA_PATH}"
echo "========================================================"
echo ""

# --- 5. 执行 Python 脚本 ---
# 构建执行命令
COMMAND="python ${SCRIPT_NAME} \
    --start-date \"${START_DATE}\" \
    --end-date \"${END_DATE}\" \
    --adjust-factor \"${ADJUST_FACTOR}\" \
    --fix ${FIX} \
    --path \"${DATA_PATH}\""

# 运行命令
echo "🚀 运行命令: ${COMMAND}"
eval ${COMMAND} # 使用 eval 来正确处理引号和变量

# --- 6. 结果判断与退出 ---
if [ $? -eq 0 ]; then
    echo ""
    echo "🎉 恭喜！脚本执行成功，退出码 $?。"
    echo "========================================================"
else
    echo ""
    echo "💔 警告！脚本执行失败，退出码 $?。请检查上方的错误信息。"
    echo "========================================================"
fi

# 退出 Conda 环境（可选，取决于后续操作）
# conda deactivate 

exit 0