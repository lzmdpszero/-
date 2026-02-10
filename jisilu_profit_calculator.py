#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
集思录LOF基金套利收益计算器
自动从集思录获取LOF基金数据，根据申购费率、佣金、账户信息计算套利收益并排序
"""

import requests
import pandas as pd
from dataclasses import dataclass, field
from typing import List
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import time


@dataclass
class Account:
    """账户信息"""
    name: str  # 账户名称
    commission_rate: Decimal  # 佣金费率 (如 0.0003 表示万3)
    min_commission: Decimal  # 最低佣金
    transfer_fee: Decimal = Decimal('0')  # 过户费


@dataclass
class FundItem:
    """基金项目"""
    fund_id: str  # 基金代码
    fund_name: str  # 基金名称
    price: Decimal  # 场内价格
    fund_nav: Decimal  # 基金净值
    discount_rt: Decimal  # 折溢价率(%)
    apply_fee: str  # 申购费
    redeem_fee: str  # 赎回费
    apply_status: str  # 申购状态
    redeem_status: str  # 赎回状态
    volume: Decimal  # 成交量(万手)
    amount: Decimal  # 成交额(万)
    issuer_nm: str  # 基金公司
    
    # 计算结果字段
    subscribe_fee_rate: Decimal = field(default=None)  # 解析后的申购费率
    estimated_sell_price: Decimal = field(default=None)  # 预估卖出价格
    profit: Decimal = field(default=None)  # 收益
    profit_rate: Decimal = field(default=None)  # 收益率
    
    def parse_apply_fee(self) -> Decimal:
        """解析申购费率"""
        try:
            fee_str = str(self.apply_fee).strip()
            # 处理 "0.12%" 格式
            if '%' in fee_str:
                return Decimal(fee_str.replace('%', '')) / Decimal('100')
            # 处理 "0.0012" 格式
            return Decimal(fee_str)
        except:
            return Decimal('0.0012')  # 默认0.12%
    
    def calculate_arbitrage_profit(self, account: Account, quantity: int = 10000) -> Decimal:
        """
        计算LOF基金套利收益
        
        套利公式（溢价套利）：
        - 场内价格 > 基金净值，存在溢价
        - 申购净值，场内卖出赚取差价
        
        计算步骤：
        1. 申购金额 = 净值 × 数量
        2. 申购费用 = 申购金额 × 申购费率
        3. 实际成本 = 申购金额 + 申购费用
        4. 卖出金额 = 场内价格 × 数量
        5. 卖出佣金 = max(卖出金额 × 佣金费率, 最低佣金)
        6. 收益 = 卖出金额 - 实际成本 - 卖出佣金
        """
        # 解析申购费率
        self.subscribe_fee_rate = self.parse_apply_fee()
        
        # 申购相关计算
        subscribe_amount = self.fund_nav * quantity
        subscribe_fee = subscribe_amount * self.subscribe_fee_rate
        actual_cost = subscribe_amount + subscribe_fee
        
        # 卖出相关计算
        sell_amount = self.price * quantity
        sell_commission = max(
            sell_amount * account.commission_rate,
            account.min_commission
        )
        
        # 计算收益
        self.profit = sell_amount - actual_cost - sell_commission - account.transfer_fee
        self.estimated_sell_price = self.price
        
        # 计算收益率
        if actual_cost > 0:
            self.profit_rate = (self.profit / actual_cost) * 100
        else:
            self.profit_rate = Decimal('0')
            
        return self.profit


class JisiluLOFFetcher:
    """集思录LOF基金数据获取器"""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.jisilu.cn/data/lof/',
        }
        self.base_url = "https://www.jisilu.cn/data/lof/index_lof_list/"
    
    @staticmethod
    def safe_decimal(value, default=Decimal('0')) -> Decimal:
        """安全转换为Decimal"""
        try:
            if value is None or value == '-' or value == '':
                return default
            cleaned = str(value).replace("%", "").replace(",", "").strip()
            return Decimal(cleaned) if cleaned else default
        except:
            return default
    
    def fetch_lof_data(self) -> List[FundItem]:
        """
        从集思录获取LOF基金数据
        """
        timestamp = int(time.time() * 1000)
        url = f"{self.base_url}?___jsl=LST___t={timestamp}&only_owned=&rp=100"
        
        try:
            print(f"正在从集思录获取LOF基金数据...")
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            json_data = response.json()
            
            if 'rows' not in json_data:
                print("返回的数据中未找到 'rows' 字段")
                return []
            
            rows = json_data['rows']
            print(f"成功获取到 {len(rows)} 条LOF基金数据")
            
            fund_items = []
            for item in rows:
                cell = item.get('cell', {})
                
                fund_item = FundItem(
                    fund_id=cell.get('fund_id', ''),
                    fund_name=cell.get('fund_nm', ''),
                    price=self.safe_decimal(cell.get('price')),
                    fund_nav=self.safe_decimal(cell.get('fund_nav')),
                    discount_rt=self.safe_decimal(cell.get('discount_rt')),
                    apply_fee=cell.get('apply_fee', '0.12%'),
                    redeem_fee=cell.get('redeem_fee', ''),
                    apply_status=cell.get('apply_status', ''),
                    redeem_status=cell.get('redeem_status', ''),
                    volume=self.safe_decimal(cell.get('volume')),
                    amount=self.safe_decimal(cell.get('amount')),
                    issuer_nm=cell.get('issuer_nm', '')
                )
                fund_items.append(fund_item)
            
            return fund_items
            
        except Exception as e:
            print(f"获取数据失败: {e}")
            return []


def filter_arbitrage_opportunities(funds: List[FundItem], min_premium: Decimal = Decimal('1.0'), mode: str = "premium") -> List[FundItem]:
    """
    筛选套利机会
    
    Args:
        funds: 基金列表
        min_premium: 最小溢价率/折价率阈值（默认1%）
        mode: 套利模式，"premium"=溢价套利(场内>净值)，"discount"=折价套利(场内<净值)
    
    Returns:
        符合条件的基金列表
    """
    opportunities = []
    for fund in funds:
        # 检查申购状态
        if not ('开放' in fund.apply_status or '限大额' in fund.apply_status):
            continue
            
        if mode == "premium":
            # 溢价套利：场内价格 > 基金净值，即 discount_rt < 0
            if fund.discount_rt < 0 and abs(fund.discount_rt) >= min_premium:
                opportunities.append(fund)
        elif mode == "discount":
            # 折价套利：场内价格 < 基金净值，即 discount_rt > 0
            if fund.discount_rt > 0 and fund.discount_rt >= min_premium:
                opportunities.append(fund)
    
    return opportunities


def calculate_all_profits(funds: List[FundItem], account: Account, quantity: int = 10000) -> List[FundItem]:
    """计算所有基金的套利收益"""
    for fund in funds:
        fund.calculate_arbitrage_profit(account, quantity)
    return funds


def sort_by_profit(funds: List[FundItem], descending: bool = True) -> List[FundItem]:
    """按收益排序"""
    return sorted(funds, key=lambda x: x.profit, reverse=descending)


def sort_by_profit_rate(funds: List[FundItem], descending: bool = True) -> List[FundItem]:
    """按收益率排序"""
    return sorted(funds, key=lambda x: x.profit_rate, reverse=descending)


def format_decimal(value: Decimal, places: int = 4) -> str:
    """格式化Decimal数值"""
    if value is None:
        return "N/A"
    quantize_str = '0.' + '0' * places
    try:
        return str(value.quantize(Decimal(quantize_str), rounding=ROUND_HALF_UP))
    except:
        return str(value)


def print_results(funds: List[FundItem], account: Account, quantity: int = 10000):
    """打印计算结果"""
    print("=" * 140)
    print(f"账户: {account.name} | 佣金: 万{format_decimal(account.commission_rate * 10000, 0)} | 最低佣金: {account.min_commission}元 | 申购数量: {quantity}")
    print("=" * 140)
    
    # 表头
    header = f"{'排名':<5}{'代码':<10}{'名称':<16}{'场内价':<10}{'净值':<10}{'溢价率':<10}{'申购费':<10}{'成交量':<10}{'收益(元)':<12}{'收益率':<10}{'申购状态':<12}"
    print(header)
    print("-" * 140)
    
    # 数据行
    for i, fund in enumerate(funds, 1):
        profit_str = format_decimal(fund.profit, 2)
        profit_rate_str = format_decimal(fund.profit_rate, 2) + '%'
        premium_str = format_decimal(fund.discount_rt, 2) + '%'
        
        name = fund.fund_name[:14] if len(fund.fund_name) > 14 else fund.fund_name
        apply_status = fund.apply_status[:10] if len(fund.apply_status) > 10 else fund.apply_status
        
        row = f"{i:<5}{fund.fund_id:<10}{name:<16}{format_decimal(fund.price):<10}{format_decimal(fund.fund_nav):<10}{premium_str:<10}{fund.apply_fee:<10}{format_decimal(fund.volume):<10}{profit_str:<12}{profit_rate_str:<10}{apply_status:<12}"
        print(row)
    
    print("=" * 140)
    print(f"总计基金数: {len(funds)}")
    if funds:
        total_profit = sum(fund.profit for fund in funds)
        avg_profit_rate = sum(fund.profit_rate for fund in funds) / len(funds)
        print(f"总收益: {format_decimal(total_profit, 2)} 元 | 平均收益率: {format_decimal(avg_profit_rate, 2)}%")
    print()


def export_to_excel(funds: List[FundItem], account: Account, quantity: int, output_file: str = "lof_arbitrage_result.xlsx"):
    """导出结果到Excel"""
    data = []
    for i, fund in enumerate(funds, 1):
        data.append({
            "排名": i,
            "基金代码": fund.fund_id,
            "基金名称": fund.fund_name,
            "场内价格": float(fund.price),
            "基金净值": float(fund.fund_nav),
            "溢价率(%)": float(fund.discount_rt),
            "申购费率": fund.apply_fee,
            "成交量(万手)": float(fund.volume),
            "成交额(万)": float(fund.amount),
            "申购状态": fund.apply_status,
            "赎回状态": fund.redeem_status,
            "基金公司": fund.issuer_nm,
            "申购数量": quantity,
            "收益(元)": float(fund.profit),
            "收益率(%)": float(fund.profit_rate),
            "账户": account.name
        })
    
    df = pd.DataFrame(data)
    df.to_excel(output_file, index=False, engine='openpyxl')
    print(f"结果已导出到: {output_file}")


def print_summary_format(funds: List[FundItem], account: Account, quantity: int):
    """打印汇总格式（类似小钱钱报告格式）"""
    from datetime import datetime
    
    if not funds:
        return
    
    # 只计算正收益基金
    profitable_funds = [f for f in funds if f.profit > 0]
    
    if not profitable_funds:
        print("\n今日无正收益套利机会")
        return
    
    total_profit = sum(fund.profit for fund in profitable_funds)
    total_cost = sum(fund.fund_nav * quantity for fund in profitable_funds)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    
    # 输出格式
    print()
    print(f" +发现可套利基金！今日小钱钱预计：+{format_decimal(total_profit, 2)} 元")
    print(f" 猜。{current_time}")
    print(f" （)报告主人！今日份的小钱钱已送达！")
    print(f" 巡逻时间：{date_time}")
    print(f" 预计小钱钱：+{format_decimal(total_profit, 2)}元")
    print(f" 占用资金：{format_decimal(total_cost, 0)}元")
    print()
    
    # 输出每个基金
    for i, fund in enumerate(profitable_funds, 1):
        print(f" 【{i}】{fund.fund_name}（{fund.fund_id}）")
        print(f"  [溢价率]：{format_decimal(fund.discount_rt, 2)}%")
        print(f"  [申购费]：{fund.apply_fee}")
        print(f"  [申购价]：{format_decimal(fund.fund_nav, 4)}元")
        print(f"  [场内价]：{format_decimal(fund.price, 4)}元")
        print(f"  [申购量]：{quantity}份")
        print(f"  [预计收益]：+{format_decimal(fund.profit, 2)}元")
        print()
    
    print("=" * 50)
    print()


def main():
    """主函数"""
    
    # ==================== 配置区域 ====================
    
    # 定义账户（根据实际情况修改）
    accounts = [
        Account(
            name="账户A-低佣金",
            commission_rate=Decimal('0.0001'),  # 万1
            min_commission=Decimal('5'),  # 最低5元
            transfer_fee=Decimal('0')
        ),
        Account(
            name="账户B-普通佣金",
            commission_rate=Decimal('0.0003'),  # 万3
            min_commission=Decimal('5'),
            transfer_fee=Decimal('0')
        ),
    ]
    
    # 套利配置
    MIN_PREMIUM_RATE = Decimal('0')  # 最小溢价率/折价率阈值(%)，设为0查看所有基金
    ARBITRAGE_MODE = "premium"  # 套利模式: "premium"=溢价套利, "discount"=折价套利
    QUANTITY = 10000  # 申购数量（默认10000份）
    EXPORT_RESULTS = True  # 是否导出结果到Excel
    
    # ==================== 数据获取 ====================
    
    fetcher = JisiluLOFFetcher()
    all_funds = fetcher.fetch_lof_data()
    
    if not all_funds:
        print("未能获取到数据，请检查网络连接")
        return
    
    print()
    
    # ==================== 筛选套利机会 ====================
    
    opportunities = filter_arbitrage_opportunities(all_funds, MIN_PREMIUM_RATE, ARBITRAGE_MODE)
    
    if not opportunities:
        mode_text = "溢价率" if ARBITRAGE_MODE == "premium" else "折价率"
        print(f"未找到{mode_text}超过 {MIN_PREMIUM_RATE}% 的套利机会")
        print(f"当前共有 {len(all_funds)} 只LOF基金，建议调整阈值或稍后再试")
        return
    
    mode_text = "溢价" if ARBITRAGE_MODE == "premium" else "折价"
    print(f"找到 {len(opportunities)} 个潜在套利机会（{mode_text}率 > {MIN_PREMIUM_RATE}%）\n")
    
    # ==================== 计算和输出 ====================
    
    for account in accounts:
        import copy
        account_funds = copy.deepcopy(opportunities)
        
        # 计算收益
        calculate_all_profits(account_funds, account, QUANTITY)
        
        # 按收益排序（从高到低）
        sorted_funds = sort_by_profit(account_funds, descending=True)
        
        # 打印结果
        print_results(sorted_funds, account, QUANTITY)
        
        # 打印汇总格式
        print_summary_format(sorted_funds, account, QUANTITY)
        
        # 导出结果
        if EXPORT_RESULTS:
            safe_name = account.name.replace("-", "_").replace(" ", "_")
            export_to_excel(sorted_funds, account, QUANTITY, f"lof_arbitrage_{safe_name}.xlsx")


if __name__ == "__main__":
    main()
